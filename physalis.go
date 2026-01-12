package physalis

import (
	"iter"
	"log/slog"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Physalis[EV any] struct {
	db             *bolt.DB
	registry       *ReducerRegistry[EV]
	transactions   chan *transactionEnvelope[EV]
	mainLoopClosed chan struct{}
	initDone       sync.WaitGroup
}

type Event[EV any] struct {
	Payload   EV    `cbor:"1,keyasint"`
	Timestamp int64 `cbor:"2,keyasint"`
}

const SkipEvent = ""
const GlobalGroup = "."

type Reducer[ST any, EV any] interface {
	Version() string
	// Возвращает
	// 1. ключ группы событий, если он пустой то событие пропускается (SkipEvent)
	// 2. опционально может вернуть другое событие, которое будет использовано вместо текущего. Менять исходное нельзя! Оно шариться между всеми редюссерами!
	Prepare(*Event[EV]) (string, *Event[EV])
	Apply(runtime *ReducerRuntime, state *ST, groupKey string,
		evs iter.Seq2[uint64, *Event[EV]]) *ST
}

type Transaction[EV any] struct {
	Events      []*Event[EV]
	BlobStorage map[string][]byte // nil - для удаления
}

type transactionEnvelope[EV any] struct {
	tx        Transaction[EV]
	rawEvents [][]byte
	done      chan error
}

func Open[EV any](dbFile string, registry *ReducerRegistry[EV]) (*Physalis[EV], error) {
	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	phs := &Physalis[EV]{
		db:             db,
		registry:       registry,
		transactions:   make(chan *transactionEnvelope[EV], 1024),
		mainLoopClosed: make(chan struct{}),
	}
	phs.initDone.Add(1)
	go phs.mainLoop()

	return phs, nil
}

func (phs *Physalis[EV]) Close() error {
	close(phs.transactions)
	<-phs.mainLoopClosed
	return phs.db.Close()
}

func (ev *Event[EV]) ReadTimestamp() time.Time {
	if ev.Timestamp == 0 {
		return time.Time{}
	}
	return time.UnixMicro(ev.Timestamp)
}

func (phs *Physalis[EV]) Write(transaction Transaction[EV]) error {
	rawEvents := make([][]byte, 0, len(transaction.Events))
	for _, ev := range transaction.Events {
		if ev.Timestamp == 0 {
			ev.Timestamp = time.Now().UnixMicro()
		}

		raw, err := CBORMarshal(ev)
		if err != nil {
			return err
		}
		rawEvents = append(rawEvents, raw)
	}

	done := make(chan error, 1)
	phs.transactions <- &transactionEnvelope[EV]{
		transaction,
		rawEvents,
		done,
	}
	return <-done
}

func (phs *Physalis[EV]) View(vf func(tx *bolt.Tx) error) error {
	phs.initDone.Wait()
	return phs.db.View(vf)
}

func (phs *Physalis[EV]) mainLoop() {
	defer close(phs.mainLoopClosed)

	eventLog := eventLogHandler[EV]{}
	if err := eventLog.init(phs.db); err != nil {
		slog.Error("failed to init event log", "error", err)
		panic(err)
	}

	if err := phs.cleanReducers(); err != nil {
		slog.Error("failed to clean reducers", "error", err)
		panic(err)
	}

	var reducersInit sync.WaitGroup
	for _, rh := range phs.registry.reducers {
		rh.cmdChan = make(chan reduceCmd[EV], 1)
		reducersInit.Add(1)
		go rh.mainLoop(phs.db, &reducersInit, eventLog.latestID)
	}
	reducersInit.Wait()

	phs.initDone.Done()

	for {
		t, ok := <-phs.transactions
		if !ok {
			return
		}
		txs := []*transactionEnvelope[EV]{t}
	readRest:
		for {
			select {
			case t, ok := <-phs.transactions:
				if !ok {
					break readRest
				}
				txs = append(txs, t)
			default:
				break readRest
			}
		}

		phs.procTransactions(txs, &eventLog)
	}
}

func (phs *Physalis[EV]) cleanReducers() error {
	return phs.db.Update(func(tx *bolt.Tx) error {
		reducers := tx.Bucket(reducersBucket)
		if reducers == nil {
			return nil
		}

		cleanList := make([]string, 0)

		c := reducers.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			name := string(k)
			if reducer, ok := strings.CutPrefix(name, versionPrefix); ok {
				if _, found := phs.registry.reducers[reducer]; !found {
					cleanList = append(cleanList, reducer)
				}
			}
		}

		for _, name := range cleanList {
			slog.Info("cleaning up reducer", "reducer", name)
			reducers.Delete([]byte(versionPrefix + name))
			reducers.DeleteBucket([]byte(name))
		}

		return nil
	})
}

func (phs *Physalis[EV]) procTransactions(
	txs []*transactionEnvelope[EV], eventLog *eventLogHandler[EV],
) {
	readEvents := wrapEvents(txs, eventLog.latestID)

	resultChan := make(chan *resultWriter, 1024)
	var activeReducers sync.WaitGroup

	for _, rh := range phs.registry.reducers {
		activeReducers.Add(1)

		rh.cmdChan <- reduceCmd[EV]{
			activeReducers: &activeReducers,
			readEvents:     readEvents,
			writeResult:    resultChan,
		}
	}

	go func() {
		activeReducers.Wait()
		close(resultChan)
	}()

	var writers []func(tx *bolt.Tx) error
	var emitters []func(em emitter)
	for writer := range resultChan {
		if writer.toDb != nil {
			writers = append(writers, writer.toDb)
		}
		if writer.emit != nil {
			emitters = append(emitters, writer.emit)
		}
	}

	if err := phs.db.Update(func(tx *bolt.Tx) error {
		if err := blobStorApply(tx, txs); err != nil {
			return err
		}

		if err := eventLog.append(tx, mkRawEvSeq(txs, eventLog.latestID)); err != nil {
			return err
		}

		for _, toDb := range writers {
			if err := toDb(tx); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		slog.Error("failed to write transaction", "error", err)
		panic(err)
	}

	for _, tx := range txs {
		tx.done <- nil
	}

	if len(emitters) > 0 {
		for _, emitFn := range emitters {
			emitFn(phs.registry)
		}
	}
}

func wrapEvents[EV any](
	txs []*transactionEnvelope[EV],
	currentLatestID uint64,
) func() iter.Seq2[uint64, *Event[EV]] {
	return func() iter.Seq2[uint64, *Event[EV]] {
		latestID := currentLatestID
		return func(yield func(uint64, *Event[EV]) bool) {
			for _, tx := range txs {
				for _, ev := range tx.tx.Events {
					latestID++
					if !yield(latestID, ev) {
						return
					}
				}
			}
		}
	}
}

func mkRawEvSeq[EV any](
	txs []*transactionEnvelope[EV],
	latestID uint64,
) iter.Seq2[uint64, []byte] {
	return func(yield func(uint64, []byte) bool) {
		for _, tx := range txs {
			for _, ev := range tx.rawEvents {
				latestID++
				if !yield(latestID, ev) {
					return
				}
			}
		}
	}
}
