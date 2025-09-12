package physalis

import (
	"iter"
	"log/slog"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	bolt "go.etcd.io/bbolt"
)

type Physalis[EV any] struct {
	db             *bolt.DB
	registry       *ReducerRegistry[EV]
	transactions   chan *transactionEnvelope[EV]
	mainLoopClosed chan struct{}
}

type Event[EV any] struct {
	Payload   EV
	Timestamp int64
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

type ReducerRuntime struct{}

type Transaction[EV any] struct {
	Events []*Event[EV]

	// TODO: тут еще добавление и удаление в blob storage
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
	go phs.mainLoop()

	return phs, nil
}

// TODO: удалять данные редьюссеров, если их нет в новом reducer_registry

func (phs *Physalis[EV]) Close() error {
	close(phs.transactions)
	<-phs.mainLoopClosed
	return phs.db.Close()
}

func (phs *Physalis[EV]) Write(transaction Transaction[EV]) error {
	rawEvents := make([][]byte, len(transaction.Events))
	for _, ev := range transaction.Events {
		raw, err := cbor.Marshal(ev)
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

func (phs *Physalis[EV]) mainLoop() {
	defer close(phs.mainLoopClosed)

	eventLog := eventLogHandler[EV]{}
	if err := eventLog.init(phs.db); err != nil {
		panic(err)
	}

	var reducersInit sync.WaitGroup
	for _, rh := range phs.registry.reducers {
		rh.cmdChan = make(chan reduceCmd[EV], 1)
		reducersInit.Add(1)
		go rh.mainLoop(phs.db, &reducersInit)
	}
	reducersInit.Wait()

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

func (phs *Physalis[EV]) procTransactions(
	txs []*transactionEnvelope[EV], eventLog *eventLogHandler[EV],
) {
	readEvents := wrapEvents(txs, eventLog.latestID)

	resultChan := make(chan func(tx *bolt.Tx) error, 1024)
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

	if err := phs.db.Update(func(tx *bolt.Tx) error {
		if err := eventLog.append(tx, mkRawEvSeq(txs, eventLog.latestID)); err != nil {
			return err
		}

		for writeFn := range resultChan {
			if err := writeFn(tx); err != nil {
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
}

func wrapEvents[EV any](
	txs []*transactionEnvelope[EV],
	latestID uint64,
) func() iter.Seq2[uint64, *Event[EV]] {
	return func() iter.Seq2[uint64, *Event[EV]] {
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
