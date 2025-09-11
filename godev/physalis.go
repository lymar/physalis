package physalis

import (
	"iter"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Physalis[E any] struct {
	db *bolt.DB
	// globReducers   map[string]*globReducerWrapper[E]
	transactions   chan *Transaction[E]
	mainLoopClosed chan struct{}
}

type Event[E any] struct {
	// Key       string
	Payload   E
	Timestamp int64
}

type Reducer[S any, E any] interface {
	Version() string
	Apply(runtime *ReducerRuntime, state *S, evs iter.Seq2[uint64, *Event[E]]) *S
}

type Transaction[E any] struct {
	Events []Event[E]

	// TODO: тут еще добавление в blob storage, а также условия для транзакции
	// как то
	// 1. Есть уже в blob storage такой объект или нет
	// 2. Ожидаемая версия состояния редюсера с учетом всех его sorted map для оптимистичной блокировки
	// 3. Что то еще?
}

func Open[E any](dbFile string) (*Physalis[E], error) {
	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	phs := &Physalis[E]{
		db: db,
		// globReducers:   make(map[string]*globReducerWrapper[E]),
		transactions:   make(chan *Transaction[E], 1024),
		mainLoopClosed: make(chan struct{}),
	}
	go phs.mainLoop()

	return phs, nil
}

func (phs *Physalis[E]) Close() error {
	close(phs.transactions)
	<-phs.mainLoopClosed
	return phs.db.Close()
}

func (phs *Physalis[E]) mainLoop() {
	defer close(phs.mainLoopClosed)

	for {
		t, ok := <-phs.transactions
		if !ok {
			return
		}
		txs := []*Transaction[E]{t}
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
	}
}

// type globReducerWrapper[E any] struct {
// 	version string
// 	apply   func(runtime *ReducerRuntime, serializedState []byte,
// 		evs iter.Seq2[uint64, *Event[E]]) ([]byte, error)
// 	// TODO: нужны отдельные методы сериализации/десериализации состояния
// }

// TODO: нужен типа ReducerRegistry, в него сначала все добавляются, а потом
// передается в Open
