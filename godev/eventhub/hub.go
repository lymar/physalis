package eventhub

import (
	"iter"
	"time"

	bolt "go.etcd.io/bbolt"
)

type EventHub[E any] struct {
	db *bolt.DB
}

type Event[E any] struct {
	Key     string
	Payload E
	At      time.Time
}

// это будет приезжать в Reducer
type StoredEvent[E any] struct {
	Uid uint64
	Event[E]
}

// Регистрировать их надо будет через внешнюю к EventHub функцию, т.к.
// тут еще типизация по S
type Reducer[S any, E any] interface {
	Version() string
	Apply(state *S, evs iter.Seq[StoredEvent[E]])
}

var eventsBucketName = []byte("events")

// func Open[E any](path string) (*EventHub[E], error) {
// 	return nil, nil
// }

// func (eh *EventHub[T]) AppendEvents(events ...any) {
// TOOD: эта функция будет вызываться из множества горутин
// поэтому надо сериализовать в ней, чтобы это было размазано по
// процессорам.
// }

// type eventPack[E any] struct {
// 	event   Event[E]
// 	binData []byte
// }

// func (eh *EventHub[E]) applyEvents(events []eventPack[E]) {
// }
