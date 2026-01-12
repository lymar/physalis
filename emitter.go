package physalis

import (
	"context"
	"sync"
)

type clientEmitter[T any] struct {
	mu         sync.RWMutex
	subs       map[uint64]*ceSubscription[T]
	next       uint64
	dropIfSlow bool
	buf        int
}

type ceSubscription[T any] struct {
	ch  chan T
	ctx context.Context
}

func newClientEmitter[T any](
	bufferPerSub int, dropIfSlow bool) *clientEmitter[T] {
	return &clientEmitter[T]{
		subs:       make(map[uint64]*ceSubscription[T]),
		buf:        bufferPerSub,
		dropIfSlow: dropIfSlow,
	}
}

func (e *clientEmitter[T]) subscribe(ctx context.Context) <-chan T {
	ch := make(chan T, e.buf)

	e.mu.Lock()
	id := e.next
	e.next++
	e.subs[id] = &ceSubscription[T]{
		ch:  ch,
		ctx: ctx,
	}
	e.mu.Unlock()

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			e.mu.Lock()
			c, ok := e.subs[id]
			if ok {
				delete(e.subs, id)
			}
			e.mu.Unlock()
			if ok {
				close(c.ch)
			}
		})
	}

	go func() {
		<-ctx.Done()
		unsub()
	}()

	return ch
}

func (e *clientEmitter[T]) emit(ev T) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, s := range e.subs {
		if !e.dropIfSlow {
			select {
			case s.ch <- ev:
			case <-s.ctx.Done():
			}
		} else {
			select {
			case s.ch <- ev:
			default:
			}
		}
	}
}
