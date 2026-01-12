package physalis

import (
	"errors"
	"sync"
)

type ReducerRegistry[EV any] struct {
	reducers map[string]*reducerHandler[EV]
	subs     *registrySubscriptions
}

type registrySubscriptions struct {
	mu          sync.RWMutex
	allKeysSubs map[string]*clientEmitter[string]
}

func NewReducerRegistry[EV any]() *ReducerRegistry[EV] {
	return &ReducerRegistry[EV]{
		reducers: make(map[string]*reducerHandler[EV]),
		subs: &registrySubscriptions{
			allKeysSubs: make(map[string]*clientEmitter[string]),
		},
	}
}

var ErrReducerAlreadyExists = errors.New("reducer already exists")

func AddReducer[ST any, EV any](
	reg *ReducerRegistry[EV],
	name string,
	reducer Reducer[ST, EV],
) (*ReducerReader[ST], error) {
	if _, exists := reg.reducers[name]; exists {
		return nil, ErrReducerAlreadyExists
	}

	h := newReducerHandler[ST](name, reducer)

	reg.reducers[name] = h

	return &ReducerReader[ST]{
		name,
		deserializeReducerState[ST, *ST],
		reg.subs,
	}, nil
}

func (rr *ReducerRegistry[EV]) stateChanged(reducerName string, groupKey string) {
	rr.subs.mu.RLock()
	defer rr.subs.mu.RUnlock()

	if s, ok := rr.subs.allKeysSubs[reducerName]; ok {
		s.emit(groupKey)
	}
}
