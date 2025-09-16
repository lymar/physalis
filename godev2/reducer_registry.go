package physalis

import "errors"

type ReducerRegistry[EV any] struct {
	reducers map[string]*reducerHandler[EV]
}

func NewReducerRegistry[EV any]() *ReducerRegistry[EV] {
	return &ReducerRegistry[EV]{
		reducers: make(map[string]*reducerHandler[EV]),
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
	}, nil
}
