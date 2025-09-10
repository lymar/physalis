package physalis

import (
	"bytes"
	"errors"
	"iter"
	"log/slog"

	"github.com/fxamacker/cbor/v2"
	bolt "go.etcd.io/bbolt"
)

// TODO: KeyedReducer / GlobalReducer

var globalReducersBucket = []byte("global_reducers")
var kvBucket = []byte("kv")
var versionKey = []byte("version")
var stateKey = []byte("state")

type ReducerRegistry[EV any] struct {
	global map[string]*reducerHandler[EV]
}

func NewReducerRegistry[EV any]() *ReducerRegistry[EV] {
	return &ReducerRegistry[EV]{
		global: make(map[string]*reducerHandler[EV]),
	}
}

type reducerHandler[EV any] struct {
	version          string
	isFresh          bool
	currentState     any
	currentStateRaw  []byte
	serializeState   func(state any) ([]byte, error)
	deserializeState func(data []byte) (any, error)
	apply            func(runtime *ReducerRuntime, state any,
		evs iter.Seq2[uint64, *Event[EV]]) any
}

func wrapReducer[ST any, EV any](
	name string,
	reducer Reducer[ST, EV],
) (*reducerHandler[EV], *ReducerReader[ST]) {
	reader := &ReducerReader[ST]{
		name: name,
		deserializeState: func(data []byte) (*ST, error) {
			var state *ST
			if err := cbor.Unmarshal(data, &state); err != nil {
				return state, err
			}
			return state, nil
		},
	}

	handler := &reducerHandler[EV]{
		version:      reducer.Version(),
		currentState: nil,
		serializeState: func(state any) ([]byte, error) {
			return cbor.Marshal(state.(*ST))
		},
		deserializeState: func(data []byte) (any, error) {
			var state *ST
			if err := cbor.Unmarshal(data, &state); err != nil {
				return nil, err
			}
			return state, nil
		},
		apply: func(runtime *ReducerRuntime, state any,
			evs iter.Seq2[uint64, *Event[EV]]) any {

			var stState *ST = nil

			if state != nil {
				stState = state.(*ST)
			}

			res := reducer.Apply(runtime, stState, evs)

			if res == nil {
				return nil
			} else {
				return res
			}
		},
	}

	return handler, reader
}

var ErrReducerAlreadyExists = errors.New("reducer already exists")

func AddGlobalReducer[ST any, EV any](
	reg *ReducerRegistry[EV],
	name string,
	reducer Reducer[ST, EV],
) (*ReducerReader[ST], error) {
	if _, exists := reg.global[name]; exists {
		return nil, ErrReducerAlreadyExists
	}

	h, r := wrapReducer(name, reducer)

	reg.global[name] = h

	return r, nil
}

func (rr *ReducerRegistry[EV]) init(tx *bolt.Tx) error {
	glob, err := tx.CreateBucketIfNotExists(globalReducersBucket)
	if err != nil {
		return err
	}

	for name, handler := range rr.global {
		var newBucket bool

		bname := []byte(name)
		reducerBucket := glob.Bucket(bname)
		if reducerBucket != nil {
			ver, err := readSystemValue[string](reducerBucket, versionKey)
			if err != nil {
				return err
			}
			if *ver != handler.version {
				slog.Debug("reducer version changed, recreating bucket",
					"name", name, "old", *ver, "new", handler.version)
				glob.DeleteBucket(bname)
				newBucket = true
			} else {
				newBucket = false
			}
		} else {
			newBucket = true
		}

		if newBucket {
			reducerBucket, err = glob.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			if err := writeSystemValue(reducerBucket, versionKey, &handler.version); err != nil {
				return err
			}
			handler.isFresh = true
			slog.Debug("created bucket for reducer", "name", name, "version", handler.version)
		} else {
			handler.currentStateRaw = bytes.Clone(reducerBucket.Get(stateKey))

			if handler.currentStateRaw != nil {
				handler.currentState, err = handler.deserializeState(
					handler.currentStateRaw)
				if err != nil {
					slog.Error("failed to deserialize reducer state",
						"name", name, "err", err)
					return err
				}
			} else {
				handler.currentState = nil
			}

			handler.isFresh = false
		}
	}

	return nil
}

type reducerApplyRes struct {
	name    string
	changed bool
	kvMaps  map[string]func(writeTx *bolt.Tx) error
	err     error
}

func (rr *ReducerRegistry[EV]) apply(
	db *bolt.DB,
	readEvents func() iter.Seq2[uint64, *Event[EV]],
) func(tx *bolt.Tx) error {
	resChan := make(chan reducerApplyRes, len(rr.global))

	for name, handler := range rr.global {
		go func(name string,
			handler *reducerHandler[EV],
			readEvents func() iter.Seq2[uint64, *Event[EV]],
			resChan chan<- reducerApplyRes,
		) {
			changed, kvMaps, err := reduceOne(
				db, name, handler, readEvents)
			resChan <- reducerApplyRes{
				name:    name,
				changed: changed,
				kvMaps:  kvMaps,
				err:     err,
			}
		}(name, handler, readEvents, resChan)
	}

	return func(writeTx *bolt.Tx) error {
		glob := writeTx.Bucket(globalReducersBucket)

		for range len(rr.global) {
			res := <-resChan
			if res.err != nil {
				return res.err
			}
			if res.kvMaps != nil {
				for _, v := range res.kvMaps {
					if err := v(writeTx); err != nil {
						return err
					}
				}
			}
			if res.changed {
				slog.Debug("reducer state changed, saving",
					"name", res.name)
				reducerBucket := glob.Bucket([]byte(res.name))
				handler := rr.global[res.name]
				if handler.currentState == nil {
					slog.Debug("reducer state is nil, deleting from db",
						"name", res.name)
					reducerBucket.Delete(stateKey)
				} else {
					if err := reducerBucket.Put(
						stateKey, handler.currentStateRaw); err != nil {
						return err
					}
				}
			} else {
				slog.Debug("reducer state not changed, not saving",
					"name", res.name)
			}
		}
		return nil
	}
}

func reduceOne[EV any](
	db *bolt.DB,
	name string,
	handler *reducerHandler[EV],
	readEvents func() iter.Seq2[uint64, *Event[EV]],
) (changed bool, kvMaps map[string]func(writeTx *bolt.Tx) error, err error) {
	slog.Debug("reducing", "name", name)

	runtime := newReducerRuntime(db, name)
	defer func() {
		err = runtime.close()
	}()

	applyRes := handler.apply(runtime, handler.currentState,
		readEvents())
	if runtime.kvMaps != nil {
		kvMaps = make(map[string]func(writeTx *bolt.Tx) error, len(runtime.kvMaps))
		for k, v := range runtime.kvMaps {
			kvMaps[k] = v.writeToDb
		}
	}
	if applyRes == nil {
		if handler.currentState != nil {
			handler.currentState = nil
			handler.currentStateRaw = nil
			return true, kvMaps, nil
		} else {
			return false, kvMaps, nil
		}
	} else {
		rawState, err := handler.serializeState(applyRes)
		if err != nil {
			return false, kvMaps, err
		}

		if bytes.Equal(rawState, handler.currentStateRaw) {
			return false, kvMaps, nil
		} else {
			handler.currentState = applyRes
			handler.currentStateRaw = rawState
			return true, kvMaps, nil
		}
	}
}
