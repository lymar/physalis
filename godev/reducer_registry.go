package physalis

import (
	"bytes"
	"errors"
	"iter"
	"log/slog"

	"github.com/fxamacker/cbor/v2"
	"github.com/go-softwarelab/common/pkg/types"
	bolt "go.etcd.io/bbolt"
)

var globalReducersBucket = []byte("global_reducers")
var keyedReducersBucket = []byte("keyed_reducers")
var keysBucket = []byte("keys")
var kvBucket = []byte("kv")
var kvsBucket = []byte("kvs")
var versionKey = []byte("version")
var stateKey = []byte("state")
var statesBucket = []byte("states")

// DB structure:
// - bucket: globalReducersBucket
// 		- bucket: <reducer name>
// 			- versionKey -> <version>
// 			- stateKey -> <serialized state>
// 			- bucket: kvBucket
// 				- bucket: <map name>
// 					- <serialized key> -> <serialized value>
// 				...
// 		...
// - bucket: keyedReducersBucket
// 		- bucket: <reducer name>
// 			- versionKey -> <version>
// 			- bucket: statesBucket
// 				- <event key> -> <serialized state>
// 				...
// 			- bucket: kvsBucket
// 				- bucket: <event key>
// 					- bucket: <map name>
// 						- <serialized key> -> <serialized value>
// 					...
// 				...

type ReducerRegistry[EV any] struct {
	global map[string]*globReducerHandler[EV]
	keyed  map[string]*reducerHandler[EV]
}

func NewReducerRegistry[EV any]() *ReducerRegistry[EV] {
	return &ReducerRegistry[EV]{
		global: make(map[string]*globReducerHandler[EV]),
		keyed:  make(map[string]*reducerHandler[EV]),
	}
}

type reducerHandler[EV any] struct {
	version          string
	isFresh          bool
	serializeState   func(state any) ([]byte, error)
	deserializeState func(data []byte) (any, error)
	apply            func(runtime *ReducerRuntime, state any,
		evs iter.Seq2[uint64, *Event[EV]]) any
}

type globReducerHandler[EV any] struct {
	reducerHandler[EV]
	currentState    any
	currentStateRaw []byte
}

func wrapGlobReducer[ST any, EV any](
	name string,
	reducer Reducer[ST, EV],
) (*globReducerHandler[EV], *GlobReducerReader[ST]) {
	handler := &globReducerHandler[EV]{
		reducerHandler: reducerHandler[EV]{
			version:          reducer.Version(),
			serializeState:   serializeReducerState[ST],
			deserializeState: deserializeReducerState[ST, any],
			apply:            makeApplyReducerFunc(reducer),
		},
		currentState: nil,
	}

	reader := &GlobReducerReader[ST]{
		reducerReader: reducerReader[ST]{
			name:             name,
			deserializeState: deserializeReducerState[ST, *ST],
		},
	}

	return handler, reader
}

func wrapKeyedReducer[ST any, EV any](
	name string,
	reducer Reducer[ST, EV],
) (*reducerHandler[EV], *KeyedReducerReader[ST]) {
	handler := &reducerHandler[EV]{
		version:          reducer.Version(),
		serializeState:   serializeReducerState[ST],
		deserializeState: deserializeReducerState[ST, any],
		apply:            makeApplyReducerFunc(reducer),
	}

	reader := &KeyedReducerReader[ST]{
		reducerReader: reducerReader[ST]{
			name:             name,
			deserializeState: deserializeReducerState[ST, *ST],
		},
	}

	return handler, reader
}

func serializeReducerState[ST any](state any) ([]byte, error) {
	return cbor.Marshal(state.(*ST))
}

func deserializeReducerState[ST any, R any](data []byte) (R, error) {
	var state *ST
	if err := cbor.Unmarshal(data, &state); err != nil {
		var zero R
		return zero, err
	}
	return any(state).(R), nil
}

func makeApplyReducerFunc[ST any, EV any](
	reducer Reducer[ST, EV],
) func(runtime *ReducerRuntime, state any,
	evs iter.Seq2[uint64, *Event[EV]]) any {

	return func(runtime *ReducerRuntime, state any,
		evs iter.Seq2[uint64, *Event[EV]]) any {

		var stState *ST = nil

		if state != nil {
			stState = state.(*ST)
		}

		res := reducer.Apply(runtime, stState, evs)

		if res == nil {
			panic("nil state not allowed for reducer")
		} else {
			return res
		}
	}
}

var ErrReducerAlreadyExists = errors.New("reducer already exists")

func AddGlobalReducer[ST any, EV any](
	reg *ReducerRegistry[EV],
	name string,
	reducer Reducer[ST, EV],
) (*GlobReducerReader[ST], error) {
	if _, exists := reg.global[name]; exists {
		return nil, ErrReducerAlreadyExists
	}

	h, r := wrapGlobReducer(name, reducer)

	reg.global[name] = h

	return r, nil
}

func AddKeyedReducer[ST any, EV any](
	reg *ReducerRegistry[EV],
	name string,
	reducer Reducer[ST, EV],
) (*KeyedReducerReader[ST], error) {
	if _, exists := reg.keyed[name]; exists {
		return nil, ErrReducerAlreadyExists
	}

	h, r := wrapKeyedReducer(name, reducer)

	reg.keyed[name] = h

	return r, nil
}

func (rr *ReducerRegistry[EV]) init(tx *bolt.Tx) error {
	if err := rr.initGlobal(tx); err != nil {
		return err
	}
	if err := rr.initKeyed(tx); err != nil {
		return err
	}
	return nil
}

func (rr *ReducerRegistry[EV]) initGlobal(tx *bolt.Tx) error {
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
				slog.Debug("global reducer version changed, recreating bucket",
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
			slog.Debug("created bucket for global reducer", "name", name, "version", handler.version)
		} else {
			handler.currentStateRaw = bytes.Clone(reducerBucket.Get(stateKey))

			if handler.currentStateRaw != nil {
				handler.currentState, err = handler.deserializeState(
					handler.currentStateRaw)
				if err != nil {
					slog.Error("failed to deserialize global reducer state",
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

func (rr *ReducerRegistry[EV]) initKeyed(tx *bolt.Tx) error {
	keyed, err := tx.CreateBucketIfNotExists(keyedReducersBucket)
	if err != nil {
		return err
	}

	for name, handler := range rr.keyed {
		var newBucket bool

		bname := []byte(name)
		reducerBucket := keyed.Bucket(bname)
		if reducerBucket != nil {
			ver, err := readSystemValue[string](reducerBucket, versionKey)
			if err != nil {
				return err
			}
			if *ver != handler.version {
				slog.Debug("keyed reducer version changed, recreating bucket",
					"name", name, "old", *ver, "new", handler.version)
				keyed.DeleteBucket(bname)
				newBucket = true
			} else {
				newBucket = false
			}
		} else {
			newBucket = true
		}

		if newBucket {
			reducerBucket, err = keyed.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			if err := writeSystemValue(reducerBucket, versionKey, &handler.version); err != nil {
				return err
			}
			handler.isFresh = true
			slog.Debug("created bucket for keyed reducer", "name", name, "version", handler.version)
		} else {
			handler.isFresh = false
		}
	}

	return nil
}

type reducerKind int

const (
	globalRK reducerKind = iota
	keyedRK
)

type reducerApplyRes struct {
	kind            reducerKind
	name            string
	changed         bool
	currentStateRaw []byte
	kvMaps          map[string]func(writeTx *bolt.Tx) error
	err             error
}

func (rr *ReducerRegistry[EV]) apply(
	db *bolt.DB,
	readEvents func() iter.Seq2[uint64, *Event[EV]],
) func(tx *bolt.Tx) error {
	resChan := make(chan reducerApplyRes, len(rr.global))

	for name, handler := range rr.global {
		go func(name string,
			handler *globReducerHandler[EV],
			readEvents func() iter.Seq2[uint64, *Event[EV]],
			resChan chan<- reducerApplyRes,
		) {
			changed, kvMaps, err := reduceOneGlob(
				db, name, handler, readEvents)
			resChan <- reducerApplyRes{
				kind:            globalRK,
				name:            name,
				changed:         changed,
				currentStateRaw: handler.currentStateRaw,
				kvMaps:          kvMaps,
				err:             err,
			}
		}(name, handler, readEvents, resChan)
	}

	byKeysMap := make(map[string][]types.Pair[uint64, *Event[EV]])

	// todo: применение получения ключей
	// for uid, ev := range readEvents() {
	// 	byKeysMap[ev.Key] = append(
	// 		byKeysMap[ev.Key],
	// 		types.Pair[uint64, *Event[EV]]{Left: uid, Right: ev})
	// }

	for name, handler := range rr.keyed {
		for key, evs := range byKeysMap {
			_ = key
			_ = evs
			_ = name
			_ = handler
		}
	}

	return func(writeTx *bolt.Tx) error {
		glob := writeTx.Bucket(globalReducersBucket)
		keyed := writeTx.Bucket(keyedReducersBucket)

		for range len(rr.global) { // TODO + keyed
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
				var parentBucket *bolt.Bucket
				switch res.kind {
				case globalRK:
					parentBucket = glob
				case keyedRK:
					parentBucket = keyed // TODO: Добавить имя ключа
				}
				reducerBucket := parentBucket.Bucket([]byte(res.name))
				if err := reducerBucket.Put(
					stateKey, res.currentStateRaw); err != nil {
					return err
				}
			} else {
				slog.Debug("reducer state not changed, not saving",
					"name", res.name)
			}
		}
		return nil
	}
}

func reduceOneGlob[EV any](
	db *bolt.DB,
	name string,
	handler *globReducerHandler[EV],
	readEvents func() iter.Seq2[uint64, *Event[EV]],
) (changed bool, kvMaps map[string]func(writeTx *bolt.Tx) error, err error) {
	slog.Debug("reducing global", "name", name)

	runtime := newReducerRuntime(db, name, nil)
	defer func() {
		err = runtime.close()
	}()

	applyRes := handler.apply(runtime, handler.currentState,
		readEvents())
	if runtime.kvMaps != nil {
		kvMaps = make(
			map[string]func(writeTx *bolt.Tx) error,
			len(runtime.kvMaps))
		for k, v := range runtime.kvMaps {
			kvMaps[k] = v.writeToDb
		}
	}
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
