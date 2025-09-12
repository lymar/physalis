package physalis

import (
	"bytes"
	"iter"
	"log/slog"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/go-softwarelab/common/pkg/types"
	bolt "go.etcd.io/bbolt"
)

var reducersBucket = []byte("reducers")
var statesBucket = []byte("states")
var kvBucket = []byte("kv")
var versionKey = []byte("version")
var logPosKey = []byte("log_pos")

// - bucket: reducersBucket - "reducers"
// 		- bucket: <reducer name>
// 			- versionKey -> <version>
// 			- logPosKey -> <last processed log position>
// 			- bucket: statesBucket - "states"
// 				- <event group key> -> <serialized state>
// 				...
// 			- bucket: kvBucket - "kv"
// 				- bucket: <event group key>
// 					- bucket: <map name>
// 						- <serialized key> -> <serialized value>
// 					...
// 				...

type reducerHandler[EV any] struct {
	name             string
	version          string
	serializeState   func(state any) ([]byte, error)
	deserializeState func(data []byte) (any, error)
	prepare          func(*Event[EV]) (string, *Event[EV])
	apply            func(runtime *ReducerRuntime, state any,
		groupKey string, evs iter.Seq2[uint64, *Event[EV]]) any
	cmdChan chan reduceCmd[EV]
}

func newReducerHandler[ST any, EV any](
	name string,
	reducer Reducer[ST, EV],
) *reducerHandler[EV] {
	return &reducerHandler[EV]{
		name:             name,
		version:          reducer.Version(),
		serializeState:   serializeReducerState[ST],
		deserializeState: deserializeReducerState[ST, any],
		prepare:          reducer.Prepare,
		apply:            makeApplyReducerFunc(reducer),
	}
}

type reduceCmd[EV any] struct {
	activeReducers *sync.WaitGroup
	readEvents     func() iter.Seq2[uint64, *Event[EV]]
	writeResult    chan<- func(tx *bolt.Tx) error
}

func (rh *reducerHandler[EV]) mainLoop(db *bolt.DB, init *sync.WaitGroup) {
	if err := rh.init(db); err != nil {
		slog.Error("failed to init reducer handler", "reducer", rh.name, "error", err)
		panic(err)
	}

	// TODO: load events from log

	init.Done()

	for {
		cmd, ok := <-rh.cmdChan
		if !ok {
			break
		}
		rh.doReduce(db, cmd)
	}
}

func (rh *reducerHandler[EV]) doReduce(db *bolt.DB, cmd reduceCmd[EV]) {
	defer cmd.activeReducers.Done()

	byKeysMap := make(map[string][]types.Pair[uint64, *Event[EV]])

	var latestEventId uint64 = 0

	for uid, ev := range cmd.readEvents() {
		latestEventId = uid
		groupKey, newEv := rh.prepare(ev)
		if groupKey == SkipEvent {
			continue
		}
		if newEv != nil {
			ev = newEv
		}
		byKeysMap[groupKey] = append(
			byKeysMap[groupKey],
			types.Pair[uint64, *Event[EV]]{Left: uid, Right: ev})
	}

	bname := []byte(rh.name)

	if latestEventId != 0 {
		cmd.writeResult <- func(tx *bolt.Tx) error {
			reducers := tx.Bucket(reducersBucket)
			bucket := reducers.Bucket(bname)
			return writeSystemValue(bucket, logPosKey, &latestEventId)
		}
	}

	for groupKey, evs := range byKeysMap {
		cmd.activeReducers.Add(1)
		go rh.doReduceKey(db, bname, &cmd, groupKey, evs)
	}
}

func (rh *reducerHandler[EV]) doReduceKey(
	db *bolt.DB,
	bname []byte,
	parentCmd *reduceCmd[EV],
	groupKey string,
	evs []types.Pair[uint64, *Event[EV]],
) {
	defer parentCmd.activeReducers.Done()

	bGroupKey := []byte(groupKey)

	if err := db.View(func(tx *bolt.Tx) (err error) {
		reducers := tx.Bucket(reducersBucket)
		bucket := reducers.Bucket(bname)
		states := bucket.Bucket(statesBucket)

		prevStateRaw := states.Get(bGroupKey)
		var prevState any = nil

		if prevStateRaw != nil {
			prevState, err = rh.deserializeState(prevStateRaw)
			if err != nil {
				return err
			}
		}

		// TODO: создать runtime и писать его если нужно

		newState := rh.apply(nil, prevState, groupKey, evPairsSeq2(evs))
		newStateRaw, err := rh.serializeState(newState)
		if err != nil {
			return err
		}

		if !bytes.Equal(prevStateRaw, newStateRaw) {
			parentCmd.writeResult <- func(tx *bolt.Tx) error {
				reducers := tx.Bucket(reducersBucket)
				bucket := reducers.Bucket(bname)
				states := bucket.Bucket(statesBucket)
				return states.Put(bGroupKey, newStateRaw)
			}
		}

		return nil
	}); err != nil {
		slog.Error("failed to reduce key", "reducer", rh.name, "group_key", groupKey, "error", err)
		panic(err)
	}
}

func evPairsSeq2[EV any](pairs []types.Pair[uint64, *Event[EV]]) iter.Seq2[uint64, *Event[EV]] {
	return func(yield func(uint64, *Event[EV]) bool) {
		for _, p := range pairs {
			if !yield(p.Left, p.Right) {
				break
			}
		}
	}
}

func (rh *reducerHandler[EV]) init(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		reducers, err := tx.CreateBucketIfNotExists(reducersBucket)
		if err != nil {
			return err
		}

		var newBucket bool

		bname := []byte(rh.name)
		reducerBucket := reducers.Bucket(bname)

		if reducerBucket != nil {
			ver, err := readSystemValue[string](reducerBucket, versionKey)
			if err != nil {
				return err
			}
			if *ver != rh.version {
				slog.Debug("reducer version changed, recreating bucket",
					"name", rh.name, "old", *ver, "new", rh.version)
				reducers.DeleteBucket(bname)
				newBucket = true
			} else {
				newBucket = false
			}
		} else {
			newBucket = true
		}

		if newBucket {
			reducerBucket, err = reducers.CreateBucket(bname)
			if err != nil {
				return err
			}
			if err := writeSystemValue(reducerBucket, versionKey, &rh.version); err != nil {
				return err
			}
			logPos := uint64(0)
			if err := writeSystemValue(reducerBucket, logPosKey, &logPos); err != nil {
				return err
			}
			_, err = reducerBucket.CreateBucket(statesBucket)
			if err != nil {
				return err
			}
			slog.Debug("created bucket for reducer", "name", rh.name, "version", rh.version)
		}

		return nil
	})
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
) func(runtime *ReducerRuntime, state any, groupKey string,
	evs iter.Seq2[uint64, *Event[EV]]) any {

	return func(runtime *ReducerRuntime, state any, groupKey string,
		evs iter.Seq2[uint64, *Event[EV]]) any {

		var stState *ST = nil

		if state != nil {
			stState = state.(*ST)
		}

		res := reducer.Apply(runtime, stState, groupKey, evs)

		if res == nil {
			panic("nil state not allowed for reducer")
		} else {
			return res
		}
	}
}
