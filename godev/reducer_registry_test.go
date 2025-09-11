package physalis

import (
	"iter"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/go-softwarelab/common/pkg/seq2"
	"github.com/lymar/physalis/internal"
	"github.com/lymar/physalis/internal/log"
	bolt "go.etcd.io/bbolt"
)

// go test ./ -v

type Increment struct {
	Val int
}

type Decrement struct {
	Val int
}

type TEvent struct {
	Increment *Increment
	Decrement *Decrement
}

type TReducerState struct {
	Counter int
}

type TReducer struct {
	version string
}

func (gr *TReducer) Version() string {
	return gr.version
}

var globReducerName = "inc_global"
var keyedReducerName = "inc_keyed"

func (gr *TReducer) Apply(
	runtime *ReducerRuntime,
	state *TReducerState,
	evs iter.Seq2[uint64, *Event[TEvent]]) *TReducerState {

	if state == nil {
		state = &TReducerState{}
	}
	for eid, ev := range evs {
		if ev.Payload.Increment != nil {
			slog.Debug("TReducer: apply", "eventId", eid, "increment", ev.Payload.Increment.Val)
			state.Counter += ev.Payload.Increment.Val
		} else if ev.Payload.Decrement != nil {
			slog.Debug("TReducer: apply", "eventId", eid, "decrement", ev.Payload.Decrement.Val)
			state.Counter -= ev.Payload.Decrement.Val
		}
	}

	return state
}

func TestGlobalBase(t *testing.T) {
	log.InitDevLog()

	reg := NewReducerRegistry[TEvent]()
	_, err := AddGlobalReducer(reg, globReducerName, &TReducer{version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	err = withTempDb(func(db *bolt.DB) {
		if err := db.Update(func(tx *bolt.Tx) error {
			err := reg.init(tx)
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		writeFn := reg.apply(db, getSimpleEvents())

		if err := db.Update(func(tx *bolt.Tx) error {
			err := writeFn(tx)
			if err != nil {
				return err
			}

			loadedState, err := loadGlobReducerStateFromDb[TReducerState](tx, globReducerName)
			if err != nil {
				return err
			}
			if loadedState == nil {
				t.Fatalf("no state after first write")
			}
			if loadedState.Counter != 3 {
				t.Fatalf("invalid state after first write, expected 3, got %v", loadedState.Counter)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		currentInMemState := reg.global[globReducerName].currentState.(*TReducerState).Counter
		if currentInMemState != 3 {
			t.Fatalf(
				"invalid in-memory state after first write, expected 3, got %v",
				currentInMemState)
		}

		// Testing that no write occurs if the state does not change

		if err := db.Update(func(tx *bolt.Tx) error {
			if err := writeGlobReducerStateToDb(tx, globReducerName, &TReducerState{Counter: 123}); err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		writeFn = reg.apply(db, mkEventSeq(4,
			&Event[TEvent]{Payload: TEvent{Increment: &Increment{Val: 5}}},
			&Event[TEvent]{Payload: TEvent{Decrement: &Decrement{Val: 5}}}))

		if err := db.Update(func(tx *bolt.Tx) error {
			err := writeFn(tx)
			if err != nil {
				return err
			}

			loadedState, err := loadGlobReducerStateFromDb[TReducerState](tx, globReducerName)

			if loadedState.Counter != 123 {
				t.Fatalf("state changed when it should not, expected 123, got %v", loadedState.Counter)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestVersionChange(t *testing.T) {
	log.InitDevLog()

	err := withTempDb(func(db *bolt.DB) {
		if err := db.Update(func(tx *bolt.Tx) error {
			reg := NewReducerRegistry[TEvent]()
			_, err := AddGlobalReducer(reg, globReducerName, &TReducer{version: "1"})
			if err != nil {
				t.Fatal(err)
			}

			err = reg.init(tx)
			if err != nil {
				return err
			}

			writeFn := reg.apply(db, getSimpleEvents())

			err = writeFn(tx)
			if err != nil {
				t.Fatal(err)
			}

			reg = NewReducerRegistry[TEvent]()
			_, err = AddGlobalReducer(reg, globReducerName, &TReducer{version: "1"})
			if err != nil {
				t.Fatal(err)
			}

			err = reg.init(tx)
			if err != nil {
				return err
			}

			loadedState, err := loadGlobReducerStateFromDb[TReducerState](tx, globReducerName)

			if loadedState.Counter != 3 {
				t.Fatalf("state changed when it should not, expected 3, got %v", loadedState.Counter)
			}

			reg = NewReducerRegistry[TEvent]()
			_, err = AddGlobalReducer(reg, globReducerName, &TReducer{version: "2"})
			if err != nil {
				t.Fatal(err)
			}

			err = reg.init(tx)
			if err != nil {
				return err
			}

			loadedState, err = loadGlobReducerStateFromDb[TReducerState](tx, globReducerName)

			if err != nil {
				t.Fatal(err)
			}
			if loadedState != nil {
				t.Fatalf("state should be nil, got %v", loadedState)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

// go test ./ -v -run ^TestKeyedBase$
func TestKeyedBase(t *testing.T) {
	log.InitDevLog()

	reg := NewReducerRegistry[TEvent]()
	_, err := AddKeyedReducer(reg, keyedReducerName, &TReducer{version: "1"})
	if err != nil {
		t.Fatal(err)
	}

	err = withTempDb(func(db *bolt.DB) {
		if err := db.Update(func(tx *bolt.Tx) error {
			err := reg.init(tx)
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		writeFn := reg.apply(db, getSimpleEvents())

		if err := db.Update(func(tx *bolt.Tx) error {
			err := writeFn(tx)
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TODO: test keyed reducer version changed

func loadGlobReducerStateFromDb[T any](tx *bolt.Tx, reducerName string) (*T, error) {
	glob := tx.Bucket(globalReducersBucket)
	reducerBucket := glob.Bucket([]byte(reducerName))
	rawState := reducerBucket.Get(stateKey)
	if rawState == nil {
		return nil, nil
	}
	var state *T
	if err := cbor.Unmarshal(rawState, &state); err != nil {
		return nil, err
	}
	return state, nil
}

func writeGlobReducerStateToDb[T any](tx *bolt.Tx, reducerName string, state *T) error {
	glob := tx.Bucket(globalReducersBucket)
	reducerBucket := glob.Bucket([]byte(reducerName))
	binState, err := cbor.Marshal(state)
	if err != nil {
		return err
	}
	return reducerBucket.Put(stateKey, binState)
}

func getSimpleEvents() func() iter.Seq2[uint64, *Event[TEvent]] {
	return mkEventSeq(1,
		&Event[TEvent]{Payload: TEvent{Increment: &Increment{Val: 5}}},
		&Event[TEvent]{Payload: TEvent{Decrement: &Decrement{Val: 4}}},
		&Event[TEvent]{Payload: TEvent{Increment: &Increment{Val: 2}}},
	)
}

func mkEventSeq(startUid uint64, events ...*Event[TEvent]) func() iter.Seq2[uint64, *Event[TEvent]] {
	return func() iter.Seq2[uint64, *Event[TEvent]] {
		return seq2.Map(
			slices.All(events),
			func(i int, ev *Event[TEvent]) (uint64, *Event[TEvent]) {
				return uint64(i) + startUid, ev
			})
	}
}

func withTempDb(cb func(*bolt.DB)) error {
	dbFile, err := internal.TempFileName("rrt_", ".db")
	if err != nil {
		return err
	}
	defer os.Remove(dbFile)

	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	cb(db)

	return nil
}
