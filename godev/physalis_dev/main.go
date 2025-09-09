package main

import (
	"iter"
	"os"

	"github.com/lymar/physalis"
	"github.com/lymar/physalis/internal"
	"github.com/lymar/physalis/internal/log"
)

type Increment struct {
	Val int
}

type Decrement struct {
	Val int
}

type Event struct {
	Increment *Increment
	Decrement *Decrement
}

type GlobalReducerState struct {
	Counter int
}

type GlobalReducer struct{}

func (gr *GlobalReducer) Version() string {
	return "aaa"
}

func (gr *GlobalReducer) Apply(
	runtime *physalis.ReducerRuntime,
	state *GlobalReducerState,
	evs iter.Seq2[uint64, *physalis.Event[Event]]) *GlobalReducerState {

	if state == nil {
		state = &GlobalReducerState{}
	}
	for _, ev := range evs {
		if ev.Payload.Increment != nil {
			state.Counter += ev.Payload.Increment.Val
		} else if ev.Payload.Decrement != nil {
			state.Counter -= ev.Payload.Decrement.Val
		}
	}
	return state
}

func main() {
	log.InitDevLog()

	dbFile, err := internal.TempFileName("pd_", ".db")
	if err != nil {
		panic(err)
	}
	defer os.Remove(dbFile)

	phs, err := physalis.Open[Event](dbFile)
	if err != nil {
		panic(err)
	}
	defer phs.Close()

	reg := physalis.NewReducerRegistry[Event]()

	physalis.AddGlobalReducer[GlobalReducerState, Event](
		reg, "global_counter", &GlobalReducer{})
}
