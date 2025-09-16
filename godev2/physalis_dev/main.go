package main

import (
	"iter"
	"log/slog"

	"github.com/lymar/physalis"
	"github.com/lymar/physalis/internal/log"
)

func main() {
	log.InitDevLog()

	reg := physalis.NewReducerRegistry[TEvent]()

	physalis.AddReducer(reg, "points", &TReducer{version: "v10"})

	phs, err := physalis.Open[TEvent]("dev.db", reg)
	if err != nil {
		panic(err)
	}
	defer phs.Close()

	// err = phs.Write(physalis.Transaction[TEvent]{
	// 	Events: []*physalis.Event[TEvent]{
	// 		{Payload: TEvent{Win: &Win{Player: "Alice", Points: 10}}},
	// 		{Payload: TEvent{Loss: &Loss{Player: "Bob", Points: 5}}},
	// 		{Payload: TEvent{Loss: &Loss{Player: "Alice", Points: 4}}},
	// 	},
	// })

	// if err != nil {
	// 	panic(err)
	// }
}

type Win struct {
	Player string
	Points int
}

type Loss struct {
	Player string
	Points int
}

type TEvent struct {
	Win  *Win
	Loss *Loss
}

type TReducerState struct {
	Points int
}

type TReducer struct {
	version string
}

func (rd *TReducer) Version() string {
	return rd.version
}

func (rd *TReducer) Prepare(
	ev *physalis.Event[TEvent],
) (string, *physalis.Event[TEvent]) {
	if ev.Payload.Win != nil {
		return ev.Payload.Win.Player, nil
	} else if ev.Payload.Loss != nil {
		return ev.Payload.Loss.Player, nil
	}
	panic("invalid event")
}

func (rd *TReducer) Apply(
	runtime *physalis.ReducerRuntime,
	state *TReducerState,
	groupKey string,
	evs iter.Seq2[uint64, *physalis.Event[TEvent]]) *TReducerState {

	if state == nil {
		state = &TReducerState{}
	}
	for eid, ev := range evs {
		if ev.Payload.Win != nil {
			slog.Debug("TReducer: apply", "eventId", eid, "win", ev.Payload.Win.Player, "points", ev.Payload.Win.Points, "name", groupKey)
			state.Points += ev.Payload.Win.Points
		} else if ev.Payload.Loss != nil {
			slog.Debug("TReducer: apply", "eventId", eid, "loss", ev.Payload.Loss.Player, "points", ev.Payload.Loss.Points, "name", groupKey)
			state.Points -= ev.Payload.Loss.Points
		}
	}

	slog.Debug("TReducer: apply result", "name", groupKey, "points", state.Points)

	return state
}
