package main

import (
	"context"
	"iter"
	"log/slog"
	"os"

	"github.com/lymar/physalis"
	"github.com/lymar/physalis/internal/log"
	bolt "go.etcd.io/bbolt"
)

func initRegistry() (*physalis.ReducerRegistry[TEvent], *physalis.ReducerReader[TReducerState]) {
	reg := physalis.NewReducerRegistry[TEvent]()

	reader, err := physalis.AddReducer(reg, "points4", &TReducer{version: "v1"})
	if err != nil {
		panic(err)
	}

	return reg, reader
}

func writeAndRead() {
	reg, reader := initRegistry()

	phs, err := physalis.Open[TEvent]("dev.db", reg)
	if err != nil {
		panic(err)
	}
	defer phs.Close()

	err = phs.Write(physalis.Transaction[TEvent]{
		Events: []*physalis.Event[TEvent]{
			{Payload: TEvent{Win: &Win{Player: "Alice", Points: 10}}},
			{Payload: TEvent{Loss: &Loss{Player: "Bob", Points: 5}}},
			{Payload: TEvent{Loss: &Loss{Player: "Alice", Points: 4}}},
		},
		BlobStorage: map[string][]byte{
			"blob1": []byte("some blob data 1"),
			"blob2": []byte("some blob data 2"),
			"blob3": nil,
		},
	})

	if err != nil {
		panic(err)
	}

	phs.View(func(tx *bolt.Tx) error {
		alice, err := reader.State(tx, "Alice")
		if err != nil {
			return err
		}
		slog.Debug("Alice", "points", alice.Points)

		bob, err := reader.State(tx, "Bob")
		if err != nil {
			return err
		}
		slog.Debug("Bob", "points", bob.Points)

		aliceLuckyMinutes := physalis.OpenKVView[uint32, int](reader, tx, "Alice", "lucky_minutes")
		for k, v := range aliceLuckyMinutes.Ascend() {
			slog.Debug("Alice lucky minute", "minute", k, "points", *v)
		}

		aliceUnluckyMinutes := physalis.OpenKVView[uint32, int](reader, tx, "Alice", "unlucky_minutes")
		for k, v := range aliceUnluckyMinutes.Ascend() {
			slog.Debug("Alice unlucky minute", "minute", k, "points", *v)
		}

		blob1 := physalis.BlobView(tx, "blob1")
		slog.Debug("blob1", "data", string(blob1))
		blob2 := physalis.BlobView(tx, "blob2")
		slog.Debug("blob2", "data", string(blob2))

		return nil
	})

	f, err := os.Create("dev.backup")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := phs.BackupTo(context.Background(), 5, f); err != nil {
		panic(err)
	}
}

func loadBackupAndRead() {
	dbName := "dev2.db"
	if err := os.Remove(dbName); err != nil {
		if !os.IsNotExist(err) {
			panic(err)
		}
	}

	backup, err := os.Open("dev.backup")
	if err != nil {
		panic(err)
	}
	defer backup.Close()

	err = physalis.RestoreDatabase(context.Background(), dbName, backup)
	if err != nil {
		panic(err)
	}
	backup.Close()

	reg, reader := initRegistry()

	phs, err := physalis.Open[TEvent](dbName, reg)
	if err != nil {
		panic(err)
	}
	defer phs.Close()

	phs.View(func(tx *bolt.Tx) error {
		alice, err := reader.State(tx, "Alice")
		if err != nil {
			return err
		}
		slog.Debug("Alice", "points", alice.Points)

		blob1 := physalis.BlobView(tx, "blob1")
		slog.Debug("blob1", "data", string(blob1))
		blob2 := physalis.BlobView(tx, "blob2")
		slog.Debug("blob2", "data", string(blob2))

		allStates := reader.AllStates(tx)
		for k, v := range allStates.Descend() {
			slog.Debug("all states", "name", k, "points", v.Points)
		}

		return nil
	})
}

func main() {
	log.InitDevLog()

	writeAndRead()
	loadBackupAndRead()
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

	luckyMinutes := physalis.OpenKV[uint32, int](runtime, "lucky_minutes")
	unluckyMinutes := physalis.OpenKV[uint32, int](runtime, "unlucky_minutes")

	if state == nil {
		state = &TReducerState{}
	}
	for eid, ev := range evs {
		minute := uint32(ev.ReadTimestamp().Minute())
		if ev.Payload.Win != nil {
			slog.Debug("TReducer: apply", "eventId", eid, "win", ev.Payload.Win.Player, "points", ev.Payload.Win.Points, "name", groupKey)
			state.Points += ev.Payload.Win.Points

			prevVal := luckyMinutes.Get(minute)
			if prevVal == nil {
				luckyMinutes.Put(minute, &ev.Payload.Win.Points)
			} else {
				newVal := *prevVal + ev.Payload.Win.Points
				luckyMinutes.Put(minute, &newVal)
			}
		} else if ev.Payload.Loss != nil {
			slog.Debug("TReducer: apply", "eventId", eid, "loss", ev.Payload.Loss.Player, "points", ev.Payload.Loss.Points, "name", groupKey)
			state.Points -= ev.Payload.Loss.Points

			prevVal := unluckyMinutes.Get(minute)
			if prevVal == nil {
				mp := -ev.Payload.Loss.Points
				unluckyMinutes.Put(minute, &mp)
			} else {
				newVal := *prevVal - ev.Payload.Loss.Points
				unluckyMinutes.Put(minute, &newVal)
			}
		}
	}

	slog.Debug("TReducer: apply result", "name", groupKey, "points", state.Points)

	return state
}
