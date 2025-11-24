package physalis

import (
	"encoding/binary"
	"iter"
	"log/slog"

	"github.com/go-softwarelab/common/pkg/types"
	bolt "go.etcd.io/bbolt"
)

var evsBucket = []byte("evs")

// - bucket: evsBucket - "evs"
// 		- <event id> -> <serialized event>

type eventLogHandler[EV any] struct {
	latestID uint64
}

func (evl *eventLogHandler[EV]) init(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		evs, err := tx.CreateBucketIfNotExists(evsBucket)
		if err != nil {
			return err
		}

		c := evs.Cursor()
		latestK, _ := c.Last()

		if latestK != nil {
			evl.latestID = binary.BigEndian.Uint64(latestK)
		} else {
			evl.latestID = 0
		}

		slog.Debug("event log initialized", "latest_id", evl.latestID)

		return nil
	})
}

func (evl *eventLogHandler[EV]) append(
	tx *bolt.Tx,
	events iter.Seq2[uint64, []byte],
) error {
	evs := tx.Bucket(evsBucket)

	for id, evData := range events {
		// slog.Debug("*********", id)

		if id != evl.latestID+1 {
			panic("event IDs must be sequential")
		}
		evl.latestID = id

		idRaw := make([]byte, 8)
		binary.BigEndian.PutUint64(idRaw, id)
		if err := evs.Put(idRaw, evData); err != nil {
			return err
		}
	}

	return nil
}

func loadEventsFromPos[EV any](
	db *bolt.DB,
	fromID uint64,
	maxCount int,
) <-chan types.Pair[uint64, *Event[EV]] {
	out := make(chan types.Pair[uint64, *Event[EV]], 1024)
	go func() {
		defer close(out)

		err := db.View(func(tx *bolt.Tx) error {
			evs := tx.Bucket(evsBucket)
			if evs == nil {
				return nil
			}

			c := evs.Cursor()

			startKey := make([]byte, 8)
			binary.BigEndian.PutUint64(startKey, fromID)

			readed := 0
			for k, v := c.Seek(startKey); k != nil; k, v = c.Next() {
				id := binary.BigEndian.Uint64(k)
				var ev *Event[EV]
				err := CBORUnmarshal(v, &ev)
				if err != nil {
					slog.Error("failed to deserialize event", "error", err, "eventId", id)
					break
				}
				out <- types.Pair[uint64, *Event[EV]]{Left: id, Right: ev}
				readed++
				if readed >= maxCount {
					break
				}
			}

			return nil
		})
		if err != nil {
			slog.Error("failed to load events from log", "error", err)
			panic(err)
		}
	}()

	return out
}
