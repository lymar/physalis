package physalis

import (
	"encoding/binary"
	"iter"
	"log/slog"

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
