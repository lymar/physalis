//go:build scratch

package main

import (
	"encoding/binary"
	"log/slog"
	"os"
	"time"

	bucketproxy "github.com/lymar/physalis/bucket_proxy"
	"github.com/lymar/physalis/internal"
	"github.com/lymar/physalis/internal/log"

	bolt "go.etcd.io/bbolt"
)

// go run -tags=scratch ./bucket_proxy/scratch

var bucketName = []byte("qwe")

func main() {
	log.InitDevLog()

	// slog.Debug("Info message")
	// slog.Info("Info message")
	// slog.Warn("Info message")
	// slog.Error("Info message")

	dbFile, err := internal.TempFileName("bps_", ".db")
	if err != nil {
		panic(err)
	}
	defer os.Remove(dbFile)

	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}

		for _, p := range []struct {
			Key   uint64
			Value string
		}{
			{1, "one"},
			{2, "two"},
			{5, "five"},
			{6, "six"},
			{15, "fifteen"},
			{42, "life"},
		} {
			if err := bucket.Put(u64be(p.Key), []byte(p.Value)); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		panic(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		bpx := bucketproxy.New(bucket)

		for k, v := range bpx.AscendRange(u64be(1), u64be(100)) {
			slog.Debug("iter", "k", binary.BigEndian.Uint64(k), "v", string(v))
		}

		bpx.Delete(u64be(5))
		bpx.Put(u64be(6), []byte("SIX!!!"))
		bpx.Put(u64be(10), []byte("TEN!!!"))

		for k, v := range bpx.AscendRange(u64be(1), u64be(100)) {
			slog.Debug("iter", "k", binary.BigEndian.Uint64(k), "v", string(v))
		}

		v1 := string(bpx.Get(u64be(1)))
		slog.Debug("get v1", "val", v1)

		bpx.Delete(u64be(1))

		v1d := string(bpx.Get(u64be(1)))
		slog.Debug("get v1 after delete", "val", v1d)

		bpx.Put(u64be(1), []byte("ONE!!!"))

		v1p := string(bpx.Get(u64be(1)))
		slog.Debug("get v1 after put", "val", v1p)

		return nil
	})

}

func u64be(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func beU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
