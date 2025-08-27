package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("events")

type SomeStruct struct {
	Fld1   string
	Fld2   int
	MapFld map[uint64]string
}

func main() {
	db, err := bolt.Open("qwe.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket(bucketName)
		b, err := tx.CreateBucket(bucketName)
		if err != nil {
			return err
		}

		var idxs = make([]uint64, 100)
		for i := range idxs {
			idxs[i] = uint64(i + 1)
		}
		rand.Shuffle(len(idxs), func(i int, j int) {
			idxs[i], idxs[j] = idxs[j], idxs[i]
		})

		// log.Printf("%+v", idxs)

		// b.Put(u64be(2), []byte("asd"))
		// b.Put(u64be(3), []byte("zxc"))

		for _, idx := range idxs {
			data := fmt.Appendf([]byte{}, "data_%d", idx)
			log.Printf("writing: %d: %s", idx, data)
			b.Put(u64be(idx), data)
		}

		return nil
	})

	if err != nil {
		log.Panic(err)
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			log.Printf("full scan: %d -> %s\n", beU64(k), v)
		}

		for k, v := c.Seek(u64be(95)); k != nil; k, v = c.Next() {
			log.Printf("with seek: %d -> %s\n", beU64(k), v)
		}

		return nil
	})

	if err != nil {
		log.Panic(err)
	}

	ssi1 := SomeStruct{
		Fld1: "qwe",
		Fld2: 123,
		MapFld: map[uint64]string{
			1:   "asd",
			2:   "zxc",
			100: "rty",
		},
	}

	b, err := cbor.Marshal(ssi1)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("marshal data: %v", b)

	var ssi2 SomeStruct
	err = cbor.Unmarshal(b, &ssi2)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("unmarshal data: %+v", ssi2)
}

func u64be(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func beU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
