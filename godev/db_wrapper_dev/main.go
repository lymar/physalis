package main

import (
	"encoding/binary"
	"log"
	"time"

	cbor "github.com/fxamacker/cbor/v2"
	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("events")

type DBInsance struct {
	db *bolt.DB
}

func NewDBInstance(dbPath string) (*DBInsance, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}

	return &DBInsance{db: db}, nil
}

func (d *DBInsance) Close() (err error) {
	if d.db != nil {
		err = d.db.Close()
		d.db = nil
	}
	return
}

func (d *DBInsance) Put(key uint64, data any) error {
	bin, err := cbor.Marshal(data)
	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		keyBin := u64be(key)
		bucket := tx.Bucket(bucketName)
		bucket.Put(keyBin[:], bin)
		return nil
	})
}

func GetAs[T any](db *DBInsance, key uint64) (T, error) {
	var data T
	var rawData []byte

	if err := db.db.View(func(tx *bolt.Tx) error {
		k := u64be(key)
		rawData = tx.Bucket(bucketName).Get(k[:])
		return nil
	}); err != nil {
		return data, err
	}

	if rawData == nil {
		return data, nil
	}

	if err := cbor.Unmarshal(rawData, &data); err != nil {
		return data, err
	}

	return data, nil
}

type SomeStruct struct {
	Fld1   string
	Fld2   int
	MapFld map[uint64]string
}

func main() {
	db, err := NewDBInstance("qwe2.db")
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	_ = db.Put(1, SomeStruct{
		Fld1: "qwe",
		Fld2: 123,
		MapFld: map[uint64]string{
			1:   "asd",
			2:   "zxc",
			100: "rty",
		},
	})

	if d, err := GetAs[SomeStruct](db, 1); err != nil {
		log.Panic(err)
	} else {
		log.Printf("%+v", d)
	}

	if d, err := GetAs[*SomeStruct](db, 1); err != nil {
		log.Panic(err)
	} else {
		log.Printf("%+v", d)
	}

	if d, err := GetAs[*SomeStruct](db, 2); err != nil {
		log.Panic(err)
	} else {
		log.Printf("%+v", d)
	}
}

func u64be(v uint64) (res [8]byte) {
	binary.BigEndian.PutUint64(res[:], v)
	return
}

func beU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
