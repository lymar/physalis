package physalis

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"os"
	"time"

	"github.com/DataDog/zstd"
	"github.com/fxamacker/cbor/v2"
	"github.com/go-softwarelab/common/pkg/seq"
	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

type backupEvent struct {
	ID   uint64 `cbor:"1,keyasint"`
	Data []byte `cbor:"2,keyasint"`
}

type backupBlob struct {
	Name string `cbor:"1,keyasint"`
	Data []byte `cbor:"2,keyasint"`
}

type backupItem struct {
	Event *backupEvent `cbor:"1,keyasint,omitempty"`
	Blob  *backupBlob  `cbor:"2,keyasint,omitempty"`
}

func writeBackup(
	ctx context.Context,
	writer io.Writer,
	zstdCompressionLevel int,
	items iter.Seq[backupItem],
) error {
	w := zstd.NewWriterLevel(writer, zstdCompressionLevel)
	defer w.Close()

	em, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return err
	}
	encoder := em.NewEncoder(w)
	n := 0
	for i := range items {
		n++
		if n%1000 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		if err := encoder.Encode(i); err != nil {
			return err
		}
	}
	return nil
}

func loadBackup(reader io.Reader) iter.Seq[backupItem] {
	return func(yield func(backupItem) bool) {
		r := zstd.NewReader(reader)
		defer r.Close()

		dec, err := cbor.DecOptions{}.DecMode()
		if err != nil {
			panic(err)
		}
		decoder := dec.NewDecoder(r)

		for {
			var item backupItem
			err := decoder.Decode(&item)
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			if !yield(item) {
				break
			}
		}
	}
}

func (phs *Physalis[EV]) BackupTo(
	ctx context.Context,
	zstdCompressionLevel int,
	w io.Writer,
) error {
	return phs.View(func(tx *bbolt.Tx) error {
		return phs.backupTo(tx, ctx, zstdCompressionLevel, w)
	})
}

func (phs *Physalis[EV]) backupTo(
	tx *bbolt.Tx,
	ctx context.Context,
	zstdCompressionLevel int,
	w io.Writer,
) error {
	evsSeq := func(yield func(backupItem) bool) {
		evs := tx.Bucket(evsBucket)
		c := evs.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			id := binary.BigEndian.Uint64(k)
			if !yield(backupItem{
				Event: &backupEvent{
					ID:   id,
					Data: v,
				},
			}) {
				break
			}
		}
	}

	blobsSeq := func(yield func(backupItem) bool) {
		blobsBucket := tx.Bucket(blobStorBucket)
		c := blobsBucket.Cursor()
		for n, v := c.First(); n != nil; n, v = c.Next() {
			if !yield(backupItem{
				Blob: &backupBlob{
					Name: string(n),
					Data: v,
				},
			}) {
				break
			}
		}
	}

	return writeBackup(ctx, w, zstdCompressionLevel, seq.Concat(evsSeq, blobsSeq))
}

func RestoreDatabase(
	ctx context.Context,
	dbFile string,
	r io.Reader,
) error {
	if _, err := os.Stat(dbFile); !os.IsNotExist(err) {
		return fmt.Errorf("database file %s already exists", dbFile)
	}

	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		evs, err := tx.CreateBucket(evsBucket)
		if err != nil {
			return err
		}

		blobs, err := tx.CreateBucket(blobStorBucket)
		if err != nil {
			return err
		}

		n := 0
		for item := range loadBackup(r) {
			n++
			if n%1000 == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}

			if item.Event != nil {
				idRaw := make([]byte, 8)
				binary.BigEndian.PutUint64(idRaw, item.Event.ID)
				if err := evs.Put(idRaw, item.Event.Data); err != nil {
					return err
				}
			} else if item.Blob != nil {
				if err := blobs.Put([]byte(item.Blob.Name), item.Blob.Data); err != nil {
					return err
				}
			}
		}

		return nil
	})
}
