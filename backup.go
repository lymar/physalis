package physalis

import (
	"io"
	"iter"

	"github.com/fxamacker/cbor/v2"
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

func writeBackup(w io.Writer, items iter.Seq[backupItem]) error {
	em, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return err
	}
	encoder := em.NewEncoder(w)
	for i := range items {
		if err := encoder.Encode(i); err != nil {
			return err
		}
	}
	return nil
}

func loadBackup(r io.Reader) iter.Seq[backupItem] {
	dec, err := cbor.DecOptions{}.DecMode()
	if err != nil {
		panic(err)
	}
	decoder := dec.NewDecoder(r)

	return func(yield func(backupItem) bool) {
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
