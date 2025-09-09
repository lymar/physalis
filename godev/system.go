package physalis

import (
	"github.com/fxamacker/cbor/v2"
	bolt "go.etcd.io/bbolt"
)

// const systemDivider = '╱'

func readSystemValue[V any](bucket *bolt.Bucket, key []byte) (*V, error) {
	rawData := bucket.Get(key)
	if rawData == nil {
		return nil, nil
	}
	var data *V
	if err := cbor.Unmarshal(rawData, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func writeSystemValue[V any](bucket *bolt.Bucket, key []byte, value *V) error {
	bin, err := cbor.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put(key, bin)
}
