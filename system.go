package physalis

import (
	bolt "go.etcd.io/bbolt"
)

func readSystemValue[V any](bucket *bolt.Bucket, key []byte) (*V, error) {
	rawData := bucket.Get(key)
	if rawData == nil {
		return nil, nil
	}
	var data *V
	if err := CBORUnmarshal(rawData, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func writeSystemValue[V any](bucket *bolt.Bucket, key []byte, value *V) error {
	bin, err := CBORMarshal(value)
	if err != nil {
		return err
	}
	return bucket.Put(key, bin)
}
