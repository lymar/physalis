package physalis

import (
	bolt "go.etcd.io/bbolt"
)

type ReducerReader[ST any] struct {
	name             string
	deserializeState func(data []byte) (*ST, error)
}

func (rr *ReducerReader[ST]) State(
	tx *bolt.Tx,
	groupKey string,
) (*ST, error) {
	reducers := tx.Bucket(reducersBucket)
	bucket := reducers.Bucket([]byte(rr.name))
	states := bucket.Bucket(statesBucket)

	stateRaw := states.Get([]byte(groupKey))

	if stateRaw == nil {
		return nil, nil
	} else {
		return rr.deserializeState(stateRaw)
	}
}

func (rr *ReducerReader[ST]) AllStates(tx *bolt.Tx) SortedKVView[string, ST] {
	reducers := tx.Bucket(reducersBucket)
	bucket := reducers.Bucket([]byte(rr.name))
	states := bucket.Bucket(statesBucket)

	return &sortedKVView[string, ST]{
		bucket:       states,
		serializeK:   func(s string) []byte { return []byte(s) },
		deserializeK: func(b []byte) string { return string(b) },
	}
}

func OpenKVView[K KType, V any, ST any](
	reader *ReducerReader[ST],
	tx *bolt.Tx,
	groupKey string,
	kvName string,
) SortedKVView[K, V] {
	var bucket *bolt.Bucket = nil

	reducers := tx.Bucket(reducersBucket)
	reducerBuck := reducers.Bucket([]byte(reader.name))
	kvsBuck := reducerBuck.Bucket(kvsBucket)

	if kvsBuck != nil {
		eventGroupBucket := kvsBuck.Bucket([]byte(groupKey))

		if eventGroupBucket != nil {
			bucket = eventGroupBucket.Bucket([]byte(kvName))
		}
	}

	serializeK, deserializeK := getKTypeSerDe[K]()

	return &sortedKVView[K, V]{
		bucket:       bucket,
		serializeK:   serializeK,
		deserializeK: deserializeK,
	}
}
