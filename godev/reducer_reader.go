package physalis

import (
	"iter"

	"github.com/lymar/physalis/internal/bucketproxy"
	bolt "go.etcd.io/bbolt"
)

type reducerReader[ST any] struct {
	name             string
	deserializeState func(data []byte) (*ST, error)
}

type GlobReducerReader[ST any] struct {
	reducerReader[ST]
}

func (rr *GlobReducerReader[ST]) ReadState(tx *bolt.Tx) (*ST, error) {
	glob := tx.Bucket(globalReducersBucket)
	reducerBucket := glob.Bucket([]byte(rr.name))
	st := reducerBucket.Get(stateKey)
	if st == nil {
		return nil, nil
	} else {
		return rr.deserializeState(st)
	}
}

type KeyedReducerReader[ST any] struct {
	reducerReader[ST]
}

type ReadOnlySortedKV[K KType, V any] interface {
	Get(key K) *V
	AscendRange(greaterOrEqual, lessThan K) iter.Seq2[K, *V]
	Ascend() iter.Seq2[K, *V]
	Descend() iter.Seq2[K, *V]
	AscendGreaterOrEqual(greaterOrEqual K) iter.Seq2[K, *V]
	AscendLessThan(lessThan K) iter.Seq2[K, *V]
	DescendRange(lessOrEqual, greaterThan K) iter.Seq2[K, *V]
	DescendGreaterThan(greaterThan K) iter.Seq2[K, *V]
	DescendLessOrEqual(lessOrEqual K) iter.Seq2[K, *V]
}

type roSortedKV[K KType, V any] struct {
	bucket       *bolt.Bucket
	serializeK   func(K) []byte
	deserializeK func([]byte) K
}

func (skv *roSortedKV[K, V]) Get(key K) *V {
	if skv.bucket == nil {
		return nil
	}

	bd := skv.bucket.Get(skv.serializeK(key))
	if bd == nil {
		return nil
	}
	dbv := kvDeserializeV[V](bd)
	return &dbv
}

func (skv *roSortedKV[K, V]) deserializeIter(i iter.Seq2[[]byte, []byte]) iter.Seq2[K, *V] {
	return func(yield func(K, *V) bool) {
		for k, v := range i {
			dv := kvDeserializeV[V](v)
			if !yield(skv.deserializeK(k), &dv) {
				return
			}
		}
	}
}

func (skv *roSortedKV[K, V]) AscendRange(greaterOrEqual, lessThan K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketAscendRange(
		skv.bucket,
		skv.serializeK(greaterOrEqual),
		skv.serializeK(lessThan),
	))
}

func (skv *roSortedKV[K, V]) Ascend() iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketAscend(
		skv.bucket,
	))
}

func (skv *roSortedKV[K, V]) Descend() iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketDescend(
		skv.bucket,
	))
}

func (skv *roSortedKV[K, V]) AscendGreaterOrEqual(greaterOrEqual K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketAscendGreaterOrEqual(
		skv.bucket,
		skv.serializeK(greaterOrEqual),
	))
}

func (skv *roSortedKV[K, V]) AscendLessThan(lessThan K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketAscendLessThan(
		skv.bucket,
		skv.serializeK(lessThan),
	))
}

func (skv *roSortedKV[K, V]) DescendRange(lessOrEqual, greaterThan K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketDescendRange(
		skv.bucket,
		skv.serializeK(lessOrEqual),
		skv.serializeK(greaterThan),
	))
}

func (skv *roSortedKV[K, V]) DescendGreaterThan(greaterThan K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketDescendGreaterThan(
		skv.bucket,
		skv.serializeK(greaterThan),
	))
}

func (skv *roSortedKV[K, V]) DescendLessOrEqual(lessOrEqual K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketDescendLessOrEqual(
		skv.bucket,
		skv.serializeK(lessOrEqual),
	))
}

func GlobOpenReadOnlyKV[K KType, V any, ST any](
	reader *GlobReducerReader[ST],
	tx *bolt.Tx,
	kvName string,
) ReadOnlySortedKV[K, V] {
	glob := tx.Bucket(globalReducersBucket)
	reducerBucket := glob.Bucket([]byte(reader.name))
	kvParent := reducerBucket.Bucket(kvBucket)
	var bucket *bolt.Bucket = nil
	if kvParent != nil {
		bucket = kvParent.Bucket([]byte(kvName))
	}

	serializeK, deserializeK := getKTypeSerDe[K]()

	return &roSortedKV[K, V]{
		bucket:       bucket,
		serializeK:   serializeK,
		deserializeK: deserializeK,
	}
}
