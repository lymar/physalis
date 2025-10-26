package physalis

import (
	"iter"

	"github.com/lymar/physalis/internal/bucketproxy"
	bolt "go.etcd.io/bbolt"
)

type SortedKVView[K KType, V any] interface {
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

type sortedKVView[K KType, V any] struct {
	bucket       *bolt.Bucket
	serializeK   func(K) []byte
	deserializeK func([]byte) K
}

func (skv *sortedKVView[K, V]) Get(key K) *V {
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

func (skv *sortedKVView[K, V]) deserializeIter(i iter.Seq2[[]byte, []byte]) iter.Seq2[K, *V] {
	return func(yield func(K, *V) bool) {
		for k, v := range i {
			dv := kvDeserializeV[V](v)
			if !yield(skv.deserializeK(k), &dv) {
				return
			}
		}
	}
}

func (skv *sortedKVView[K, V]) AscendRange(greaterOrEqual, lessThan K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketAscendRange(
		skv.bucket,
		skv.serializeK(greaterOrEqual),
		skv.serializeK(lessThan),
	))
}

func (skv *sortedKVView[K, V]) Ascend() iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketAscend(
		skv.bucket,
	))
}

func (skv *sortedKVView[K, V]) Descend() iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketDescend(
		skv.bucket,
	))
}

func (skv *sortedKVView[K, V]) AscendGreaterOrEqual(greaterOrEqual K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketAscendGreaterOrEqual(
		skv.bucket,
		skv.serializeK(greaterOrEqual),
	))
}

func (skv *sortedKVView[K, V]) AscendLessThan(lessThan K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketAscendLessThan(
		skv.bucket,
		skv.serializeK(lessThan),
	))
}

func (skv *sortedKVView[K, V]) DescendRange(lessOrEqual, greaterThan K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketDescendRange(
		skv.bucket,
		skv.serializeK(lessOrEqual),
		skv.serializeK(greaterThan),
	))
}

func (skv *sortedKVView[K, V]) DescendGreaterThan(greaterThan K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketDescendGreaterThan(
		skv.bucket,
		skv.serializeK(greaterThan),
	))
}

func (skv *sortedKVView[K, V]) DescendLessOrEqual(lessOrEqual K) iter.Seq2[K, *V] {
	return skv.deserializeIter(bucketproxy.BucketDescendLessOrEqual(
		skv.bucket,
		skv.serializeK(lessOrEqual),
	))
}
