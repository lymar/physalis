package bucketproxy

import (
	"cmp"
	"iter"
	"log/slog"
	"slices"

	"github.com/google/btree"
	bolt "go.etcd.io/bbolt"
)

type BucketProxy[K cmp.Ordered, V any] struct {
	bucket       *bolt.Bucket
	proxy        *btree.BTreeG[*proxyVal[K, V]]
	serializeK   func(*K) []byte
	deserializeK func([]byte) K
	serializeV   func(*V) []byte
	deserializeV func([]byte) V
}

type proxyVal[K cmp.Ordered, V any] struct {
	key    K
	data   *V
	exists bool
}

func New[K cmp.Ordered, V any](
	bucket *bolt.Bucket,
	serializeK func(*K) []byte,
	deserializeK func([]byte) K,
	serializeV func(*V) []byte,
	deserializeV func([]byte) V) BucketProxy[K, V] {
	return BucketProxy[K, V]{
		bucket,
		btree.NewG(64, func(a *proxyVal[K, V], b *proxyVal[K, V]) bool {
			return a.key < b.key
		}),
		serializeK,
		deserializeK,
		serializeV,
		deserializeV,
	}
}

func (bp *BucketProxy[K, V]) Get(key K) *V {
	val, found := bp.proxy.Get(&proxyVal[K, V]{key: key})
	if found {
		if !val.exists {
			return nil
		} else {
			return val.data
		}
	}

	if bp.bucket == nil {
		return nil
	}

	bd := bp.bucket.Get(bp.serializeK(&key))
	if bd == nil {
		return nil
	}
	dbv := bp.deserializeV(bd)
	return &dbv
}

func (bp *BucketProxy[K, V]) Delete(key K) {
	bp.proxy.ReplaceOrInsert(&proxyVal[K, V]{key, nil, false})
}

func (bp *BucketProxy[K, V]) Put(key K, data *V) {
	bp.proxy.ReplaceOrInsert(&proxyVal[K, V]{key, data, true})
}

func emptyIterator(yield func([]byte, []byte) bool) {}

func BucketAscendRange(bucket *bolt.Bucket, greaterOrEqual, lessThan []byte) iter.Seq2[[]byte, []byte] {
	if bucket == nil {
		return emptyIterator
	}

	c := bucket.Cursor()
	nextK, nextV := c.Seek(greaterOrEqual)
	return func(yield func([]byte, []byte) bool) {
		for nextK != nil {
			if slices.Compare(nextK, lessThan) < 0 {
				if !yield(nextK, nextV) {
					return
				}
				nextK, nextV = c.Next()
			} else {
				break
			}
		}
	}
}

func (bp *BucketProxy[K, V]) proxyAscendRange(greaterOrEqual, lessThan K) iter.Seq[*proxyVal[K, V]] {
	return func(yield func(*proxyVal[K, V]) bool) {
		bp.proxy.AscendRange(&proxyVal[K, V]{key: greaterOrEqual}, &proxyVal[K, V]{key: lessThan}, func(item *proxyVal[K, V]) bool {
			return yield(item)
		})
	}
}

func (bp *BucketProxy[K, V]) AscendRange(greaterOrEqual, lessThan K) iter.Seq2[K, *V] {
	return bp.mergeIters(
		BucketAscendRange(
			bp.bucket,
			bp.serializeK(&greaterOrEqual),
			bp.serializeK(&lessThan)),
		bp.proxyAscendRange(greaterOrEqual, lessThan),
		false)
}

func BucketAscend(bucket *bolt.Bucket) iter.Seq2[[]byte, []byte] {
	if bucket == nil {
		return emptyIterator
	}

	c := bucket.Cursor()
	nextK, nextV := c.First()
	return func(yield func([]byte, []byte) bool) {
		for nextK != nil {
			if !yield(nextK, nextV) {
				return
			}
			nextK, nextV = c.Next()
		}
	}
}

func (bp *BucketProxy[K, V]) proxyAscend() iter.Seq[*proxyVal[K, V]] {
	return func(yield func(*proxyVal[K, V]) bool) {
		bp.proxy.Ascend(func(item *proxyVal[K, V]) bool {
			return yield(item)
		})
	}
}

func (bp *BucketProxy[K, V]) Ascend() iter.Seq2[K, *V] {
	return bp.mergeIters(
		BucketAscend(bp.bucket),
		bp.proxyAscend(),
		false)
}

func BucketDescend(bucket *bolt.Bucket) iter.Seq2[[]byte, []byte] {
	if bucket == nil {
		return emptyIterator
	}

	c := bucket.Cursor()
	nextK, nextV := c.Last()
	return func(yield func([]byte, []byte) bool) {
		for nextK != nil {
			if !yield(nextK, nextV) {
				return
			}
			nextK, nextV = c.Prev()
		}
	}
}

func (bp *BucketProxy[K, V]) proxyDescend() iter.Seq[*proxyVal[K, V]] {
	return func(yield func(*proxyVal[K, V]) bool) {
		bp.proxy.Descend(func(item *proxyVal[K, V]) bool {
			return yield(item)
		})
	}
}

func (bp *BucketProxy[K, V]) Descend() iter.Seq2[K, *V] {
	return bp.mergeIters(
		BucketDescend(bp.bucket),
		bp.proxyDescend(),
		true)
}

func BucketAscendGreaterOrEqual(bucket *bolt.Bucket, greaterOrEqual []byte) iter.Seq2[[]byte, []byte] {
	if bucket == nil {
		return emptyIterator
	}

	c := bucket.Cursor()
	nextK, nextV := c.Seek(greaterOrEqual)
	return func(yield func([]byte, []byte) bool) {
		for nextK != nil {
			if !yield(nextK, nextV) {
				return
			}
			nextK, nextV = c.Next()
		}
	}
}

func (bp *BucketProxy[K, V]) proxyAscendGreaterOrEqual(greaterOrEqual K) iter.Seq[*proxyVal[K, V]] {
	return func(yield func(*proxyVal[K, V]) bool) {
		bp.proxy.AscendGreaterOrEqual(
			&proxyVal[K, V]{key: greaterOrEqual},
			func(item *proxyVal[K, V]) bool {
				return yield(item)
			})
	}
}

func (bp *BucketProxy[K, V]) AscendGreaterOrEqual(greaterOrEqual K) iter.Seq2[K, *V] {
	return bp.mergeIters(
		BucketAscendGreaterOrEqual(bp.bucket,
			bp.serializeK(&greaterOrEqual)),
		bp.proxyAscendGreaterOrEqual(greaterOrEqual),
		false)
}

func BucketAscendLessThan(bucket *bolt.Bucket, lessThan []byte) iter.Seq2[[]byte, []byte] {
	if bucket == nil {
		return emptyIterator
	}

	c := bucket.Cursor()
	nextK, nextV := c.First()
	return func(yield func([]byte, []byte) bool) {
		for nextK != nil {
			if slices.Compare(nextK, lessThan) < 0 {
				if !yield(nextK, nextV) {
					return
				}
				nextK, nextV = c.Next()
			} else {
				break
			}
		}
	}
}

func (bp *BucketProxy[K, V]) proxyAscendLessThan(lessThan K) iter.Seq[*proxyVal[K, V]] {
	return func(yield func(*proxyVal[K, V]) bool) {
		bp.proxy.AscendLessThan(&proxyVal[K, V]{key: lessThan}, func(item *proxyVal[K, V]) bool {
			return yield(item)
		})
	}
}

func (bp *BucketProxy[K, V]) AscendLessThan(lessThan K) iter.Seq2[K, *V] {
	return bp.mergeIters(
		BucketAscendLessThan(bp.bucket,
			bp.serializeK(&lessThan)),
		bp.proxyAscendLessThan(lessThan),
		false)
}

func BucketDescendRange(bucket *bolt.Bucket, lessOrEqual, greaterThan []byte) iter.Seq2[[]byte, []byte] {
	if bucket == nil {
		return emptyIterator
	}

	c := bucket.Cursor()
	nextK, nextV := c.Seek(lessOrEqual)

	if nextK == nil {
		nextK, nextV = c.Last()
	} else {
		if slices.Compare(nextK, lessOrEqual) > 0 {
			nextK, nextV = c.Prev()
		}
	}

	return func(yield func([]byte, []byte) bool) {
		for nextK != nil {
			if slices.Compare(nextK, greaterThan) > 0 {
				if !yield(nextK, nextV) {
					return
				}
				nextK, nextV = c.Prev()
			} else {
				break
			}
		}
	}
}

func (bp *BucketProxy[K, V]) proxyDescendRange(lessOrEqual, greaterThan K) iter.Seq[*proxyVal[K, V]] {
	return func(yield func(*proxyVal[K, V]) bool) {
		bp.proxy.DescendRange(&proxyVal[K, V]{key: lessOrEqual}, &proxyVal[K, V]{key: greaterThan}, func(item *proxyVal[K, V]) bool {
			return yield(item)
		})
	}
}

func (bp *BucketProxy[K, V]) DescendRange(lessOrEqual, greaterThan K) iter.Seq2[K, *V] {
	return bp.mergeIters(
		BucketDescendRange(bp.bucket,
			bp.serializeK(&lessOrEqual),
			bp.serializeK(&greaterThan)),
		bp.proxyDescendRange(lessOrEqual, greaterThan),
		true)
}

func BucketDescendGreaterThan(bucket *bolt.Bucket, greaterThan []byte) iter.Seq2[[]byte, []byte] {
	if bucket == nil {
		return emptyIterator
	}

	c := bucket.Cursor()
	nextK, nextV := c.Last()

	return func(yield func([]byte, []byte) bool) {
		for nextK != nil {
			if slices.Compare(nextK, greaterThan) > 0 {
				if !yield(nextK, nextV) {
					return
				}
				nextK, nextV = c.Prev()
			} else {
				break
			}
		}
	}
}

func (bp *BucketProxy[K, V]) proxyDescendGreaterThan(greaterThan K) iter.Seq[*proxyVal[K, V]] {
	return func(yield func(*proxyVal[K, V]) bool) {
		bp.proxy.DescendGreaterThan(&proxyVal[K, V]{key: greaterThan}, func(item *proxyVal[K, V]) bool {
			return yield(item)
		})
	}
}

func (bp *BucketProxy[K, V]) DescendGreaterThan(greaterThan K) iter.Seq2[K, *V] {
	return bp.mergeIters(
		BucketDescendGreaterThan(bp.bucket,
			bp.serializeK(&greaterThan)),
		bp.proxyDescendGreaterThan(greaterThan),
		true)
}

func BucketDescendLessOrEqual(bucket *bolt.Bucket, lessOrEqual []byte) iter.Seq2[[]byte, []byte] {
	if bucket == nil {
		return emptyIterator
	}

	c := bucket.Cursor()
	nextK, nextV := c.Seek(lessOrEqual)

	if nextK == nil {
		nextK, nextV = c.Last()
	} else {
		if slices.Compare(nextK, lessOrEqual) > 0 {
			nextK, nextV = c.Prev()
		}
	}

	return func(yield func([]byte, []byte) bool) {
		for nextK != nil {
			if !yield(nextK, nextV) {
				return
			}
			nextK, nextV = c.Prev()
		}
	}
}

func (bp *BucketProxy[K, V]) proxyDescendLessOrEqual(lessOrEqual K) iter.Seq[*proxyVal[K, V]] {
	return func(yield func(*proxyVal[K, V]) bool) {
		bp.proxy.DescendLessOrEqual(&proxyVal[K, V]{key: lessOrEqual}, func(item *proxyVal[K, V]) bool {
			return yield(item)
		})
	}
}

func (bp *BucketProxy[K, V]) DescendLessOrEqual(lessOrEqual K) iter.Seq2[K, *V] {
	return bp.mergeIters(
		BucketDescendLessOrEqual(bp.bucket,
			bp.serializeK(&lessOrEqual)),
		bp.proxyDescendLessOrEqual(lessOrEqual),
		true)
}

func (bp *BucketProxy[K, V]) mergeIters(
	fromBucket iter.Seq2[[]byte, []byte],
	fromProxy iter.Seq[*proxyVal[K, V]],
	reverse bool) iter.Seq2[K, *V] {
	return func(yield func(K, *V) bool) {
		nextBucket, stopBucket := iter.Pull2(fromBucket)
		defer stopBucket()

		nextProxy, stopProxy := iter.Pull(fromProxy)
		defer stopProxy()

		bucketK, bucketV, bucketOk := nextBucket()
		proxyV, proxyOk := nextProxy()

		for {
			if !bucketOk && !proxyOk {
				return
			} else if bucketOk && !proxyOk {
				dbv := bp.deserializeV(bucketV)
				if !yield(bp.deserializeK(bucketK), &dbv) {
					return
				}
				bucketK, bucketV, bucketOk = nextBucket()
			} else if !bucketOk && proxyOk {
				if proxyV.exists {
					if !yield(proxyV.key, proxyV.data) {
						return
					}
				}
				proxyV, proxyOk = nextProxy()
			} else {
				bucketKVal := bp.deserializeK(bucketK)
				cmp := compare(bucketKVal, proxyV.key)
				if reverse {
					cmp = -cmp
				}
				if cmp == 0 {
					if proxyV.exists {
						if !yield(proxyV.key, proxyV.data) {
							return
						}
					}
					bucketK, bucketV, bucketOk = nextBucket()
					proxyV, proxyOk = nextProxy()
				} else if cmp < 0 {
					dbv := bp.deserializeV(bucketV)
					if !yield(bucketKVal, &dbv) {
						return
					}
					bucketK, bucketV, bucketOk = nextBucket()
				} else {
					if proxyV.exists {
						if !yield(proxyV.key, proxyV.data) {
							return
						}
					}
					proxyV, proxyOk = nextProxy()
				}
			}
		}
	}
}

func compare[T cmp.Ordered](a, b T) int {
	if a == b {
		return 0
	} else if a < b {
		return -1
	} else {
		return 1
	}
}

func (bp *BucketProxy[K, V]) WriteProxyToDb(bucket *bolt.Bucket) error {
	for e := range bp.proxyAscend() {
		if e.exists {
			slog.Debug("BucketProxy: writing to db", "key", e.key)
			err := bucket.Put(bp.serializeK(&e.key), bp.serializeV(e.data))
			if err != nil {
				return err
			}
		} else {
			slog.Debug("BucketProxy: deleting from db", "key", e.key)
			err := bucket.Delete(bp.serializeK(&e.key))
			if err != nil {
				return err
			}
		}
	}

	return nil
}
