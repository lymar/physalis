package bucketproxy

import (
	"iter"
	"slices"

	"github.com/google/btree"
	bolt "go.etcd.io/bbolt"
)

type BucketProxy struct {
	bucket *bolt.Bucket
	proxy  *btree.BTreeG[*proxyVal]
}

func New(bucket *bolt.Bucket) BucketProxy {
	return BucketProxy{
		bucket: bucket,
		proxy:  btree.NewG(64, byKeys),
	}
}

type proxyVal struct {
	key    []byte
	data   []byte
	exists bool
}

func byKeys(a, b *proxyVal) bool {
	return slices.Compare(a.key, b.key) < 0
}

func (bp *BucketProxy) Get(key []byte) []byte {
	val, found := bp.proxy.Get(&proxyVal{key: key})
	if found {
		if !val.exists {
			return nil
		} else {
			return val.data
		}
	}
	return bp.bucket.Get(key)
}

func (bp *BucketProxy) Delete(key []byte) {
	bp.proxy.ReplaceOrInsert(&proxyVal{key, nil, false})
}

func (bp *BucketProxy) Put(key []byte, data []byte) {
	bp.proxy.ReplaceOrInsert(&proxyVal{key, data, true})
}

func (bp *BucketProxy) bucketAscendRange(greaterOrEqual, lessThan []byte) iter.Seq2[[]byte, []byte] {
	c := bp.bucket.Cursor()
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

func (bp *BucketProxy) proxyAscendRange(greaterOrEqual, lessThan []byte) iter.Seq[*proxyVal] {
	return func(yield func(*proxyVal) bool) {
		bp.proxy.AscendRange(&proxyVal{key: greaterOrEqual}, &proxyVal{key: lessThan}, func(item *proxyVal) bool {
			return yield(item)
		})
	}
}

func (bp *BucketProxy) AscendRange(greaterOrEqual, lessThan []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		nextBucket, stopBucket := iter.Pull2(bp.bucketAscendRange(greaterOrEqual, lessThan))
		defer stopBucket()

		nextProxy, stopProxy := iter.Pull(bp.proxyAscendRange(greaterOrEqual, lessThan))
		defer stopProxy()

		bucketK, bucketV, bucketOk := nextBucket()
		proxyV, proxyOk := nextProxy()

		for {
			if !bucketOk && !proxyOk {
				return
			} else if bucketOk && !proxyOk {
				if !yield(bucketK, bucketV) {
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
				cmp := slices.Compare(bucketK, proxyV.key)
				if cmp == 0 {
					if proxyV.exists {
						if !yield(proxyV.key, proxyV.data) {
							return
						}
					}
					bucketK, bucketV, bucketOk = nextBucket()
					proxyV, proxyOk = nextProxy()
				} else if cmp < 0 {
					if !yield(bucketK, bucketV) {
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
