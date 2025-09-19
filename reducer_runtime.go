package physalis

import (
	"encoding/binary"
	"iter"
	"log/slog"
	"reflect"
	"unsafe"

	"github.com/fxamacker/cbor/v2"
	"github.com/lymar/physalis/internal/bucketproxy"
	bolt "go.etcd.io/bbolt"
)

type ReducerRuntime struct {
	db          *bolt.DB
	tx          *bolt.Tx
	reducerName string
	key         string
	kvMaps      map[string]kvMapData
}

type KType interface {
	~int32 | ~int64 | ~uint32 | ~uint64 | ~string
}

type kvMapData struct {
	proxy     any
	writeToDb func() func(writeTx *bolt.Tx) error
}

func newReducerRuntime(db *bolt.DB, reducerName string, key string) *ReducerRuntime {
	return &ReducerRuntime{
		db,
		nil,
		reducerName,
		key,
		nil,
	}
}

func (rr *ReducerRuntime) close() error {
	if rr.tx != nil {
		slog.Debug("closing reducer runtime transaction", "reducer", rr.reducerName)
		return rr.tx.Rollback()
	}
	return nil
}

type SortedKV[K KType, V any] interface {
	Get(key K) *V
	Delete(key K)
	Put(key K, data *V)
	AscendRange(greaterOrEqual, lessThan K) iter.Seq2[K, *V]
	Ascend() iter.Seq2[K, *V]
	Descend() iter.Seq2[K, *V]
	AscendGreaterOrEqual(greaterOrEqual K) iter.Seq2[K, *V]
	AscendLessThan(lessThan K) iter.Seq2[K, *V]
	DescendRange(lessOrEqual, greaterThan K) iter.Seq2[K, *V]
	DescendGreaterThan(greaterThan K) iter.Seq2[K, *V]
	DescendLessOrEqual(lessOrEqual K) iter.Seq2[K, *V]
}

func OpenKV[K KType, V any](rr *ReducerRuntime, kvName string) SortedKV[K, V] {
	if rr.kvMaps == nil {
		rr.kvMaps = make(map[string]kvMapData)
	}

	prev, found := rr.kvMaps[kvName]
	if found {
		prevProxy, ok := prev.proxy.(*bucketproxy.BucketProxy[K, V])
		if !ok {
			panic("kv map with the same name but different K, V types")
		}
		return prevProxy
	}

	if rr.tx == nil {
		tx, err := rr.db.Begin(false)
		if err != nil {
			panic(err)
		}
		rr.tx = tx
	}

	var bucket *bolt.Bucket = nil

	reducers := rr.tx.Bucket(reducersBucket)
	reducerBuck := reducers.Bucket([]byte(rr.reducerName))
	kvsBuck := reducerBuck.Bucket(kvsBucket)

	if kvsBuck != nil {
		eventGroupBucket := kvsBuck.Bucket([]byte(rr.key))
		if eventGroupBucket != nil {
			bucket = eventGroupBucket.Bucket([]byte(kvName))
		}
	}

	serializeK, deserializeK := getKTypeSerDe[K]()

	bp := bucketproxy.New[K, V](
		bucket,
		func(k *K) []byte {
			return serializeK(*k)
		},
		deserializeK,
		kvSerizlizeV,
		kvDeserializeV,
	)

	rr.kvMaps[kvName] = kvMapData{
		proxy: &bp,
		writeToDb: func() func(writeTx *bolt.Tx) error {
			writeProxyToDBFunc := bp.WriteProxyToDb()

			return func(writeTx *bolt.Tx) error {
				reducers := writeTx.Bucket(reducersBucket)
				reducerBuck := reducers.Bucket([]byte(rr.reducerName))
				kvsBuck, err := reducerBuck.CreateBucketIfNotExists(kvsBucket)
				if err != nil {
					return err
				}

				eventGroupBucket, err := kvsBuck.CreateBucketIfNotExists([]byte(rr.key))
				if err != nil {
					return err
				}

				bucket, err = eventGroupBucket.CreateBucketIfNotExists([]byte(kvName))
				if err != nil {
					return err
				}

				if err := writeProxyToDBFunc(bucket); err != nil {
					return err
				}

				return nil
			}
		},
	}

	return &bp
}

func kvSerizlizeV[V any](data *V) []byte {
	bin, err := cbor.Marshal(data)
	if err != nil {
		panic(err)
	}
	return bin
}

func kvDeserializeV[V any](rawData []byte) V {
	var data V
	if err := cbor.Unmarshal(rawData, &data); err != nil {
		panic(err)
	}
	return data
}

func getKTypeSerDe[K KType]() (serializer func(k K) []byte,
	deserializer func(data []byte) K) {

	var zero K
	switch reflect.TypeOf(zero).Kind() {
	case reflect.Int:
		serializer = func(k K) []byte {
			v := *(*int)(unsafe.Pointer(&k))
			v64 := int64(v)
			u := uint64(v64) ^ (1 << 63)
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, u)
			return buf
		}
		deserializer = func(b []byte) K {
			u := binary.BigEndian.Uint64(b)
			v64 := int64(u ^ (1 << 63))
			var out K
			*(*int)(unsafe.Pointer(&out)) = int(v64)
			return out
		}
	case reflect.Int32:
		serializer = func(k K) []byte {
			v := *(*int32)(unsafe.Pointer(&k))
			u := uint32(v) ^ (1 << 31)
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, u)
			return buf
		}
		deserializer = func(b []byte) K {
			u := binary.BigEndian.Uint32(b)
			v := int32(u ^ (1 << 31))
			var out K
			*(*int32)(unsafe.Pointer(&out)) = v
			return out
		}
	case reflect.Int64:
		serializer = func(k K) []byte {
			v := *(*int64)(unsafe.Pointer(&k))
			u := uint64(v) ^ (1 << 63)
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, u)
			return buf
		}
		deserializer = func(b []byte) K {
			u := binary.BigEndian.Uint64(b)
			v := int64(u ^ (1 << 63))
			var out K
			*(*int64)(unsafe.Pointer(&out)) = v
			return out
		}
	case reflect.Uint32:
		serializer = func(k K) []byte {
			v := *(*uint32)(unsafe.Pointer(&k))
			buf := make([]byte, 4)
			binary.BigEndian.PutUint32(buf, v)
			return buf
		}
		deserializer = func(b []byte) K {
			u := binary.BigEndian.Uint32(b)
			var out K
			*(*uint32)(unsafe.Pointer(&out)) = u
			return out
		}
	case reflect.Uint64:
		serializer = func(k K) []byte {
			v := *(*uint64)(unsafe.Pointer(&k))
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, v)
			return buf
		}
		deserializer = func(b []byte) K {
			u := binary.BigEndian.Uint64(b)
			var out K
			*(*uint64)(unsafe.Pointer(&out)) = u
			return out
		}
	case reflect.String:
		serializer = func(k K) []byte {
			return []byte(*(*string)(unsafe.Pointer(&k)))
		}
		deserializer = func(b []byte) K {
			s := string(b)
			var out K
			*(*string)(unsafe.Pointer(&out)) = s
			return out
		}
	default:
		panic("unsupported K in getKTypeSerDe")
	}

	return
}
