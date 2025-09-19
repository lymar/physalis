package bucketproxy

import (
	"encoding/binary"
	"fmt"
	"iter"
	"math/rand"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/btree"
	"github.com/lymar/physalis/internal"
	bolt "go.etcd.io/bbolt"
)

// go test ./internal/bucketproxy -v

var bucketName = []byte("test")

func TestBucketProxyBase(t *testing.T) {
	err := withTempDb(func(db *bolt.DB) {
		if err := fillDbWithSomeData(db); err != nil {
			t.Fatal(err)
		}

		err := db.View(func(tx *bolt.Tx) error {
			bp := makeTestBP(
				tx.Bucket(bucketName))

			if v := bp.Get(15); *v != "fifteen" {
				t.Errorf("unexpected value for key 15: %v", v)
			}
			if v := bp.Get(1000); v != nil {
				t.Errorf("unexpected value for key 1000: %v", v)
			}

			bp.Delete(6)
			if v := bp.Get(6); v != nil {
				t.Errorf("unexpected value for key 6 after delete: %v", v)
			}

			bp.Delete(2000)
			bp.Put(17, mkptp("seventeen"))
			if v := bp.Get(17); *v != "seventeen" {
				t.Errorf("unexpected value for key 17 after put: %v", v)
			}

			bp.Put(2, mkptp("<< TWO >>"))

			data1 := readFromBPIterator(bp.AscendRange(1, 100000))
			if !slices.Equal(data1, []testPair{
				{1, "one"},
				{2, "<< TWO >>"},
				{5, "five"},
				{15, "fifteen"},
				{17, "seventeen"},
				{42, "life"}}) {

				t.Errorf("unexpected data1: %v", data1)
			}

			data2 := readFromBPIterator(bp.AscendRange(2, 15))
			if !slices.Equal(data2, []testPair{
				{2, "<< TWO >>"},
				{5, "five"}}) {

				t.Errorf("unexpected data2: %v", data2)
			}

			data3 := readFromBPIterator(bp.AscendRange(4, 15))
			if !slices.Equal(data3, []testPair{
				{5, "five"}}) {

				t.Errorf("unexpected data3: %v", data3)
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestBucketProxyNilIter(t *testing.T) {
	bp := New[uint64, string](
		nil,
		serializeUint64,
		deserializeUint64,
		serializeString,
		deserializeString)
	bp.Put(10, mkptp("ten"))
	bp.Put(20, mkptp("twenty"))
	bp.Put(30, mkptp("thirty"))

	if v := bp.Get(10); *v != "ten" {
		t.Errorf("unexpected value for key 10: %v", v)
	}

	if v := bp.Get(11); v != nil {
		t.Errorf("unexpected value for key 11: %v", v)
	}

	brRes := readFromBPIterator(bp.AscendRange(10, 30))

	if !slices.Equal(brRes, []testPair{
		{10, "ten"},
		{20, "twenty"}}) {

		t.Errorf("AscendRange: unexpected data: %v", brRes)
	}

	brRes = readFromBPIterator(bp.Ascend())
	if !slices.Equal(brRes, []testPair{
		{10, "ten"},
		{20, "twenty"},
		{30, "thirty"}}) {

		t.Errorf("Ascend: unexpected data: %v", brRes)
	}

	brRes = readFromBPIterator(bp.Descend())
	if !slices.Equal(brRes, []testPair{
		{30, "thirty"},
		{20, "twenty"},
		{10, "ten"}}) {

		t.Errorf("Descend: unexpected data: %v", brRes)
	}

	brRes = readFromBPIterator(bp.AscendGreaterOrEqual(30))
	if !slices.Equal(brRes, []testPair{
		{30, "thirty"}}) {

		t.Errorf("AscendGreaterOrEqual: unexpected data: %v", brRes)
	}

	brRes = readFromBPIterator(bp.AscendLessThan(20))
	if !slices.Equal(brRes, []testPair{
		{10, "ten"}}) {

		t.Errorf("AscendLessThan: unexpected data: %v", brRes)
	}

	brRes = readFromBPIterator(bp.DescendRange(30, 10))
	if !slices.Equal(brRes, []testPair{
		{30, "thirty"},
		{20, "twenty"}}) {

		t.Errorf("DescendRange: unexpected data: %v", brRes)
	}

	brRes = readFromBPIterator(bp.DescendGreaterThan(20))
	if !slices.Equal(brRes, []testPair{
		{30, "thirty"}}) {

		t.Errorf("DescendGreaterThan: unexpected data: %v", brRes)
	}

	brRes = readFromBPIterator(bp.DescendLessOrEqual(20))
	if !slices.Equal(brRes, []testPair{
		{20, "twenty"},
		{10, "ten"}}) {

		t.Errorf("DescendLessOrEqual: unexpected data: %v", brRes)
	}
}

func TestRandomizedIter(t *testing.T) {
	for _, v := range []int64{100, 101, 110, 123, 555, 800} {
		t.Run(
			fmt.Sprintf("seed=%d", v),
			func(t *testing.T) {
				randomizedIter(t, v)
			})
	}
}

func randomizedIter(t *testing.T, seed int64) {
	err := withTempDb(func(db *bolt.DB) {
		refBTree := btree.NewG(64,
			func(a *refBTreeItem, b *refBTreeItem) bool {
				return a.key < b.key
			})

		err := db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(bucketName)
			for k, v := range getRandKV(seed) {
				bucket.Put(serializeUint64(&k), serializeString(&v))
				refBTree.ReplaceOrInsert(&refBTreeItem{key: k, val: v})
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		err = db.View(func(tx *bolt.Tx) error {
			bp := makeTestBP(
				tx.Bucket(bucketName))
			for k, v := range getRandUpdates(seed + 10) {
				if v != nil {
					bp.Put(k, v)
					refBTree.ReplaceOrInsert(&refBTreeItem{key: k, val: *v})
				} else {
					bp.Delete(k)
					refBTree.Delete(&refBTreeItem{key: k})
				}
			}

			// random get checks
			for k := range getRandK(seed + 20) {
				v1 := bp.Get(k)
				ri, _ := refBTree.Get(&refBTreeItem{key: k})
				if v1 != nil || ri != nil {
					if *v1 != ri.val {
						t.Errorf("mismatch for key %d: bp=%v, ref=%v", k, v1, ri.val)
					}
				}
			}

			checkAscend(t, &bp, refBTree)
			checkAscendRange(t, seed+30, &bp, refBTree)
			checkAscendGreaterOrEqual(t, seed+40, &bp, refBTree)
			checkAscendLessThan(t, seed+50, &bp, refBTree)

			checkDescend(t, &bp, refBTree)
			checkDescendRange(t, seed+60, &bp, refBTree)
			checkDescendGreaterThan(t, seed+70, &bp, refBTree)
			checkDescendLessOrEqual(t, seed+80, &bp, refBTree)

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

func checkAscendRange(t *testing.T, seed int64, bp *BucketProxy[uint64, string],
	refBTree *btree.BTreeG[*refBTreeItem]) {

	r := rand.New(rand.NewSource(seed))

	for range 100 {
		greaterOrEqual := uint64(r.Intn(1000))
		lessThan := uint64(r.Intn(1000))

		brRes := readFromBPIterator(bp.AscendRange(greaterOrEqual, lessThan))

		var refRes []testPair
		refBTree.AscendRange(
			&refBTreeItem{key: greaterOrEqual},
			&refBTreeItem{key: lessThan},
			func(item *refBTreeItem) bool {
				refRes = append(refRes, testPair{item.key, item.val})
				return true
			})

		if !slices.Equal(brRes, refRes) {
			t.Errorf("mismatch for AscendRange(%d, %d): bp=%v, ref=%v",
				greaterOrEqual, lessThan, brRes, refRes)
		}
	}
}

func checkAscend(t *testing.T, bp *BucketProxy[uint64, string],
	refBTree *btree.BTreeG[*refBTreeItem]) {

	brRes := readFromBPIterator(bp.Ascend())

	var refRes []testPair
	refBTree.Ascend(
		func(item *refBTreeItem) bool {
			refRes = append(refRes, testPair{item.key, item.val})
			return true
		})

	if !slices.Equal(brRes, refRes) {
		t.Errorf("mismatch for Ascend: bp=%v, ref=%v",
			brRes, refRes)
	}
}

func checkDescend(t *testing.T, bp *BucketProxy[uint64, string],
	refBTree *btree.BTreeG[*refBTreeItem]) {

	brRes := readFromBPIterator(bp.Descend())

	var refRes []testPair
	refBTree.Descend(
		func(item *refBTreeItem) bool {
			refRes = append(refRes, testPair{item.key, item.val})
			return true
		})

	if !slices.Equal(brRes, refRes) {
		t.Errorf("mismatch for Descend: bp=%v, ref=%v",
			brRes, refRes)
	}
}

func checkAscendGreaterOrEqual(t *testing.T, seed int64, bp *BucketProxy[uint64, string],
	refBTree *btree.BTreeG[*refBTreeItem]) {

	r := rand.New(rand.NewSource(seed))

	for range 100 {
		greaterOrEqual := uint64(r.Intn(1000))

		brRes := readFromBPIterator(bp.AscendGreaterOrEqual(greaterOrEqual))

		var refRes []testPair
		refBTree.AscendGreaterOrEqual(
			&refBTreeItem{key: greaterOrEqual},

			func(item *refBTreeItem) bool {
				refRes = append(refRes, testPair{item.key, item.val})
				return true
			})

		if !slices.Equal(brRes, refRes) {
			t.Errorf(
				"mismatch for AscendGreaterOrEqual(%d): bp=%v, ref=%v",
				greaterOrEqual, brRes, refRes)
		}
	}
}

func checkAscendLessThan(t *testing.T, seed int64, bp *BucketProxy[uint64, string],
	refBTree *btree.BTreeG[*refBTreeItem]) {

	r := rand.New(rand.NewSource(seed))

	for range 100 {
		lessThan := uint64(r.Intn(1000))

		brRes := readFromBPIterator(bp.AscendLessThan(lessThan))

		var refRes []testPair
		refBTree.AscendLessThan(
			&refBTreeItem{key: lessThan},

			func(item *refBTreeItem) bool {
				refRes = append(refRes, testPair{item.key, item.val})
				return true
			})

		if !slices.Equal(brRes, refRes) {
			t.Errorf(
				"mismatch for AscendLessThan(%d): bp=%v, ref=%v",
				lessThan, brRes, refRes)
		}
	}
}

func checkDescendRange(t *testing.T, seed int64, bp *BucketProxy[uint64, string],
	refBTree *btree.BTreeG[*refBTreeItem]) {

	r := rand.New(rand.NewSource(seed))

	for range 100 {
		lessOrEqual := uint64(r.Intn(1000))
		greaterThan := uint64(r.Intn(1000))

		brRes := readFromBPIterator(bp.DescendRange(lessOrEqual, greaterThan))

		var refRes []testPair
		refBTree.DescendRange(
			&refBTreeItem{key: lessOrEqual},
			&refBTreeItem{key: greaterThan},
			func(item *refBTreeItem) bool {
				refRes = append(refRes, testPair{item.key, item.val})
				return true
			})

		if !slices.Equal(brRes, refRes) {
			t.Errorf("mismatch for DescendRange(%d, %d): bp=%v, ref=%v",
				lessOrEqual, greaterThan, brRes, refRes)
		}
	}
}

func checkDescendGreaterThan(t *testing.T, seed int64, bp *BucketProxy[uint64, string],
	refBTree *btree.BTreeG[*refBTreeItem]) {

	r := rand.New(rand.NewSource(seed))

	for range 100 {
		greaterThan := uint64(r.Intn(1000))

		brRes := readFromBPIterator(bp.DescendGreaterThan(greaterThan))

		var refRes []testPair
		refBTree.DescendGreaterThan(
			&refBTreeItem{key: greaterThan},

			func(item *refBTreeItem) bool {
				refRes = append(refRes, testPair{item.key, item.val})
				return true
			})

		if !slices.Equal(brRes, refRes) {
			t.Errorf(
				"mismatch for DescendGreaterThan(%d): bp=%v, ref=%v",
				greaterThan, brRes, refRes)
		}
	}
}

func checkDescendLessOrEqual(t *testing.T, seed int64, bp *BucketProxy[uint64, string],
	refBTree *btree.BTreeG[*refBTreeItem]) {

	r := rand.New(rand.NewSource(seed))

	for range 100 {
		lessOrEqual := uint64(r.Intn(1000))

		brRes := readFromBPIterator(bp.DescendLessOrEqual(lessOrEqual))

		var refRes []testPair
		refBTree.DescendLessOrEqual(
			&refBTreeItem{key: lessOrEqual},

			func(item *refBTreeItem) bool {
				refRes = append(refRes, testPair{item.key, item.val})
				return true
			})

		if !slices.Equal(brRes, refRes) {
			t.Errorf(
				"mismatch for DescendLessOrEqual(%d): bp=%v, ref=%v",
				lessOrEqual, brRes, refRes)
		}
	}
}

type refBTreeItem struct {
	key uint64
	val string
}

func getRandK(seed int64) iter.Seq[uint64] {
	r := rand.New(rand.NewSource(seed))
	return func(yield func(uint64) bool) {
		count := r.Intn(1000) + 1
		for range count {
			rv := uint64(r.Intn(1000))
			if !yield(rv) {
				return
			}
		}
	}
}

func getRandKV(seed int64) iter.Seq2[uint64, string] {
	r := rand.New(rand.NewSource(seed))
	return func(yield func(uint64, string) bool) {
		count := r.Intn(1000) + 1
		for range count {
			rv := uint64(r.Intn(1000))
			if !yield(rv, fmt.Sprintf("<< %d >>", rv)) {
				return
			}
		}
	}
}

func getRandUpdates(seed int64) iter.Seq2[uint64, *string] {
	r := rand.New(rand.NewSource(seed))
	return func(yield func(uint64, *string) bool) {
		count := r.Intn(1000) + 1
		for range count {
			rv := uint64(r.Intn(1000))

			if r.Intn(10) < 3 {
				if !yield(rv, nil) {
					return
				}
			}

			s := fmt.Sprintf("<< %d - %d >>", rv, r.Intn(1000000))
			if !yield(rv, &s) {
				return
			}
		}
	}
}

func mkptp[T any](s T) *T {
	return &s
}

func fillDbWithSomeData(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)

		for _, p := range []struct {
			Key   uint64
			Value string
		}{
			{1, "one"},
			{2, "two"},
			{5, "five"},
			{6, "six"},
			{15, "fifteen"},
			{42, "life"},
		} {
			if err := bucket.Put(serializeUint64(&p.Key), serializeString(&p.Value)); err != nil {
				return err
			}
		}

		return nil
	})
}

type testPair struct {
	key   uint64
	value string
}

func readFromBPIterator(i iter.Seq2[uint64, *string]) []testPair {
	var res []testPair
	for k, v := range i {
		res = append(res, testPair{k, *v})
	}
	return res
}

func makeTestBP(bucket *bolt.Bucket) BucketProxy[uint64, string] {
	return New[uint64, string](
		bucket,
		serializeUint64,
		deserializeUint64,
		serializeString,
		deserializeString)
}

func serializeUint64(v *uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, *v)
	return b
}

func deserializeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func serializeString(v *string) []byte {
	return []byte(*v)
}

func deserializeString(b []byte) string {
	return string(b)
}

func withTempDb(cb func(*bolt.DB)) error {
	dbFile, err := internal.TempFileName("bpt_", ".db")
	if err != nil {
		return err
	}
	defer os.Remove(dbFile)

	db, err := bolt.Open(dbFile, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucketName)
		return err
	}); err != nil {
		return err
	}

	cb(db)

	return nil
}
