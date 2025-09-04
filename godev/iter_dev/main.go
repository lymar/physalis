package main

import (
	"encoding/binary"
	"fmt"
	"iter"
	"reflect"

	"slices"

	"github.com/google/btree"
)

type Item struct {
	Key []byte
	Val string
}

func byKeys(a, b *Item) bool {
	return slices.Compare(a.Key, b.Key) < 0
}

type MyInt int
type MyString string

func main() {
	t := btree.NewG(64, byKeys)
	k1 := u64be(123)
	t.ReplaceOrInsert(&Item{
		Key: k1[:],
		Val: "qwe",
	})
	k2 := u64be(321)
	t.ReplaceOrInsert(&Item{
		Key: k2[:],
		Val: "asd",
	})
	k3 := u64be(125)
	t.ReplaceOrInsert(&Item{
		Key: k3[:],
		Val: "zzz",
	})
	fmt.Printf("qwe: %+v\n", t)

	for v := range Items(t) {
		fmt.Println("item", v)
	}

	NewSortedMap[MyInt]()
	NewSortedMap[MyString]()
	NewSortedMap[string]()
	NewSortedMap[int8]()
}

func u64be(v uint64) (res [8]byte) {
	binary.BigEndian.PutUint64(res[:], v)
	return
}

func Items(t *btree.BTreeG[*Item]) iter.Seq[*Item] {
	return func(yield func(*Item) bool) {
		t.Ascend(func(ti *Item) bool {
			return yield(ti)
		})
	}
}

type KType interface {
	~int | ~int8 | ~int64 | ~string
}

type SortedMap[K KType, V any] interface {
	Put(k K, v V)
}

func NewSortedMap[K KType]() {
	// TODO: ниже см. TypeFor
	tt := reflect.TypeOf((*K)(nil)).Elem().Kind()
	isInt := tt == reflect.Int
	isString := tt == reflect.String
	isInt8 := tt == reflect.Int8
	fmt.Printf("%+v %v %v %v\n", tt, isInt, isString, isInt8)

	// tt := reflect.TypeOf((*K)(nil)).Elem()
	// fmt.Printf("%+v\n", tt)

	tt2 := reflect.TypeFor[K]().Kind()
	fmt.Printf("*** %+v\n", tt2)
}
