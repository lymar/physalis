package physalis

import (
	"bytes"
	"iter"
	"log/slog"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/go-softwarelab/common/pkg/seq2"
	"github.com/lymar/physalis/internal/log"
	bolt "go.etcd.io/bbolt"
)

// go test ./ -v

func TestSerDeInt32(t *testing.T) {
	r := rand.New(rand.NewSource(125))
	numCount := 10_000
	nums := make([]int32, numCount)
	bins := make([][]byte, numCount)
	ser, de := getKTypeSerDe[int32]()
	for i := range numCount {
		v := int32(r.Uint32())
		nums[i] = v
		bins[i] = ser(v)
	}

	slices.Sort(nums)
	sort.Slice(bins, func(i int, j int) bool {
		return bytes.Compare(bins[i], bins[j]) < 0
	})

	for i := range numCount {
		v := de(bins[i])
		if v != nums[i] {
			t.Fatalf("mismatch at %d: %d != %d", i, v, nums[i])
		}
	}
}

func TestSerDeInt64(t *testing.T) {
	r := rand.New(rand.NewSource(135))
	numCount := 10_000
	nums := make([]int64, numCount)
	bins := make([][]byte, numCount)
	ser, de := getKTypeSerDe[int64]()
	for i := range numCount {
		v := int64(r.Uint64())
		nums[i] = v
		bins[i] = ser(v)
	}

	slices.Sort(nums)
	sort.Slice(bins, func(i int, j int) bool {
		return bytes.Compare(bins[i], bins[j]) < 0
	})

	for i := range numCount {
		v := de(bins[i])
		// t.Logf("i=%d: v=%d, nums[i]=%d", i, v, nums[i])
		if v != nums[i] {
			t.Fatalf("mismatch at %d: %d != %d", i, v, nums[i])
		}
	}
}

func TestSerDeUInt32(t *testing.T) {
	r := rand.New(rand.NewSource(150))
	numCount := 10_000
	nums := make([]uint32, numCount)
	bins := make([][]byte, numCount)
	ser, de := getKTypeSerDe[uint32]()
	for i := range numCount {
		v := r.Uint32()
		nums[i] = v
		bins[i] = ser(v)
	}

	slices.Sort(nums)
	sort.Slice(bins, func(i int, j int) bool {
		return bytes.Compare(bins[i], bins[j]) < 0
	})

	for i := range numCount {
		v := de(bins[i])
		// t.Logf("i=%d: v=%d, nums[i]=%d", i, v, nums[i])
		if v != nums[i] {
			t.Fatalf("mismatch at %d: %d != %d", i, v, nums[i])
		}
	}
}

func TestSerDeUInt64(t *testing.T) {
	r := rand.New(rand.NewSource(155))
	numCount := 10_000
	nums := make([]uint64, numCount)
	bins := make([][]byte, numCount)
	ser, de := getKTypeSerDe[uint64]()
	for i := range numCount {
		v := r.Uint64()
		nums[i] = v
		bins[i] = ser(v)
	}

	slices.Sort(nums)
	sort.Slice(bins, func(i int, j int) bool {
		return bytes.Compare(bins[i], bins[j]) < 0
	})

	for i := range numCount {
		v := de(bins[i])
		// t.Logf("i=%d: v=%d, nums[i]=%d", i, v, nums[i])
		if v != nums[i] {
			t.Fatalf("mismatch at %d: %d != %d", i, v, nums[i])
		}
	}
}

// pick a random rune from available ranges
func randomRune(r *rand.Rand) rune {
	// rune ranges (letters, digits, Greek, Cyrillic, emoji)
	var ranges = []struct {
		start, end rune
	}{
		{'0', '9'},         // digits
		{'A', 'Z'},         // Latin uppercase
		{'a', 'z'},         // Latin lowercase
		{0x0410, 0x044F},   // Cyrillic (А–я)
		{0x0391, 0x03C9},   // Greek (Α–ω)
		{0x1F600, 0x1F64F}, // emoji (smileys)
	}
	rr := ranges[r.Intn(len(ranges))]
	return rr.start + rune(r.Intn(int(rr.end-rr.start+1)))
}

// generate a random string with length between 1 and 10
func randomUnicodeString(r *rand.Rand) string {
	length := 1 + r.Intn(10) // random length in [1..10]
	result := make([]rune, length)
	for i := range result {
		result[i] = randomRune(r)
	}
	return string(result)
}

func TestSerDeString(t *testing.T) {
	r := rand.New(rand.NewSource(155))
	numCount := 10_000
	strs := make([]string, numCount)
	bins := make([][]byte, numCount)
	ser, de := getKTypeSerDe[string]()
	for i := range numCount {
		v := randomUnicodeString(r)
		strs[i] = v
		bins[i] = ser(v)
	}

	slices.Sort(strs)
	sort.Slice(bins, func(i int, j int) bool {
		return bytes.Compare(bins[i], bins[j]) < 0
	})

	for i := range numCount {
		v := de(bins[i])
		// t.Logf("i=%d: v=%s, strs[i]=%s", i, v, strs[i])
		if v != strs[i] {
			t.Fatalf("mismatch at %d: %s != %s", i, v, strs[i])
		}
	}
}

type TRuntimeEvent struct {
	Name   string
	Points int64
}

type TRuntimeReducerState struct {
	TotalCounter int64
}

type TRuntimeReducer struct {
	version string
}

func (trr *TRuntimeReducer) Version() string {
	return trr.version
}

var tRuntimeReducerName = "runtime_red_global"

func (gr *TRuntimeReducer) Apply(
	runtime *ReducerRuntime,
	state *TRuntimeReducerState,
	evs iter.Seq2[uint64, *Event[TRuntimeEvent]]) *TRuntimeReducerState {

	byNameCounter := OpenKV[string, int64](runtime, "by_name_counter")

	if state == nil {
		state = &TRuntimeReducerState{}
		slog.Debug("TRuntimeReducer: init state")
	}
	for _, ev := range evs {
		state.TotalCounter += ev.Payload.Points

		v := byNameCounter.Get(ev.Payload.Name)
		if v == nil {
			slog.Debug("TRuntimeReducer: new name", "name", ev.Payload.Name, "points", ev.Payload.Points)
			byNameCounter.Put(ev.Payload.Name, &ev.Payload.Points)
		} else {
			slog.Debug("TRuntimeReducer: existing name", "name", ev.Payload.Name, "old", *v, "add", ev.Payload.Points)
			res := *v + ev.Payload.Points
			byNameCounter.Put(ev.Payload.Name, &res)
		}
	}

	return state
}

func TestReducerRuntime(t *testing.T) {
	log.InitDevLog()

	reg := NewReducerRegistry[TRuntimeEvent]()
	reducerReader, err := AddGlobalReducer(reg, tRuntimeReducerName, &TRuntimeReducer{version: "1"})
	if err != nil {
		t.Fatal(err)
	}

	err = withTempDb(func(db *bolt.DB) {
		if err := db.Update(func(tx *bolt.Tx) error {
			err := reg.init(tx)
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := db.Update(func(tx *bolt.Tx) error {
			writeFn := reg.apply(db, mkRuntimeEventSeq(1,
				&Event[TRuntimeEvent]{
					Payload: TRuntimeEvent{Name: "alice", Points: 10}},
				&Event[TRuntimeEvent]{
					Payload: TRuntimeEvent{Name: "bob", Points: 5}},
				&Event[TRuntimeEvent]{
					Payload: TRuntimeEvent{Name: "alice", Points: 20}},
			))

			err = writeFn(tx)
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := db.View(func(tx *bolt.Tx) error {
			loadedState, err := reducerReader.ReadState(tx)
			if err != nil {
				return err
			}
			if loadedState == nil {
				t.Fatalf("no state after first write")
			}
			if loadedState.TotalCounter != 35 {
				t.Fatalf("invalid state after first write, expected 35, got %v", loadedState.TotalCounter)
			}

			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	if err != nil {
		t.Fatal(err)
	}
}

func mkRuntimeEventSeq(startUid uint64, events ...*Event[TRuntimeEvent]) func() iter.Seq2[uint64, *Event[TRuntimeEvent]] {
	return func() iter.Seq2[uint64, *Event[TRuntimeEvent]] {
		return seq2.Map(
			slices.All(events),
			func(i int, ev *Event[TRuntimeEvent]) (uint64, *Event[TRuntimeEvent]) {
				return uint64(i) + startUid, ev
			})
	}
}
