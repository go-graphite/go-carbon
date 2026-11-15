package whisperio

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type classicRangeIO struct {
	store.ContinuationIO
	scans int
	fail  error
}

func (kv *classicRangeIO) VisitRange(start, end []byte, visit func([]byte, []byte) error) error {
	kv.scans++
	if kv.fail != nil {
		return kv.fail
	}
	var keys, values [][]byte
	if err := kv.ContinuationIO.Visit(start[:16], func(k, v []byte) error {
		if bytes.Compare(k, start) >= 0 && bytes.Compare(k, end) < 0 {
			keys = append(keys, bytes.Clone(k))
			values = append(values, bytes.Clone(v))
		}
		return nil
	}); err != nil {
		return err
	}
	order := make([]int, len(keys))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return bytes.Compare(keys[order[i]], keys[order[j]]) < 0 })
	for _, i := range order {
		if err := visit(keys[i], values[i]); err != nil {
			return err
		}
	}
	return nil
}

func TestClassicRangeMatchesPointReader(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1700000000, 0) }
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "range.classic", Retentions: []store.Retention{{Step: 60, Count: 1027}}, AggregationMethod: store.Last}}
	kv := mapContinuationIO{}
	snap := store.Snapshot{Metadata: m, Archives: []store.Archive{{Retention: m.Retentions[0]}}}
	bits := []uint64{0, 1, 1 << 63, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff123456789abcd}
	for i := 0; i < 1027; i++ {
		if i%7 != 0 {
			snap.Archives[0].Points = append(snap.Archives[0].Points, store.Point{Timestamp: 1699938360 + int64(i*60), Value: math.Float64frombits(bits[i%len(bits)])})
		}
	}
	codec := &ContinuationCodec{}
	if err := codec.RestoreArchives(ctx, kv, snap, clock); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(732))
	rangeIO := &classicRangeIO{ContinuationIO: kv}
	for i := 0; i < 200; i++ {
		from := 1699938360 + rng.Intn(1000)*60
		until := min(1700000000, from+(257+rng.Intn(900))*60)
		want, e1 := codec.Fetch(ctx, kv, m, from, until, clock)
		got, e2 := codec.Fetch(ctx, rangeIO, m, from, until, clock)
		if (e1 == nil) != (e2 == nil) || (want == nil) != (got == nil) {
			t.Fatalf("errors %v/%v", e1, e2)
		}
		if want != nil {
			if got.FromTime != want.FromTime || got.UntilTime != want.UntilTime || got.Step != want.Step || len(got.Values) != len(want.Values) {
				t.Fatal("shape differs")
			}
			for j, v := range want.Values {
				if math.Float64bits(v) != math.Float64bits(got.Values[j]) {
					t.Fatalf("query %d point %d differs", i, j)
				}
			}
		}
		d1, e1 := codec.SumDigest(ctx, kv, m, from, until, clock)
		d2, e2 := codec.SumDigest(ctx, rangeIO, m, from, until, clock)
		if e1 != nil || e2 != nil || !reflect.DeepEqual(d1, d2) {
			t.Fatalf("digest differs %v/%v", e1, e2)
		}
	}
	if rangeIO.scans == 0 {
		t.Fatal("scan was not exercised")
	}
	rangeIO.fail = errors.New("scan failure")
	if _, err := codec.Fetch(ctx, rangeIO, m, 1699939000, 1700000000, clock); !errors.Is(err, rangeIO.fail) {
		t.Fatal("scan error lost", err)
	}
}
