package v2backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/points"
)

func TestAppendDataRangesDisjointAcrossSeals(t *testing.T) {
	var prior []byte
	for seq := uint64(1); seq <= 128; seq++ {
		var keys [][]byte
		for _, name := range []string{"group.a", "group.z", "group.a;tag=x", "group.zero\x00"} {
			old, err := partsformat.Encode(partsformat.Key{Group: "group", VShard: partsformat.Route(partsformat.ID(name)), Partition: -1, Part: partsformat.PartID{255}, Metric: partsformat.ID(name), Archive: compactArchive})
			if err != nil {
				t.Fatal(err)
			}
			key, err := encodeDataKey(old, seq, [8]byte{99})
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeDataKey(key)
			if err != nil || got.Metric != partsformat.ID(name) || got.Partition != -1 || got.VShard != partsformat.Route(got.Metric) {
				t.Fatal(got, err)
			}
			if _, err = partsformat.Decode(key); err == nil {
				t.Fatal("old decoder accepted new format")
			}
			keys = append(keys, key)
		}
		slices.SortFunc(keys, bytes.Compare)
		if prior != nil && bytes.Compare(prior, keys[0]) >= 0 {
			t.Fatal("seal range overlaps predecessor", seq)
		}
		prior = keys[len(keys)-1]
	}
}
func TestAppendCheckpointBoundAndRestartExact(t *testing.T) {
	clock := int64(1794700800)
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.WriteEngine = "core"
	opts.Owners = 3
	open := func() *Store {
		t.Helper()
		s, e := openAt(opts, func() time.Time { return time.Unix(clock, 0) })
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	s := open()
	names := []string{"group.a", "other.z", "group.tag;tag=x"}
	for seal := 0; seal < 130; seal++ {
		clock++
		for i, name := range names {
			value := float64(seal + i)
			if seal%9 == 0 {
				value = math.Float64frombits(0x8000000000000000)
			}
			if e := s.Append(points.OnePoint(name, value, clock-1)); e != nil {
				t.Fatal(e)
			}
		}
		if e := s.Flush(); e != nil {
			t.Fatal(e)
		}
		it, e := s.dbs[0].NewIter(&pebble.IterOptions{LowerBound: appendLogPrefix, UpperBound: partsformat.PrefixEnd(appendLogPrefix)})
		if e != nil {
			t.Fatal(e)
		}
		count := 0
		firstFull := false
		for it.First(); it.Valid(); it.Next() {
			full, _, _, e := decodeAppendRecord(it.Value())
			if e != nil {
				t.Fatal(e)
			}
			if count == 0 {
				firstFull = full
			}
			count++
		}
		if e = it.Close(); e != nil {
			t.Fatal(e)
		}
		if count > appendCheckpointSeals {
			t.Fatal("unbounded seal history", count)
		}
		if s.owners[0].appendState.sequence >= 64 && !firstFull {
			t.Fatal("checkpoint missing before deltas")
		}
	}
	var before [][]uint64
	for _, name := range names {
		r, e := s.Fetch(context.Background(), name, int(clock-200), int(clock))
		if e != nil {
			t.Fatal(e)
		}
		v := make([]uint64, len(r.Values))
		for i, x := range r.Values {
			v[i] = math.Float64bits(x)
		}
		before = append(before, v)
		r.Release()
	}
	sequence := s.owners[0].appendState.sequence
	partCount := len(s.owners[0].appendState.parts)
	requireClose(t, s)
	s = open()
	defer requireClose(t, s)
	if s.owners[0].appendState.sequence != sequence || len(s.owners[0].appendState.parts) != partCount {
		t.Fatal("catalog rebuild differs")
	}
	for j, name := range names {
		r, e := s.Fetch(context.Background(), name, int(clock-200), int(clock))
		if e != nil {
			t.Fatal(e)
		}
		v := make([]uint64, len(r.Values))
		for i, x := range r.Values {
			v[i] = math.Float64bits(x)
		}
		r.Release()
		if !slices.Equal(before[j], v) {
			t.Fatal("restart changed render bits", name)
		}
	}
	clock++
	if e := s.Append(points.OnePoint(names[0], 9, clock-1)); e != nil {
		t.Fatal(e)
	}
	if e := s.Flush(); e != nil {
		t.Fatal(e)
	}
	if s.owners[0].appendState.sequence <= sequence {
		t.Fatal("seal sequence reused after restart")
	}
}
func TestAppendSealRecordRejectsTruncation(t *testing.T) {
	b := encodeAppendRecord(true, []v2core.KV{{Key: []byte("k"), Value: []byte("v")}}, nil)
	for i := range b {
		if _, _, _, e := decodeAppendRecord(b[:i]); e == nil {
			t.Fatal("accepted truncated seal", i)
		}
	}
	full, rows, _, e := decodeAppendRecord(b)
	if e != nil || !full || string(rows[0].Value) != "v" {
		t.Fatal(e)
	}
	if binary.BigEndian.Uint64(appendLogKey(2)[4:]) != 2 {
		t.Fatal("seal order")
	}
}

func TestAppendCheckpointIncludesSkippedSealSequences(t *testing.T) {
	opts := testOptions(t.TempDir(), simplePolicy())
	opts.WriteEngine = "core"
	s, e := openAt(opts, time.Now)
	if e != nil {
		t.Fatal(e)
	}
	defer requireClose(t, s)
	live := 0
	for seq := uint64(1); seq < 256; seq++ {
		// Empty frontier-only seals need no metadata record, including boundaries.
		if seq%64 == 0 {
			continue
		}
		plan := v2core.SealPlan{Files: [][]v2core.KV{{{Key: []byte("metadata"), Value: []byte("v")}}}, Commit: func() {}}
		if e = s.appendSeal(0, seq, &plan); e != nil {
			t.Fatal(e)
		}
		full, _, _, err := decodeAppendRecord(plan.Files[0][0].Value)
		if err != nil {
			t.Fatal(err)
		}
		if full {
			live = 1
		} else {
			live++
		}
		if live > appendCheckpointSeals {
			t.Fatal("skipped boundary left unbounded history", seq, live)
		}
		plan.Commit()
	}
}
