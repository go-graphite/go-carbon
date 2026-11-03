package inputlog

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

// The Entry encoder is unchanged. Cold, warm and stale atomic hints from the
// two-column path must produce exactly its bytes, including after segment
// changes and replacement references for the same name.
func TestBatchMatchesEntryEncoding(t *testing.T) {
	var legacy, columnar streamEncoder
	var a, b streamScratch
	refs := make([]*StreamRef, 128)
	for i := range refs {
		refs[i] = NewStreamRef(fmt.Sprintf("batch.metric.%d", i))
	}
	for cycle := range 8 {
		seq := uint64(cycle/3 + 1)
		legacy.begin(seq)
		columnar.begin(seq)
		var entries []Entry
		var batch Batch
		for i := range 4096 {
			ref := refs[(i*17+cycle)%len(refs)]
			if i%31 == 0 {
				ref = NewStreamRef(ref.Metric)
			}
			value := math.Float64frombits(uint64(i*cycle)*0x9e3779b97f4a7c15 + uint64(i))
			ts := int64(i)*math.MaxInt32 - int64(cycle)*math.MaxInt64
			entries = append(entries, ref.Entry(value, ts))
			batch.Refs = append(batch.Refs, ref)
			batch.Points = append(batch.Points, points.Point{Value: value, Timestamp: ts})
		}
		for _, e := range entries {
			legacy.add(&a, e)
		}
		// References now hold another encoder's hints, possibly for another ID.
		for i, p := range batch.Points {
			columnar.add(&b, batch.Refs[i].Entry(p.Value, p.Timestamp))
		}
		var oldFrames, newFrames [][]byte
		legacy.flush(&a, nil, func(raw []byte, _ int) []byte {
			oldFrames = append(oldFrames, bytes.Clone(raw))
			return raw
		})
		columnar.flush(&b, nil, func(raw []byte, _ int) []byte {
			newFrames = append(newFrames, bytes.Clone(raw))
			return raw
		})
		if !reflect.DeepEqual(oldFrames, newFrames) {
			t.Fatalf("cycle %d changed durable bytes", cycle)
		}
	}
}

type batchTestSource struct {
	*fakeSource
	spares []Batch
}

func (s *batchTestSource) TakeLogEntries(int, uint64, []Entry) []Entry {
	panic("BatchSource fell back to row expansion")
}

func (s *batchTestSource) TakeLogBatch(shard int, gen uint64, spare Batch) Batch {
	entries := s.fakeSource.TakeLogEntries(shard, gen, nil)
	batch := s.spares[shard]
	s.spares[shard] = spare
	batch.Refs, batch.Points = batch.Refs[:0], batch.Points[:0]
	for _, e := range entries {
		batch.Refs = append(batch.Refs, e.Ref)
		batch.Points = append(batch.Points, points.Point{Value: e.Value, Timestamp: e.Timestamp})
	}
	return batch
}

// Use the real group-commit driver, then reopen its segments through the
// ordinary reader. Every consumed spare must have released its references.
func TestBatchSourceReplayAndSpareOwnership(t *testing.T) {
	src := &batchTestSource{fakeSource: newFakeSource(), spares: make([]Batch, testShards)}
	src.refs = map[string]*StreamRef{}
	dir := t.TempDir()
	l, _, _ := reopen(t, dir, Options{}, src)
	defer l.Kill()
	want := recorded{}
	for cycle := range 4 {
		for i := range 1000 {
			name := fmt.Sprintf("batch.metric.%d", i%64)
			v, ts := float64(cycle*1000+i), int64(cycle*1000-i)
			src.add(name, v, ts)
			want.add(name, v, ts)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	for _, spare := range l.spareBatches {
		if len(spare.Points) != 0 || len(spare.Refs) != 0 || slices.ContainsFunc(spare.Refs[:cap(spare.Refs)], func(r *StreamRef) bool { return r != nil }) {
			t.Fatal("consumed batch retained a popped item")
		}
	}
	l.Kill()
	reader, err := Open(dir, withTestDefaults(Options{}))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, stats := replayAll(t, reader)
	equalRecorded(t, got, want)
	if stats.Corrupt != 0 || stats.Points != 4000 {
		t.Fatalf("replay stats %+v", stats)
	}
}
