package inputlog

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestAppendBeforeCacheKeepsHistoryAndPointBits(t *testing.T) {
	dir := t.TempDir()
	opts := withTestDefaults(Options{SegmentSize: 4096})
	src := newFakeSource()
	l := openStarted(t, dir, opts, src)
	src.add("historical", 42, 7)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	l.StartAppend()
	bits := []uint64{0, 1 << 63, 1, 0x7ff8000000000042, 0x7ff0000000000001, 0x7ff0000000000000, 0xfff0000000000000, 0x3ff0000000000001}
	const batches = 8
	var wg sync.WaitGroup
	for part := range partitions {
		wg.Go(func() {
			ref := NewStreamRef(fmt.Sprintf("partition.%d", part))
			var previous AppendReceipt
			for batch := range batches {
				var input Batch
				for i, bit := range bits {
					input.Append(ref, math.Float64frombits(bit), int64(batch*4+i/2))
				}
				receipt, err := l.Append(uint16(part), &input)
				if err != nil {
					t.Error(err)
					return
				}
				if receipt.Salt == 0 || receipt.Tail.Offset <= segmentHeaderSize {
					t.Error("invalid durable append receipt", receipt)
				}
				if batch > 0 && (receipt.Generation <= previous.Generation || (position{receipt.Tail.Segment, receipt.Tail.Offset}).less(position{previous.Tail.Segment, previous.Tail.Offset})) {
					t.Error("partition append receipt moved backwards", previous, receipt)
				}
				previous = receipt
			}
		})
	}
	wg.Wait()
	// The live suffix must never mutate the inventory used by old replay.
	history, _ := replayAll(t, l)
	if len(history) != 1 || len(history["historical"]) != 1 || history["historical"][0].Value != 42 {
		t.Fatal("live appends changed immutable historical inventory", history)
	}
	// A newly constructed source may immediately report its empty generations
	// persisted. It cannot retire original append authority before application.
	src = newFakeSource()
	src.setLow(0, true)
	if err := l.AttachSource(src, testShards); err != nil {
		t.Fatal(err)
	}
	if err := l.AttachSource(newFakeSource(), testShards); err == nil {
		t.Fatal("replaced live source")
	}
	for range 4 {
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got, _ := replayAll(t, l)
	if len(got) != partitions+1 || len(got["historical"]) != 1 {
		t.Fatal("lost append generation or historical prefix", len(got))
	}
	for part := range partitions {
		name := fmt.Sprintf("partition.%d", part)
		if len(got[name]) != batches*len(bits) {
			t.Fatal("missing/duplicate append records", name, len(got[name]))
		}
		for i, point := range got[name] {
			if math.Float64bits(point.Value) != bits[i%len(bits)] || point.Timestamp != int64(i/len(bits)*4+i%len(bits)/2) {
				t.Fatal("append correction bits/order changed", name, i)
			}
		}
	}
}

func TestAppendRefusesInvalidBatchWithoutPoisoningStream(t *testing.T) {
	l, err := Open(t.TempDir(), withTestDefaults(Options{}))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var valid Batch
	valid.Append(NewStreamRef("valid"), 1, 2)
	if _, err := l.Append(0, &valid); !errors.Is(err, ErrClosed) {
		t.Fatal("append before ownership", err)
	}
	l.StartAppend()
	for _, input := range []*Batch{nil, {}, {Refs: []*StreamRef{nil}, Points: []points.Point{{}}}, {Points: []points.Point{{}}}} {
		if _, err := l.Append(0, input); err == nil {
			t.Fatal("accepted invalid append", input)
		}
	}
	if _, err := l.Append(partitions, &valid); err == nil {
		t.Fatal("accepted invalid partition")
	}
	if _, err := l.Append(0, &valid); err != nil {
		t.Fatal("invalid request poisoned stream", err)
	}
}
