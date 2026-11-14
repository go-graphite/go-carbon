package chunkstore

import (
	"cmp"
	"context"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"
	"unsafe"
)

// Frozen decoder before the allocation fix, including its temporary Store.
func legacyTransferSnapshot(ctx context.Context, record TransferMetric, codec ContinuationCodec, now func() time.Time) (Snapshot, error) {
	if now == nil {
		now = time.Now
	}
	if record.Continuation && codec == nil {
		return Snapshot{}, ErrFormat
	}
	return (&Store{continuation: codec, now: now}).legacyTransferRecordSnapshot(ctx, record)
}

func (s *Store) legacyTransferRecordSnapshot(ctx context.Context, record TransferMetric) (Snapshot, error) {
	m := Metadata{MetricConfig: record.Config, ID: record.Version.ID, Generation: record.Version.Generation, Continuation: record.Continuation}
	if record.Continuation {
		return s.continuation.Snapshot(ctx, transferRecordIO(record), m, s.now)
	}
	result := Snapshot{Metadata: m, Archives: make([]Archive, len(m.Retentions))}
	for i, r := range m.Retentions {
		result.Archives[i].Retention = r
	}
	var c chunk
	for _, kv := range record.Keys {
		if len(kv.Key) != 6 {
			return result, ErrFormat
		}
		archive := int(binary.BigEndian.Uint16(kv.Key))
		if archive >= len(result.Archives) {
			return result, ErrFormat
		}
		if err := decodeChunk(kv.Value, &c); err != nil {
			return result, err
		}
		for slot, point := range c.Points {
			if c.has(slot) {
				result.Archives[archive].Points = append(result.Archives[archive].Points, point)
			}
		}
	}
	for i := range result.Archives {
		slices.SortFunc(result.Archives[i].Points, func(a, b Point) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
	}
	return result, ctx.Err()
}

type transferSnapshotCodec struct{ ContinuationCodec }

func (transferSnapshotCodec) Snapshot(ctx context.Context, io ContinuationIO, m Metadata, now func() time.Time) (Snapshot, error) {
	value, err := io.Get([]byte("payload"))
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Metadata: m, Archives: []Archive{{Retention: m.Retentions[0], Points: []Point{{Timestamp: now().Unix(), Value: math.Float64frombits(binary.BigEndian.Uint64(value))}}}}}, ctx.Err()
}

func snapshotBits(s Snapshot) any {
	type point struct {
		Timestamp int64
		Bits      uint64
	}
	out := make([][]point, len(s.Archives))
	for i, a := range s.Archives {
		for _, p := range a.Points {
			out[i] = append(out[i], point{p.Timestamp, math.Float64bits(p.Value)})
		}
	}
	return struct {
		Metadata Metadata
		Points   [][]point
	}{s.Metadata, out}
}

func TestTransferSnapshotDifferential(t *testing.T) {
	ctx := context.Background()
	now := func() time.Time { return time.Unix(1800000000, 0) }
	rng := rand.New(rand.NewPCG(3, 7))
	store := &Store{exactBlocks: true}
	for i := 0; i < 100; i++ {
		r := Retention{Step: 60, Count: 128}
		snap := Snapshot{Metadata: Metadata{MetricConfig: MetricConfig{Name: "snapshot", Retentions: []Retention{r}, AggregationMethod: Sum}}, Archives: []Archive{{Retention: r}}}
		for j := 0; j < i; j++ {
			snap.Archives[0].Points = append(snap.Archives[0].Points, Point{Timestamp: int64(60 * (j + 1)), Value: math.Float64frombits(rng.Uint64())})
		}
		record, err := store.nativeSnapshotRecord(ctx, snap, TransferVersion{}, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, continuation := range []bool{false, true} {
			if continuation {
				record.Continuation = true
				value := make([]byte, 8)
				binary.BigEndian.PutUint64(value, rng.Uint64())
				record.Keys = []RelayoutKV{{Key: []byte("payload"), Value: value}}
			}
			got, ge := TransferSnapshot(ctx, record, transferSnapshotCodec{}, now)
			want, we := legacyTransferSnapshot(ctx, record, transferSnapshotCodec{}, now)
			if !reflect.DeepEqual(ge, we) || !reflect.DeepEqual(snapshotBits(got), snapshotBits(want)) {
				t.Fatalf("case %d continuation %v: errors %v/%v snapshots differ", i, continuation, ge, we)
			}
		}
	}
	for _, record := range []TransferMetric{{Continuation: true}, {Keys: []RelayoutKV{{Key: []byte("bad")}}}} {
		_, ge := TransferSnapshot(ctx, record, nil, nil)
		_, we := legacyTransferSnapshot(ctx, record, nil, nil)
		if !reflect.DeepEqual(ge, we) {
			t.Fatalf("errors %v/%v", ge, we)
		}
	}
	t.Logf("avoided temporary Store size: %d bytes", unsafe.Sizeof(Store{}))
}

func BenchmarkTransferSnapshotAllocation(b *testing.B) {
	record := TransferMetric{Continuation: true, Config: MetricConfig{Retentions: []Retention{{Step: 60, Count: 128}}}, Keys: []RelayoutKV{{Key: []byte("payload"), Value: make([]byte, 8)}}}
	for _, legacy := range []bool{true, false} {
		name := "current"
		if legacy {
			name = "legacy"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var err error
				if legacy {
					_, err = legacyTransferSnapshot(context.Background(), record, transferSnapshotCodec{}, time.Now)
				} else {
					_, err = TransferSnapshot(context.Background(), record, transferSnapshotCodec{}, time.Now)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
