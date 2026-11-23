package whisperio

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSparseRepairUnsupported(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(100000, 0) }
	codec := &ContinuationCodec{}
	r := store.Retention{Step: 60, Count: 1024}
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "repair.fallback", Retentions: []store.Retention{r}, AggregationMethod: store.Sum}, Revision: 7}
	snap := store.Snapshot{Metadata: m, Archives: []store.Archive{{Retention: r, Points: []store.Point{{Timestamp: 60000, Value: 1}, {Timestamp: 90000, Value: 2}}}}}
	p := store.SparseRepair{Expected: store.TransferVersion{Name: m.Name}, Archives: []store.RepairArchive{{Archive: 0, Retention: r, Points: []store.RepairPoint{{Timestamp: 60060, Bits: math.Float64bits(4)}}}}}
	cases := []string{"geometry", "archive", "legacy-index", "stale-index", "legacy-certificate", "stale-certificate", "nan-outside", "sidecar", "references", "compressed", "off-grid-alias"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			kv := &windowCountingIO{mapContinuationIO: mapContinuationIO{}, revision: 7}
			fixture := snap
			if name == "nan-outside" {
				fixture.Archives = []store.Archive{{Retention: r, Points: []store.Point{{Timestamp: 60000, Value: 1}, {Timestamp: 90000, Value: math.Float64frombits(0x7ff8000000000042)}}}}
			}
			if err := codec.RestoreArchives(ctx, kv, fixture, clock); err != nil {
				t.Fatal(err)
			}
			work := p
			work.Archives = append([]store.RepairArchive(nil), p.Archives...)
			fs := newContinuationFiles(kv, false)
			file, err := fs.OpenFile(fs.main(), 2, 0600)
			if err != nil {
				t.Fatal(err)
			}
			f := file.(*continuationFile)
			id := f.data.id
			switch name {
			case "geometry":
				work.Archives[0].Retention.Count++
			case "archive":
				work.Archives[0].Archive = 1
			case "legacy-index":
				delete(kv.mapContinuationIO, string(classicWindowKey(id)))
			case "stale-index":
				v := kv.mapContinuationIO[string(classicWindowKey(id))]
				binary.BigEndian.PutUint64(v[1:], 6)
			case "legacy-certificate":
				delete(kv.mapContinuationIO, string(classicRepairKey(id)))
			case "stale-certificate":
				v := kv.mapContinuationIO[string(classicRepairKey(id))]
				binary.BigEndian.PutUint64(v[1:], 6)
			case "sidecar":
				kv.mapContinuationIO["n/extra.wsp"] = append([]byte(nil), kv.mapContinuationIO["n/main.wsp"]...)
			case "references":
				f.data.references = true
				f.data.dirty = true
				if err := fs.flush(); err != nil {
					t.Fatal(err)
				}
			case "off-grid-alias":
				key := continuationBodyKey(id, 'p', 0, 0, 0)
				var block store.PointBlock
				if err := store.DecodePointBlock(kv.mapContinuationIO[string(key)], &block); err != nil {
					t.Fatal(err)
				}
				block.Points[0].Timestamp++
				if err := f.setPointBlock(key, &block); err != nil {
					t.Fatal(err)
				}
				if err := fs.flush(); err != nil {
					t.Fatal(err)
				}
			case "compressed":
				kv.mapContinuationIO = mapContinuationIO{}
				if err := codec.Create(ctx, kv, m, clock); err != nil {
					t.Fatal(err)
				}

			}
			before := len(kv.mapContinuationIO)
			_, err = codec.PlanSparseRepair(ctx, kv, m, work, clock)
			var unsupported *store.SparseRepairUnsupported
			if !errors.As(err, &unsupported) {
				t.Fatalf("want typed unsupported, got %v", err)
			}
			if len(kv.mapContinuationIO) != before {
				t.Fatal("planner mutated input")
			}
		})
	}
}

func TestSparseRepairReadsOnlyAffectedGroups(t *testing.T) {
	ctx := context.Background()
	clock := func() time.Time { return time.Unix(1000000, 0) }
	codec := &ContinuationCodec{}
	r := store.Retention{Step: 60, Count: 8192}
	m := store.Metadata{MetricConfig: store.MetricConfig{Name: "repair.seek", Retentions: []store.Retention{r}, AggregationMethod: store.Sum}, Revision: 7}
	a := store.Archive{Retention: r}
	for i := range r.Count {
		if i != 4001 && i != 4002 {
			a.Points = append(a.Points, store.Point{Timestamp: 480000 + int64(i*60), Value: float64(i)})
		}
	}
	kv := &windowCountingIO{mapContinuationIO: mapContinuationIO{}, revision: 7}
	if err := codec.RestoreArchives(ctx, kv, store.Snapshot{Metadata: m, Archives: []store.Archive{a}}, clock); err != nil {
		t.Fatal(err)
	}
	kv.groups = 0
	p := store.SparseRepair{Expected: store.TransferVersion{Name: m.Name}, Archives: []store.RepairArchive{{Archive: 0, Retention: r, Points: []store.RepairPoint{{Timestamp: 480000 + 4001*60, Bits: 0x8000000000000000}, {Timestamp: 480000 + 4002*60, Bits: math.Float64bits(5)}}}}}
	plan, err := codec.PlanSparseRepair(ctx, kv, m, p, clock)
	if err != nil {
		t.Fatal(err)
	}
	if kv.groups != 1 {
		t.Fatalf("two missing adjacent points read %d groups, want 1", kv.groups)
	}
	written := 0
	for _, entry := range plan.Writes {
		if len(entry.Key) == 20 && entry.Key[9] == 'p' {
			written++
		}
	}
	if written != 1 {
		t.Fatalf("wrote %d groups", written)
	}
}

func TestSparseRepairCertificateRevisionBinding(t *testing.T) {
	codec := &ContinuationCodec{}
	index := &classicWindowIndex{revision: 7, repairSafe: true}
	key := classicRepairKey(1)
	encoded := encodeClassicRepairIndex(index)
	// A legacy writer can preserve unknown metadata but cannot silently make
	// its old certificate valid for a changed revision or a transferred image.
	checked := codec.CheckIndex(key, encoded, 8)
	if len(checked) != 9 {
		t.Fatal("stale certificate survived export")
	}
	rebound := codec.BindIndex(key, checked, 9)
	if len(rebound) != 9 {
		t.Fatal("transfer revived stale certificate")
	}
	valid := codec.BindIndex(key, encoded, 9)
	if len(valid) != 10 || valid[9] != 1 || binary.BigEndian.Uint64(valid[1:]) != 9 {
		t.Fatal("valid certificate not rebound")
	}
	if binary.BigEndian.Uint64(encoded[1:]) != 7 {
		t.Fatal("binding changed immutable preimage")
	}
}
