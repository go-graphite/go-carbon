package v2core

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	partsformat "github.com/go-graphite/go-carbon/internal/v2part"
)

type singleArchiveImage map[string]struct {
	Meta    RollupMetadata
	Records []sealrollup.Record
}

// runMixedIdleStore seals single- and multi-archive names, reopens, then runs
// one idle closure seal. A non-nil expire forces the full-metric idle scan.
func runMixedIdleStore(t *testing.T, expire func(RollupPolicy, RollupMetadata, time.Time) bool) (singleArchiveImage, []int) {
	t.Helper()
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	root := t.TempDir()
	now := int64(1794700800)
	open := func() *Engine {
		e, err := Open(root, cfg, Hooks{Expire: expire, Now: func() time.Time { return time.Unix(now, 0) }})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	var batches []Batch
	for i, retention := range []string{"1s:1h", "10s:1d", "1s:1h,1m:1d,1h:1y", "1s:1h", "10s:1d,1m:7d"} {
		_, p := rollupTestPolicy(t, fmt.Sprintf("mixed.%d", i), retention, sealrollup.Average, 0)
		id := uint64(i + 1)
		batches = append(batches, Batch{ID: id, Policy: p, Order: id, Clock: now, Points: []Row{{ID: id, Time: now - int64(i), Bits: math.Float64bits(float64(i) + .5), Seq: 1}}})
	}
	if err := e.AppendBatches(0, batches, uint64(len(batches)), true); err != nil {
		t.Fatal(err)
	}
	closable := func() []int {
		var ids []int
		for id := range e.owners[0].rollups.closable {
			ids = append(ids, int(id))
		}
		if len(e.owners[0].rollups.metrics) != len(batches) {
			t.Fatalf("metrics %d", len(e.owners[0].rollups.metrics))
		}
		return ids
	}
	before := closable()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = open()
	defer func() { _ = e.Close() }()
	if got := closable(); len(got) != len(before) {
		t.Fatalf("closable after reopen %v, before %v", got, before)
	}
	now += 3600
	if err := e.AppendBatches(0, nil, uint64(len(batches)), true); err != nil {
		t.Fatal(err)
	}
	image := singleArchiveImage{}
	pin := e.DBs()[0].NewSnapshot()
	defer pin.Close()
	if err := scanRollups(pin, nil, func(p RollupPolicy, m RollupMetadata, rs []sealrollup.Record) error {
		image[string(p.Name)] = struct {
			Meta    RollupMetadata
			Records []sealrollup.Record
		}{m, append([]sealrollup.Record(nil), rs...)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return image, closable()
}

// The closure index changes only which metrics an idle pass visits: a mixed
// store yields the same records and metadata as the full-metric scan.
func TestSingleArchiveIdleIndexDifferential(t *testing.T) {
	indexed, closable := runMixedIdleStore(t, nil)
	full, _ := runMixedIdleStore(t, func(RollupPolicy, RollupMetadata, time.Time) bool { return false })
	if len(indexed) != 5 || !reflect.DeepEqual(indexed, full) {
		t.Fatalf("indexed %+v\nfull %+v", indexed, full)
	}
	if len(closable) != 2 {
		t.Fatalf("closable %v, want the two coarse-archive names", closable)
	}
	for name, m := range indexed {
		if m.Meta.ClosedThrough == 0 && len(m.Records) == 0 {
			t.Fatalf("%s lost data", name)
		}
	}
}

func TestSingleArchiveNeedsNoMetadataSeal(t *testing.T) {
	r := newCoreRollups()
	single, err := NewApplicationPolicy("s", mustRollupPolicy(t, "s", "1s:1h"))
	if err != nil {
		t.Fatal(err)
	}
	multi, err := NewApplicationPolicy("m", mustRollupPolicy(t, "m", "1s:1h,1m:1d"))
	if err != nil {
		t.Fatal(err)
	}
	if r.needsMetadataSeal(&coreBatch{Registered: single}) || !r.needsMetadataSeal(&coreBatch{Registered: multi}) || !r.needsMetadataSeal(&coreBatch{Policy: []byte("x")}) {
		t.Fatal("metadata seal rule")
	}
	r.expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return false }
	if !r.needsMetadataSeal(&coreBatch{Registered: single}) {
		t.Fatal("expiry needs every new metric anchored")
	}
}

func mustRollupPolicy(t *testing.T, name, retention string) []byte {
	t.Helper()
	_, p := rollupTestPolicy(t, name, retention, sealrollup.Average, 0)
	return p
}

// BenchmarkIdleMaintenanceScan is the once-per-second owner scan on a
// single-archive store: full metric map (before) against the closure index.
func BenchmarkIdleMaintenanceScan(b *testing.B) {
	const names = 200000
	r := newCoreRollups()
	for i := 0; i < names; i++ {
		// One identity per ID, as the catalog interns them: the slot hint
		// lives on the identity.
		p, err := EncodeRollupPolicy(RollupPolicy{Name: partsformat.ID(fmt.Sprintf("s.%d", i)), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}}}})
		if err != nil {
			b.Fatal(err)
		}
		m, err := r.metric(uint64(i+1), p)
		if err != nil {
			b.Fatal(err)
		}
		meta := freshMetadata
		meta.Version, meta.Frontier.Order, meta.Frontier.MaxTime = 3, uint64(i+1), 1794700800
		r.install(uint64(i+1), m, meta)
	}
	scan := func(metrics map[uint64]*rollupMetric) bool {
		for id, m := range metrics {
			if meta := r.metaOf(id); !meta.Expired && rollupClosureDue(m, meta, 1794700801) {
				return true
			}
		}
		return false
	}
	b.Run("full", func(b *testing.B) {
		for range b.N {
			if scan(r.metrics) {
				b.Fatal("due")
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/names, "ns/name")
	})
	b.Run("indexed", func(b *testing.B) {
		for range b.N {
			if scan(r.idleCandidates()) {
				b.Fatal("due")
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/names, "ns/name")
	})
}
