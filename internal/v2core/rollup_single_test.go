package v2core

import (
	"fmt"
	"math"
	"reflect"
	"sync/atomic"
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
// one idle closure seal. A non-nil expire is asked about every name there.
func runMixedIdleStore(t *testing.T, expire func(RollupPolicy, RollupMetadata, time.Time) bool) (singleArchiveImage, []int) {
	h := Hooks{}
	if expire != nil {
		h.Expire, h.ExpiryTTL = expire, expiryWindow(time.Nanosecond)
	}
	return runMixedIdleStoreHooks(t, h, nil)
}

// runMixedIdleStoreHooks is runMixedIdleStore with the expiry hooks given;
// arm runs just before the idle seal.
func runMixedIdleStoreHooks(t *testing.T, hooks Hooks, arm func()) (singleArchiveImage, []int) {
	t.Helper()
	t.Setenv("CORE_MERGE_WORKERS", "0")
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	root := t.TempDir()
	now := int64(1794700800)
	open := func() *Engine {
		h := hooks
		h.Now = func() time.Time { return time.Unix(now, 0) }
		e, err := Open(root, cfg, h)
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
		if err := e.owners[0].rollups.loadAllCheckpoint(); err != nil {
			t.Fatal(err)
		}
		return nil // seals have no closure work, regardless of archive count
	}
	before := closable()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = open()
	defer func() { _ = e.Close() }()
	closable() // recovery files every open coarse window
	if arm != nil {
		arm()
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
	return image, before
}

// The closure calendar changes only which metrics an idle pass visits: a
// mixed store yields the same records and metadata as one where expiry asks
// about every name and refuses.
func TestSingleArchiveIdleIndexDifferential(t *testing.T) {
	indexed, closable := runMixedIdleStore(t, nil)
	full, _ := runMixedIdleStore(t, func(RollupPolicy, RollupMetadata, time.Time) bool { return false })
	if len(indexed) != 5 || !reflect.DeepEqual(indexed, full) {
		t.Fatalf("indexed %+v\nfull %+v", indexed, full)
	}
	if len(closable) != 0 {
		t.Fatalf("filed %v, want no closure work", closable)
	}
	for name, m := range indexed {
		if len(m.Records) == 0 {
			t.Fatalf("%s lost data", name)
		}
	}
}

// Through real seals, a reopen and a refused (fenced) expiry, per-name
// windows yield the same records and metadata as a 1 ns window for every
// name, which asks Expire about every name at every pass.
func TestExpiryCalendarEngineDifferential(t *testing.T) {
	ttl := func(p RollupPolicy, _ time.Time) (time.Duration, time.Time) {
		if n := string(p.Name); n == "mixed.1" || n == "mixed.3" {
			return time.Hour, time.Time{}
		}
		return 0, time.Time{}
	}
	run := func(withTTL bool) singleArchiveImage {
		var armed atomic.Bool
		// Every name: Expire applies the window itself, as the walk did.
		h := Hooks{Expire: func(p RollupPolicy, m RollupMetadata, now time.Time) bool {
			d, _ := ttl(p, now)
			return armed.Load() && m.LastUpdate != 0 && d > 0 && now.Sub(time.Unix(0, m.LastUpdate)) >= d
		}, ExpiryTTL: expiryWindow(time.Nanosecond)}
		if withTTL {
			// Per-name windows: Expire is a gate only, as carbon's.
			h.Expire = func(RollupPolicy, RollupMetadata, time.Time) bool { return armed.Load() }
			h.ExpiryTTL = ttl
		}
		image, _ := runMixedIdleStoreHooks(t, h, func() { armed.Store(true) })
		return image
	}
	every, calendar := run(false), run(true)
	if !reflect.DeepEqual(every, calendar) {
		t.Fatalf("every name %+v\ncalendar %+v", every, calendar)
	}
	for name, m := range calendar {
		if want := name == "mixed.1" || name == "mixed.3"; m.Meta.Expired != want {
			t.Fatalf("%s expired %v, want %v", name, m.Meta.Expired, want)
		}
	}
}

// Expire is asked only about names idle for their window: with no window,
// an Expire that always agrees expires nothing, though idle closure stages
// the coarse-archive names.
func TestExpireAskedOnlyWhenOverdue(t *testing.T) {
	h := Hooks{Expire: func(p RollupPolicy, _ RollupMetadata, _ time.Time) bool {
		t.Errorf("Expire asked about %s", p.Name)
		return true
	}, ExpiryTTL: expiryWindow(0)}
	image, closable := runMixedIdleStoreHooks(t, h, nil)
	if len(closable) != 0 {
		t.Fatalf("filed %v, want no closure work", closable)
	}
	for name, m := range image {
		if m.Meta.Expired {
			t.Fatalf("%s expired", name)
		}
	}
}

func TestExpireNeedsExpiryTTL(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Rollups = true
	if _, err := Open(t.TempDir(), cfg, Hooks{Expire: func(RollupPolicy, RollupMetadata, time.Time) bool { return true }}); err == nil {
		t.Fatal("Open accepted Expire without ExpiryTTL")
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

// closureTestStore installs n names, every fourth single-archive, the rest
// on the bench catch-all ladder (1m fine, 30m coarse) or a three-coarse one.
func closureTestStore(t testing.TB, n int, t0 int64) *coreRollups {
	r := newCoreRollups()
	for i := 0; i < n; i++ {
		archives := []sealrollup.Archive{{Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}
		switch i % 4 {
		case 0:
			archives = archives[:1]
		case 1:
			archives = []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}, {Step: 86400, Count: 3650}}
		}
		p, err := EncodeRollupPolicy(RollupPolicy{Name: partsformat.ID(fmt.Sprintf("c.%d", i)), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5, Archives: archives}})
		if err != nil {
			t.Fatal(err)
		}
		m, err := r.metric(uint64(i+1), p)
		if err != nil {
			t.Fatal(err)
		}
		meta := freshMetadata
		meta.Version, meta.Frontier.Order, meta.Frontier.MaxTime = 3, uint64(i+1), t0-int64(i%300)
		r.install(uint64(i+1), m, meta)
	}
	return r
}
