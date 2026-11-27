package v2core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

type partTableBuffer struct{ bytes.Buffer }

func (b *partTableBuffer) Write(p []byte) error { _, err := b.Buffer.Write(p); return err }
func (b *partTableBuffer) Finish() error        { return nil }
func (b *partTableBuffer) Abort()               {}

// The fixture writes real SST originals and descriptors, without the writer's
// durability fsyncs. It exercises read cost independently of laptop fsync speed.
func TestOpenedRollupTwentyThousandParts(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "parts")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// Parallel unlink avoids turning the macOS fixture cleanup into the test's
	// dominant cost. TempDir still owns and removes the rest of the fixture.
	t.Cleanup(func() {
		entries, _ := os.ReadDir(dir)
		var cleaners sync.WaitGroup
		for worker := range 8 {
			cleaners.Go(func() {
				for i := worker; i < len(entries); i += 8 {
					_ = os.Remove(filepath.Join(dir, entries[i].Name()))
				}
			})
		}
		cleaners.Wait()
	})
	db, err := pebble.Open(filepath.Join(root, "manifest"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Set(rollupDirectoryKey, []byte(root), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	_, raw := rollupTestPolicy(t, "opened.background", "1s:1d", sealrollup.Last, .5)
	planner := newCoreRollups()
	plan, err := planner.plan(coreSealInput{Batches: []Batch{{ID: 1, Policy: raw, Order: 1, Clock: 1700000002, Points: []Row{{ID: 1, Time: 1700000000, Bits: 0x8000000000000000, Seq: 1}}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Abort()
	file := plan.Files[0]
	template, err := WriteDenseRollupPart(filepath.Join(root, "template.sst"), file, v2part.Provenance{Incarnation: v2part.PartID{1}, Stream: "test"})
	if err != nil {
		t.Fatal(err)
	}
	install := func(number int, rows []coreKV) {
		t.Helper()
		var part v2part.PartID
		binary.BigEndian.PutUint64(part[:], uint64(number+1))
		b := &partTableBuffer{}
		w := sstable.NewWriter(b, sstable.WriterOptions{Compression: sstable.NoCompression})
		d := template
		d.Scope.Part = part
		for i, kv := range rows {
			key, err := v2part.Decode(kv.Key)
			if err != nil {
				t.Fatal(err)
			}
			key.Part = part
			encoded, err := v2part.Encode(key)
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				d.FirstKey = bytes.Clone(encoded)
				d.Scope.Group = key.Group
				d.Scope.VShard = key.VShard
				d.VShards = []uint16{key.VShard}
			}
			d.LastKey = bytes.Clone(encoded)
			if err := w.Set(encoded, kv.encodedValue()); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		d.Bytes = uint64(b.Len())
		d.SHA256 = sha256.Sum256(b.Bytes())
		if err := os.WriteFile(filepath.Join(dir, hex.EncodeToString(d.SHA256[:])+".sst"), b.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		encoded, err := d.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Set(partBinding(sealedPartPrefix, d.SHA256), encoded, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	var writers sync.WaitGroup
	for worker := range 8 {
		writers.Go(func() {
			for i := worker; i < 20000; i += 8 {
				install(i, file)
			}
		})
	}
	writers.Wait()
	t.Logf("prepared twenty thousand real parts in %s", time.Since(start))
	// The target has a different vshard: all twenty thousand background files
	// must remain unopened even during a cold target lookup.
	name := "opened.target"
	for v2part.Route(v2part.ID(name)) == v2part.Route("opened.background") {
		name += "x"
	}
	_, targetPolicy := rollupTestPolicy(t, name, "1s:1d", sealrollup.Last, .5)
	targetPlan, err := newCoreRollups().plan(coreSealInput{Batches: []Batch{{ID: 2, Policy: targetPolicy, Order: 2, Clock: 1700000002, Points: []Row{{ID: 2, Time: 1700000000, Bits: 0x7ff8000000000042, Seq: 2}}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer targetPlan.Abort()
	install(20000, targetPlan.Files[0])
	inventory, err := loadRollupInventory(db)
	if err != nil {
		t.Fatal(err)
	}
	snap := db.NewSnapshot()
	defer func() { _ = snap.Close() }()
	parts, err := OpenRollupParts(context.Background(), snap, inventory, nil, testBlockCache(t), 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { parts.Close() }()
	cache := NewRollupReadCache(1 << 20)
	var samples []time.Duration
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := range 200 {
		if i%20 == 0 {
			install(20001+i, file)
			inv, err := loadRollupInventory(db)
			if err != nil {
				t.Fatal(err)
			}
			inv.registry = inventory.registry
			nextSnap := db.NewSnapshot()
			next, err := OpenRollupParts(context.Background(), nextSnap, inv, nil, testBlockCache(t), 4<<20)
			if err != nil {
				t.Fatal(err)
			}
			for id, p := range parts.parts {
				if next.parts[id] != p {
					t.Fatal("reopened unchanged part")
				}
			}
			parts.Close()
			parts = next
			_ = snap.Close()
			snap = nextSnap
		}
		started := time.Now()
		view, err := parts.SnapshotForNames(context.Background(), snap, []string{name}, cache)
		if err != nil {
			t.Fatal(err)
		}
		records, err := view.Records(context.Background(), name)
		if err != nil || len(records) != 1 || records[0].Sample.Bits != 0x7ff8000000000042 {
			t.Fatalf("bit-exact render: %v %v", records, err)
		}
		samples = append(samples, time.Since(started))
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	opened := 0
	for _, p := range parts.parts {
		if p.reader != nil {
			opened++
		}
	}
	if opened != 1 {
		t.Fatalf("opened %d files for one routed name", opened)
	}
	slices.Sort(samples)
	p99 := samples[len(samples)*99/100]
	if p99 > 50*time.Millisecond {
		t.Fatalf("p99 %s exceeds 50ms", p99)
	}
	t.Logf("parts=%d opened=%d render p99=%s heap before=%d after=%d fixture=%s", len(parts.parts), opened, p99, before.HeapAlloc, after.HeapAlloc, time.Since(start))
	// Cancellation while another caller owns the part-open token must return
	// immediately; the next generation still owns exactly one reference.
	p := parts.routes[v2part.Route(v2part.ID(name))][0]
	<-p.gate
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = parts.SnapshotForNames(ctx, snap, []string{name}, cache)
	p.gate <- struct{}{}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	parts.registry.mu.Lock()
	refs := p.refs
	parts.registry.mu.Unlock()
	if refs != 1 {
		t.Fatalf("references leaked: %d", refs)
	}
}

// testBlockCache is a per-test block cache; opened parts take their own references.
func testBlockCache(t testing.TB) *pebble.Cache {
	c := pebble.NewCacheWithShards(4<<20, 1)
	t.Cleanup(c.Unref)
	return c
}
