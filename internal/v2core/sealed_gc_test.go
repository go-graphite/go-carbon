package v2core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2placement"
)

func gcFixture(t *testing.T, root string) (*pebble.DB, *sealedPartGC) {
	t.Helper()
	db, err := pebble.Open(filepath.Join(root, "db"), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	g, err := openSealedPartGC(db, root, nil)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, g
}
func gcPart(t *testing.T, g *sealedPartGC, group string, part byte, last int64) v2part.Descriptor {
	t.Helper()
	root := filepath.Dir(g.dir)
	path := filepath.Join(root, fmt.Sprintf("stage-%d.sst", part))
	spec := v2part.Spec{Scope: v2part.Scope{Group: v2part.Group(group), VShard: v2part.Route("fixture.metric"), Partition: 0, Part: v2part.PartID{part}}, Encoding: "gc-fixture-v1", Source: v2part.Provenance{Incarnation: v2part.PartID{9}, Stream: "fixture", MinOrder: 1, MaxOrder: 1}, MinTime: 0, MaxTime: last}
	policy := []byte(`{"fixture":true}`)
	spec.Policies = []v2part.ArchivePolicy{{Archive: 0, Definition: policy, Digest: sha256.Sum256(policy)}}
	w, err := v2part.NewWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	// Deterministic incompressible bytes make physical disk numbers meaningful.
	value := make([]byte, 128<<10)
	x := uint64(part) + 1
	for i := range value {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		value[i] = byte(x)
	}
	key := v2part.Key{Group: spec.Scope.Group, VShard: spec.Scope.VShard, Partition: 0, Part: spec.Scope.Part, Metric: "fixture.metric", Archive: 0, BlockStart: 0, Ordinal: 0}
	if err = w.Set(key, value); err != nil {
		t.Fatal(err)
	}
	d, err := w.Close()
	if err != nil {
		t.Fatal(err)
	}
	kv, _, err := retainSealedPart(root, path, d)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.db.Set(kv.Key, kv.Value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	return d
}
func gcPath(g *sealedPartGC, d v2part.Descriptor) string {
	return filepath.Join(g.dir, hex.EncodeToString(d.SHA256[:])+".sst")
}
func gcAdvance(g *sealedPartGC) {
	g.mu.Lock()
	g.now = func() time.Time { return time.Now().Add(2 * sealedPartGrace) }
	g.mu.Unlock()
}
func gcAuthorize(v2part.Descriptor) error { return nil }

func TestSealedGCReaderFenceGraceAndHardlinks(t *testing.T) {
	db, g := gcFixture(t, t.TempDir())
	defer db.Close()
	d := gcPart(t, g, "fixture", 1, 100)
	first := db.NewSnapshot()
	if err := db.Set([]byte("advance"), []byte("1"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	second := db.NewSnapshot()
	if err := g.retire([]v2part.Digest{d.SHA256}, nil); err == nil {
		t.Fatal("missing authority accepted")
	}
	if err := g.retire([]v2part.Digest{d.SHA256}, gcAuthorize); err != nil {
		t.Fatal(err)
	}
	latest := db.NewSnapshot()
	defer latest.Close()
	if n, err := g.sweep(); err != nil || n != 0 {
		t.Fatalf("grace bypass: %d %v", n, err)
	}
	gcAdvance(g)
	if n, err := g.sweep(); err != nil || n != 0 {
		t.Fatalf("pinned deletion: %d %v", n, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := g.sweep(); err != nil || n != 0 {
		t.Fatalf("second reader lost: %d %v", n, err)
	}
	snapshotDir := filepath.Join(t.TempDir(), "snapshot")
	inventory, err := LinkPartSnapshot(context.Background(), snapshotDir, SealedSnapshotReader{Snapshot: second, Directory: filepath.Dir(g.dir)})
	if err != nil || len(inventory.Parts) != 2 {
		t.Fatalf("old snapshot lost files: %v %v", inventory, err)
	}
	if err = second.Close(); err != nil {
		t.Fatal(err)
	}
	n, err := g.sweep()
	if err != nil || n == 0 {
		t.Fatalf("unreferenced bytes retained: %d %v", n, err)
	}
	if _, err = os.Stat(gcPath(g, d)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original retained: %v", err)
	}
	for _, p := range inventory.Parts {
		b, err := os.ReadFile(filepath.Join(snapshotDir, p.ID+".part"))
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(b)) != p.ID {
			t.Fatalf("hardlink damaged: %v", err)
		}
	}
	t.Logf("original links reclaimed=%d bytes; exported hardlinks remain until snapshot removal", n)
}

func TestSealedGCWholePartRetentionAtomicity(t *testing.T) {
	db, g := gcFixture(t, t.TempDir())
	defer db.Close()
	old := gcPart(t, g, "fixture", 1, 99)
	mixed := gcPart(t, g, "fixture", 2, 100)
	e := &coreEngine{owners: []*coreOwner{{gc: g}}}
	if err := e.ExpireSealedParts(0, []v2part.Digest{old.SHA256, mixed.SHA256}, 100); err == nil {
		t.Fatal("mixed-age part retired")
	}
	if len(g.pending) != 0 {
		t.Fatal("partial retirement after failed authorization")
	}
	if err := e.ExpireSealedParts(0, []v2part.Digest{old.SHA256}, 100); err != nil {
		t.Fatal(err)
	}
	gcAdvance(g)
	if n, err := g.sweep(); err != nil || n == 0 {
		t.Fatalf("retention: %d %v", n, err)
	}
	if _, err := os.Stat(gcPath(g, mixed)); err != nil {
		t.Fatalf("retained boundary deleted: %v", err)
	}
}

func gcMoveCertificate(t *testing.T) (v2placement.Document, v2placement.Document, v2placement.Activation) {
	t.Helper()
	b, err := os.ReadFile("../v2placement/testdata/map-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	old, err := v2placement.Decode(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	var next v2placement.Document
	if err = json.Unmarshal(b, &next); err != nil {
		t.Fatal(err)
	}
	next.Revision++
	next.Predecessor = old.Digest()
	next.Operation = "move-gc"
	r := old.Groups[0].Replicas[0]
	n := &next.Groups[0].Replicas[0]
	n.Writer = "d"
	n.Epoch++
	n.Read = append(n.Read, "d")
	digest := func(v any) string { b, _ := json.Marshal(v); return fmt.Sprintf("%x", sha256.Sum256(b)) }
	h := digest("coherent snapshot")
	roots := v2placement.Roots{Parts: h, Catalog: h, Policy: h, Tombstones: h}
	f := v2placement.Fence{Group: old.Groups[0].Selector.GroupID(), Replica: r.ID, Store: r.Writer, Epoch: r.Epoch, MapDigest: old.Digest(), Operation: next.Operation, WriterSet: digest(r.Producers), Frontier: v2placement.Frontier{{Owner: "owner-0", Cursor: 7}}, Roots: roots}
	for _, l := range r.Producers {
		f.Ends = append(f.Ends, v2placement.EndMarker{Lane: l, Final: 5, Durable: 5})
	}
	c := v2placement.Coverage{Group: f.Group, Replica: f.Replica, Store: n.Writer, Epoch: n.Epoch, MapDigest: next.Digest(), Operation: next.Operation, FenceDigest: f.Digest(), Frontier: append(v2placement.Frontier(nil), f.Frontier...), Roots: roots, Required: n.Required}
	a := v2placement.Activation{Fence: f, Coverage: c}
	if err = old.ValidateTransition(next, []v2placement.Activation{a}); err != nil {
		t.Fatal(err)
	}
	return old, next, a
}
func TestSealedGCMovedRangeRequiresFenceAndCoverage(t *testing.T) {
	old, next, a := gcMoveCertificate(t)
	authority, err := v2placement.OpenAuthority(t.TempDir(), old, a.Fence.Group, a.Fence.Replica, a.Fence.Store)
	if err != nil {
		t.Fatal(err)
	}
	_, err = authority.Fence(context.Background(), next.Operation, a.Fence.Ends, func() (v2placement.Frontier, v2placement.Roots, error) { return a.Fence.Frontier, a.Fence.Roots, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.Adopt(next, []v2placement.Activation{a}); err != nil {
		t.Fatal(err)
	}

	db, g := gcFixture(t, t.TempDir())
	defer db.Close()
	d := gcPart(t, g, a.Fence.Group, 1, 100)
	other := gcPart(t, g, "other", 2, 100)
	e := &coreEngine{owners: []*coreOwner{{gc: g}}}
	bad := a
	bad.Coverage.FenceDigest = ""
	if err := e.RetireMovedParts(0, []v2part.Digest{d.SHA256}, old, next, bad, authority); err == nil {
		t.Fatal("copy without fence accepted")
	}
	if err := e.RetireMovedParts(0, []v2part.Digest{other.SHA256}, old, next, a, authority); err == nil {
		t.Fatal("unfenced group accepted")
	}
	pinned := db.NewSnapshot()
	if err := e.RetireMovedParts(0, []v2part.Digest{d.SHA256}, old, next, a, authority); err != nil {
		t.Fatal(err)
	}
	gcAdvance(g)
	if n, err := g.sweep(); err != nil || n != 0 {
		t.Fatalf("move reader pin: %d %v", n, err)
	}
	if err := pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := g.sweep(); err != nil || n == 0 {
		t.Fatalf("move reclaim: %d %v", n, err)
	}
}

func TestSealedGCCrashBoundaries(t *testing.T) {
	if root := os.Getenv("V2_GC_CRASH_ROOT"); root != "" {
		db, g := gcFixture(t, root)
		defer db.Close()
		d := gcPart(t, g, "fixture", 1, 100)
		phase := os.Getenv("V2_GC_CRASH_PHASE")
		g.stage = func(s string) {
			if s == phase {
				os.Exit(62)
			}
		}
		if err := g.retire([]v2part.Digest{d.SHA256}, gcAuthorize); err != nil {
			t.Fatal(err)
		}
		gcAdvance(g)
		if _, err := g.sweep(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("crash boundary not reached")
	}
	for _, phase := range []string{"gc-before-publish", "gc-published", "gc-before-unlink", "gc-data-unlinked", "gc-descriptor-unlinked", "gc-directory-synced"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestSealedGCCrashBoundaries$")
			cmd.Env = append(os.Environ(), "V2_GC_CRASH_ROOT="+root, "V2_GC_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			exit := &exec.ExitError{}
			if !errors.As(err, &exit) || exit.ExitCode() != 62 {
				t.Fatalf("child: %v %s", err, out)
			}
			db, g := gcFixture(t, root)
			defer db.Close()
			if n, err := g.sweep(); err != nil || n != 0 {
				t.Fatalf("restart grace: %d %v", n, err)
			}
			gcAdvance(g)
			if _, err := g.sweep(); err != nil {
				t.Fatal(err)
			}
			paths, err := filepath.Glob(filepath.Join(g.dir, "*"))
			want := 0
			if phase == "gc-before-publish" {
				want = 2
			}
			if err != nil || len(paths) != want {
				t.Fatalf("files=%d want=%d: %v", len(paths), want, err)
			}
		})
	}
}

func TestSealedGCPublishedReplacementAndDiskBytes(t *testing.T) {
	db, g := gcFixture(t, t.TempDir())
	defer db.Close()
	var old []v2part.Digest
	var before uint64
	for i := byte(1); i <= 16; i++ {
		d := gcPart(t, g, "fixture", i, 100)
		old = append(old, d.SHA256)
		for _, p := range []string{gcPath(g, d), gcPath(g, d) + ".descriptor"} {
			st, err := os.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			before += uint64(st.Size())
		}
	}
	replacement := gcPart(t, g, "fixture", 17, 100)
	// Merge owner publishes its replacement before asking GC to retire inputs.
	authorize := func(v2part.Descriptor) error {
		_, c, err := db.Get(partBinding(sealedPartPrefix, replacement.SHA256))
		if err == nil {
			err = c.Close()
		}
		return err
	}
	if err := g.retire(old, authorize); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := g.sweep()
	if err != nil || reclaimed != before {
		t.Fatalf("reclaimed=%d expected=%d: %v", reclaimed, before, err)
	}
	var after uint64
	paths, err := filepath.Glob(filepath.Join(g.dir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		after += uint64(st.Size())
	}
	t.Logf("fixture input_parts=16 replacement_parts=1 original_file_bytes_before=%d after=%d reclaimed=%d", before+after, after, reclaimed)
}

func TestSealedGCOrphanReplayWithinGrace(t *testing.T) {
	root := t.TempDir()
	db, g := gcFixture(t, root)
	d := gcPart(t, g, "fixture", 1, 100)
	// Emulate an unpublished original by removing the descriptor binding.
	if err := db.Delete(partBinding(sealedPartPrefix, d.SHA256), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, g = gcFixture(t, root)
	defer db.Close()
	if len(g.pending) != 1 {
		t.Fatal("startup orphan not discovered")
	}
	// The exact deterministic bytes must be reusable before startup grace ends.
	replay := gcPart(t, g, "fixture", 1, 100)
	if replay.SHA256 != d.SHA256 {
		t.Fatal("fixture replay differs")
	}
	gcAdvance(g)
	if n, err := g.sweep(); err != nil || n != 0 {
		t.Fatalf("replayed original deleted: %d %v", n, err)
	}
	if _, err := os.Stat(gcPath(g, d)); err != nil {
		t.Fatal(err)
	}
}

func TestSealedGCLiveOwnerRefreshesIdleSnapshot(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	root := t.TempDir()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	_, policy := rollupTestPolicy(t, "gc.live", "1s:1d", sealrollup.Last, .5)
	rows := []Row{{ID: 7, Time: 1700000000, Bits: 1, Seq: 1}}
	if err = e.AppendBatches(0, []Batch{{ID: 7, Order: 1, Clock: 1700000002, Policy: policy, Points: rows}}, 1, true); err != nil {
		t.Fatal(err)
	}
	o := e.owners[0]
	pinned := o.db.NewSnapshot()
	files, err := (SealedSnapshotReader{Snapshot: pinned, Directory: o.dir}).ReadPartFiles(context.Background())
	if err != nil || len(files) != 2 {
		t.Fatalf("inventory: %d %v", len(files), err)
	}
	b, err := os.ReadFile(files[1].Path)
	if err != nil {
		t.Fatal(err)
	}
	d, err := v2part.ParseDescriptor(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.RetireSealedParts(0, []v2part.Digest{d.SHA256}, gcAuthorize); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		current := o.pub.acquire()
		_, c, getErr := current.s.Get(partBinding(sealedPartPrefix, d.SHA256))
		if getErr == nil {
			_ = c.Close()
		}
		current.release()
		if errors.Is(getErr, pebble.ErrNotFound) {
			break
		}
		if getErr != nil {
			t.Fatal(getErr)
		}
		if time.Now().After(deadline) {
			t.Fatal("idle owner still publishes retired binding")
		}
		time.Sleep(time.Millisecond)
	}
	gcAdvance(o.gc)
	if n, err := o.gc.sweep(); err != nil || n != 0 {
		t.Fatalf("live pin: %d %v", n, err)
	}
	if err = pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = o.gc.sweep(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(gcPath(o.gc, d)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("idle owner prevents reclaim: %v", err)
	}
	if e.DurableFrontiers()[0][0] != 1 {
		t.Fatal("GC advanced input frontier")
	}
}
