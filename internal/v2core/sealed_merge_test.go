package v2core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func mergeFixture(t *testing.T, root string, stage func(string)) *coreEngine {
	t.Helper()
	e, err := openCoreWithHooks(root, adoptConfig(), coreHooks{Stage: stage})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func mergeAppend(t *testing.T, e *coreEngine, order uint64) {
	t.Helper()
	_, policy := rollupTestPolicy(t, "merge.metric", "1s:120,10s:1h", sealrollup.Last, .5)
	values := []uint64{0, 1 << 63, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff80000000000ab, math.Float64bits(42)}
	stamp := int64(1000 + order%24)
	rows := []row{{ID: 7, Time: stamp, Bits: values[order%uint64(len(values))], Seq: order * 3}, {ID: 7, Time: stamp - 4, Bits: values[(order+1)%uint64(len(values))], Seq: order*3 + 1}, {ID: 7, Time: stamp, Bits: values[(order+2)%uint64(len(values))], Seq: order*3 + 2}}
	if err := e.submitBatches(0, []coreBatch{{ID: 7, Policy: policy, Order: order, Clock: 1030, Points: rows}}, order, true); err != nil {
		t.Fatal(err)
	}
}
func mergeRead(t *testing.T, s *pebble.Snapshot) []sealrollup.Record {
	t.Helper()
	var out []sealrollup.Record
	index, err := NewRollupSnapshot(context.Background(), s, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var readErr error
	index.Metadata(func(name string, _ RollupPolicy, _ RollupMetadata) {
		rs, err := index.Records(context.Background(), name)
		if err != nil {
			readErr = err
		}
		out = append(out, rs...)
	})
	if readErr != nil {
		t.Fatal(readErr)
	}

	slices.SortFunc(out, func(a, b sealrollup.Record) int {
		if a.Archive != b.Archive {
			return a.Archive - b.Archive
		}
		return int(a.Slot - b.Slot)
	})
	return out
}
func mergeCount(o *coreOwner) int {
	o.gc.mu.Lock()
	defer o.gc.mu.Unlock()
	return len(o.gc.inventory.bindings)
}
func mergeDrain(t *testing.T, o *coreOwner) int {
	t.Helper()
	count := 0
	for {
		changed, err := o.mergeSealedParts(nil)
		if err != nil {
			t.Fatal(err)
		}
		if !changed {
			return count
		}
		count++
	}
}
func TestSealedMergeDifferential(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	root := t.TempDir()
	e := mergeFixture(t, root, nil)
	defer func() {
		if e != nil {
			_ = e.close()
		}
	}()
	for order := uint64(1); order <= 64; order++ {
		mergeAppend(t, e, order)
	}
	o := e.owners[0]
	pin := o.db.NewSnapshot()
	want := mergeRead(t, pin)
	before := mergeCount(o)
	merges := mergeDrain(t, o)
	current := o.db.NewSnapshot()
	got := mergeRead(t, current)
	_ = current.Close()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merge changed exact records\ngot=%+v\nwant=%+v", got, want)
	}
	if merges == 0 || mergeCount(o) >= before {
		t.Fatalf("parts not bounded: %d -> %d", before, mergeCount(o))
	}
	gcAdvance(o.gc)
	if _, err := o.gc.sweep(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mergeRead(t, pin), want) {
		t.Fatal("pinned generation lost input")
	}
	_ = pin.Close()
	// The engine's own last publication still pins old inputs until the owner
	// acknowledges gcDirty; force that normal publication before testing unlink.
	deadline := time.Now().Add(time.Second)
	for o.gcDirty.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := o.gc.sweep(); err != nil {
		t.Fatal(err)
	}
	if err := e.close(); err != nil {
		t.Fatal(err)
	}
	e = nil
	e = mergeFixture(t, root, nil)
	s := e.owners[0].db.NewSnapshot()
	defer s.Close()
	if !reflect.DeepEqual(mergeRead(t, s), want) {
		t.Fatal("restart changed stored winners")
	}
	t.Logf("live parts %d -> %d in %d merges", before, mergeCount(e.owners[0]), merges)
}

func TestSealedMergeKillRecovery(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	if root := os.Getenv("V2_MERGE_CRASH_ROOT"); root != "" {
		phase := os.Getenv("V2_MERGE_CRASH_PHASE")
		e := mergeFixture(t, root, func(stage string) {
			if stage == phase {
				_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			}
		})
		_, err := e.owners[0].mergeSealedParts(nil)
		t.Fatalf("crash boundary missed: %v", err)
	}
	for _, phase := range []string{"merge-written", "merge-retained", "merge-before-publish", "merge-published"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			e := mergeFixture(t, root, nil)
			for n := uint64(1); n <= sealedMergeFanIn; n++ {
				mergeAppend(t, e, n)
			}
			s := e.owners[0].db.NewSnapshot()
			want := mergeRead(t, s)
			_ = s.Close()
			if err := e.close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSealedMergeKillRecovery$")
			cmd.Env = append(os.Environ(), "V2_MERGE_CRASH_ROOT="+root, "V2_MERGE_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("child not killed: %v %s", err, out)
			}
			e = mergeFixture(t, root, nil)
			defer e.close()
			s = e.owners[0].db.NewSnapshot()
			defer s.Close()
			if !reflect.DeepEqual(mergeRead(t, s), want) {
				t.Fatal("crash lost or changed slots")
			}
			expected := sealedMergeFanIn
			if phase == "merge-published" {
				expected = 1
			}
			if got := mergeCount(e.owners[0]); got != expected {
				t.Fatalf("live=%d want %d", got, expected)
			}
			t.Logf("SIGKILL at %s recovered %d live parts", phase, expected)
		})
	}
}

func TestSealedMergeMoveDuringWrite(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	policy := adoptPolicy(t, sealrollup.Last)
	donor := adoptDonor(t, policy, 100, moveSample(1000, 1), moveSample(1001, 3), sealrollup.Sample{Time: 1003, Bits: 1 << 63})
	prepared, err := PrepareMove(context.Background(), t.TempDir(), []MovePart{donor})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	written, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	e := mergeFixture(t, t.TempDir(), func(stage string) {
		if stage == "merge-written" {
			once.Do(func() { close(written); <-release })
		}
	})
	defer e.close()
	for n := uint64(1); n <= sealedMergeFanIn; n++ {
		adoptLive(t, e, policy, n, n, moveSample(1000, 2), moveSample(1002, float64(n)))
	}
	o := e.owners[0]
	done := make(chan error, 1)
	go func() { _, err := o.mergeSealedParts(nil); done <- err }()
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("merge did not start")
	}
	if err = e.AdoptMove(context.Background(), prepared, map[string]uint64{"move.metric": 7}, 1010); err != nil {
		close(release)
		t.Fatal(err)
	}
	_, want := adoptedRecords(t, e)
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	mergeDrain(t, o)
	_, got := adoptedRecords(t, e)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("move authority changed\ngot=%v\nwant=%v", got, want)
	}
	// Copied SST remains unchanged, with its class and source catalog binding.
	key, _ := v2part.Decode(donor.Descriptor.FirstKey)
	s := o.db.NewSnapshot()
	defer s.Close()
	origin, err := readPartOrigin(s, key)
	if err != nil || origin.Class != CopiedPart {
		t.Fatalf("copy reclassified: %+v %v", origin, err)
	}
}

func TestSealedMergeBackgroundBound(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "1")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	for n := uint64(1); n <= 80; n++ {
		mergeAppend(t, e, n)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && mergeCount(e.owners[0]) > 12 {
		time.Sleep(20 * time.Millisecond)
	}
	if got := mergeCount(e.owners[0]); got > 12 {
		t.Fatalf("background backlog=%d", got)
	}
	if failure := e.gcFailure.Load(); failure != nil {
		t.Fatal(failure.err)
	}
	t.Logf("background live parts=%d", mergeCount(e.owners[0]))
}

// Install exact immutable fixture records through the ordinary descriptor
// namespace; no reducer state is consulted by merge selection or publication.
func installMergeRecords(t *testing.T, o *coreOwner, part byte, metas []RollupMetadata, records [][]sealrollup.Record) v2part.Descriptor {
	t.Helper()
	var file []KV
	for i, m := range metas {
		var policy RollupPolicy
		if err := json.Unmarshal(m.Policy, &policy); err != nil {
			t.Fatal(err)
		}
		key := v2part.Key{Group: policy.Group, VShard: v2part.Route(policy.Name), Part: v2part.PartID{part}, Metric: policy.Name}
		for _, r := range records[i] {
			key.Archive = uint32(r.Archive)
			key.BlockStart = r.Slot
			enc, err := v2part.Encode(key)
			if err != nil {
				t.Fatal(err)
			}
			file = append(file, KV{Key: enc, Value: sealrollup.EncodeRecord(r)})
		}
		key.Archive = v2part.MetadataArchive
		key.BlockStart = 0
		enc, err := v2part.Encode(key)
		if err != nil {
			t.Fatal(err)
		}
		file = append(file, KV{Key: enc, Value: AppendRollupMetadata(nil, m)})
	}
	slices.SortFunc(file, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
	path := filepath.Join(t.TempDir(), "input.sst")
	d, err := WriteDenseRollupPart(path, file, o.partSource)
	if err != nil {
		t.Fatal(err)
	}
	kv, _, err := retainSealedPart(o.dir, path, d)
	if err != nil {
		t.Fatal(err)
	}
	o.gc.mu.Lock()
	defer o.gc.mu.Unlock()
	if err = o.db.Set(kv.Key, kv.Value, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	o.gc.inventory = o.gc.inventory.with([]coreKV{kv}, nil)
	return d
}

func TestSealedMergeLifecycleAndRoutes(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprint(expired), func(t *testing.T) {
			e := mergeFixture(t, t.TempDir(), nil)
			defer e.close()
			o := e.owners[0]
			// Different exact routes in one group must remain present in the output.
			policies := make([]RollupPolicy, 2)
			encoded := make([][]byte, 2)
			for i, name := range []string{"merge.reset", "merge.other"} {
				policies[i], encoded[i] = rollupTestPolicy(t, name, "1s:120,10s:1h", sealrollup.Last, .5)
			}
			for n := byte(1); n <= sealedMergeFanIn; n++ {
				metas := make([]RollupMetadata, 2)
				rs := make([][]sealrollup.Record, 2)
				for i := range metas {
					digest, err := sealrollup.PolicyDigest(policies[i].Config)
					if err != nil {
						t.Fatal(err)
					}
					reset := uint64(0)
					if n >= 2 {
						reset = 2
					}
					isExpired := n == 2 || (expired && n == sealedMergeFanIn)
					if expired && n == sealedMergeFanIn {
						reset = sealedMergeFanIn
					}
					metas[i] = RollupMetadata{Version: 2, ID: uint64(i + 1), Policy: encoded[i], Frontier: sealrollup.Frontier{Order: uint64(n), MaxTime: 1120, Policy: digest}, LastUpdate: 1130e9, Revision: uint64(n), ResetOrder: reset, Expired: isExpired}
					if !isExpired {
						stamp := int64(1000)
						if n == 1 {
							stamp = 1120
						}
						rs[i] = []sealrollup.Record{{Delta: sealrollup.Delta{Archive: 0, Slot: 40, Order: uint64(n), Sample: sealrollup.Sample{Time: stamp, Bits: []uint64{0, 0x7ff8000000000042, 0, 1 << 63, 0x7ff0000000000000}[1+(n-1)%4]}}}}
					}
				}
				installMergeRecords(t, o, n, metas, rs)
			}
			s := o.db.NewSnapshot()
			want := mergeRead(t, s)
			_ = s.Close()
			if got := mergeDrain(t, o); got != 1 {
				t.Fatalf("merges=%d", got)
			}
			s = o.db.NewSnapshot()
			defer s.Close()
			got := mergeRead(t, s)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("lifecycle changed records got=%v want=%v", got, want)
			}
			if expired && len(got) != 0 {
				t.Fatal("expired records retained")
			}
			if !expired && (len(got) != 2 || got[0].Order != sealedMergeFanIn || got[1].Order != sealedMergeFanIn) {
				t.Fatalf("reset lost revived winners: %v", got)
			}
			o.gc.mu.Lock()
			inventory := o.gc.inventory
			o.gc.mu.Unlock()
			for _, value := range inventory.bindings {
				d, err := v2part.ParseDescriptor(value)
				if err != nil {
					t.Fatal(err)
				}
				if len(d.VShards) != 2 {
					t.Fatalf("routes=%v", d.VShards)
				}
				if err = v2part.ReadExpanded(context.Background(), gcPath(o.gc, d), &d, nil); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSealedMergeOverloadKeepsIntakeMoving(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "0")
	e := mergeFixture(t, t.TempDir(), nil)
	defer e.close()
	o := e.owners[0]
	// No merger runs: crossing the former hard limit must still seal promptly.
	for n := uint64(1); n <= 2*sealedMergeFanIn; n++ {
		mergeAppend(t, e, n)
	}
	o.mergeStop = make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); mergeAppend(t, e, 2*sealedMergeFanIn+1) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("seal stalled behind merge backlog")
	}
	if got := mergeCount(o); got != 2*sealedMergeFanIn+1 {
		t.Fatalf("parts=%d", got)
	}
	if got := e.PartGCStats()[0].MergeOverBudgetSeals; got != 1 {
		t.Fatalf("pressure seals=%d", got)
	}
	if n := mergeDrain(t, o); n == 0 {
		t.Fatal("backlog did not drain")
	}
}

func TestSealedMergeWorkerBudget(t *testing.T) {
	t.Setenv("CORE_MERGE_WORKERS", "")
	for _, owners := range []int{1, 16, 96} {
		got, err := mergeWorkerCount(owners)
		want := min(owners, max(1, runtime.GOMAXPROCS(0)/2))
		if err != nil || got != want {
			t.Fatalf("owners=%d workers=%d want=%d err=%v", owners, got, want, err)
		}
		t.Setenv("CORE_MERGE_WORKERS", strconv.Itoa(want+1))
		if _, err = mergeWorkerCount(owners); err == nil {
			t.Fatal("CPU budget exceeded")
		}
		t.Setenv("CORE_MERGE_WORKERS", "0")
		if got, err = mergeWorkerCount(owners); err != nil || got != 0 {
			t.Fatalf("disable: %d %v", got, err)
		}
		t.Setenv("CORE_MERGE_WORKERS", "")
	}
}
