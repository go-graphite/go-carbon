package chunkstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func moveReclaimMetric(t *testing.T, s *Store, codec ContinuationRelayouter, m Metadata) {
	t.Helper()
	ctx := context.Background()
	p, err := codec.RelayoutPlan(ctx, s.newContinuationIO(ctx, s.db, nil, m))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.relayoutGroup(ctx, []relayoutItem{{m: m, plan: p}}); err != nil {
		t.Fatal(err)
	}
}

func TestRelayoutReclaimJournalSurvivesCursorAndReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fs := vfs.NewMem()
	codec := relayoutCodec{}
	opts := Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutReclaimBytes: 15}
	s, _ := openTestStore(t, "/store", opts)
	var metas []Metadata
	for i := 0; i < 8; i++ {
		m := createTestMetric(t, s, fmt.Sprintf("journal.%d", i))
		metas = append(metas, m)
		moveReclaimMetric(t, s, codec, m)
		if err := s.setRelayoutCursor(m.ID, false); err != nil {
			t.Fatal(err)
		}
	}
	if st := s.RelayoutStats(); st.Reclaims != 2 || st.UnreclaimedBytes != 10 {
		t.Fatalf("batch boundaries: %+v", st)
	}
	closeTestStore(t, s)
	s, _ = openTestStore(t, "/store", opts)
	defer closeTestStore(t, s)
	if cursor, done, err := s.relayoutCursor(); err != nil || done || cursor != 8 {
		t.Fatalf("cursor: %d %v %v", cursor, done, err)
	}
	if err := s.RelayoutContinuation(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if st := s.RelayoutStats(); !st.Done || st.Blocks != 0 || st.Bytes != 0 || st.ReclaimedBytes != 10 || st.UnreclaimedBytes != 0 {
		t.Fatalf("startup catch-up: %+v", st)
	}
	keys, _, _, err := s.readRelayoutReclaims(ctx)
	if err != nil || len(keys) != 0 {
		t.Fatalf("journal not cleared: %d %v", len(keys), err)
	}
	for _, m := range metas {
		io := s.newContinuationIO(ctx, s.db, nil, m)
		if v, err := io.Get(ContinuationBucketKey(0, []byte("c"))); err != nil || string(v) != "group" {
			t.Fatalf("live copy: %q %v", v, err)
		}
	}
}

func TestRelayoutCursorReadsLegacyKey(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	legacy := binary.BigEndian.AppendUint64(nil, 7)
	legacy = append(legacy, 0)
	if err := s.db.Set([]byte("z/relayout"), legacy, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if n, done, err := s.relayoutCursor(); err != nil || n != 7 || done {
		t.Fatalf("legacy: %d %v %v", n, done, err)
	}
	if err := s.setRelayoutCursor(9, false); err != nil {
		t.Fatal(err)
	}
	if n, done, err := s.relayoutCursor(); err != nil || n != 9 || done {
		t.Fatalf("new cursor: %d %v %v", n, done, err)
	}
	if err := s.db.Set(relayoutCursorKey(), []byte{1}, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.relayoutCursor(); err == nil {
		t.Fatal("corrupt new cursor fell back to legacy state")
	}
}

type reclaimBudgetFS struct {
	vfs.FS
	store    atomic.Pointer[Store]
	external atomic.Uint64
	peak     atomic.Uint64
}

func (f *reclaimBudgetFS) GetDiskUsage(string) (vfs.DiskUsage, error) {
	var pending, total uint64
	if s := f.store.Load(); s != nil {
		m := &s.relayout.reclaim
		m.mu.Lock()
		pending = m.pending
		total = m.pending + m.reserved
		m.mu.Unlock()
	}
	for p := f.peak.Load(); total > p; p = f.peak.Load() {
		if f.peak.CompareAndSwap(p, total) {
			break
		}
	}
	used := pending + f.external.Load()
	free := uint64(740)
	if used > free {
		free = 0
	} else {
		free -= used
	}
	return vfs.DiskUsage{AvailBytes: free}, nil
}
func TestRelayoutReclaimHalfHeadroomAndLiveThreshold(t *testing.T) {
	t.Parallel()
	fs := &reclaimBudgetFS{FS: vfs.NewMem()}
	codec := &controlledRelayoutCodec{bytes: func(string) int { return 20 }}
	s, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutMinFreeBytes: 100, RelayoutReclaimBytes: 500})
	defer closeTestStore(t, s)
	fs.store.Store(s)
	for i := 0; i < 32; i++ {
		m := createTestMetric(t, s, fmt.Sprintf("budget.%d", i))
		moveReclaimMetric(t, s, codec, m)
		st := s.RelayoutStats()
		usage, _ := fs.GetDiskUsage("")
		if st.UnreclaimedBytes > (usage.AvailBytes-100)/2 {
			t.Fatalf("half-headroom cap exceeded: pending=%d free=%d", st.UnreclaimedBytes, usage.AvailBytes)
		}
	}
	before := s.RelayoutStats()
	if before.Reclaims < 3 || before.UnreclaimedBytes == 0 {
		t.Fatalf("no capacity-triggered batches: %+v", before)
	}
	// A live setting change must apply even while the shared byte pacer waits.
	s.relayout.rate.Store(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	paced, at := uint64(0), time.Now()
	go func() { done <- s.paceRelayout(ctx, &paced, &at) }()
	if err := s.SetRelayoutReclaimBytes(20); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(3 * time.Second); s.RelayoutStats().UnreclaimedBytes != 0; {
		if time.Now().After(deadline) {
			t.Fatal("live threshold not applied during pacing")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("pacer: %v", err)
	}
	if err := s.SetRelayoutReclaimBytes(0); err == nil {
		t.Fatal("zero threshold accepted")
	}
}

func TestRelayoutReclaimSharesWorkerReservations(t *testing.T) {
	t.Parallel()
	fs := &relayoutUsageFS{FS: vfs.NewMem()}
	fs.free.Store(1 << 30)
	codec := &controlledRelayoutCodec{bytes: func(string) int { return 20 }}
	s, _ := openTestStore(t, "/store", Options{fs: fs, ExactBlocks: true, TimeBuckets: true, Continuation: codec, RelayoutWorkers: 4, RelayoutMinFreeBytes: 1, RelayoutReclaimBytes: 120})
	defer closeTestStore(t, s)
	for i := 0; i < 64; i++ {
		createTestMetric(t, s, fmt.Sprintf("shared.%d", i))
	}
	if err := s.RelayoutContinuation(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if st := s.RelayoutStats(); !st.Done || st.UnreclaimedBytes != 0 || st.ReclaimedBytes != 1280 || st.Bytes != 1280 || st.Reclaims != 11 {
		t.Fatalf("shared batch accounting: %+v", st)
	}
}

func TestRelayoutReclaimDebtGateRetainsObligation(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	var calls atomic.Int64
	opts := (&pebble.Options{FS: fs, DisableAutomaticCompactions: true, L0CompactionThreshold: 2, L0StopWritesThreshold: 20, MemTableSize: 1 << 20, EventListener: &pebble.EventListener{CompactionBegin: func(pebble.CompactionInfo) { calls.Add(1) }}}).EnsureDefaults()
	db, err := pebble.Open("/store", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	usageFS := &relayoutUsageFS{FS: fs}
	usageFS.free.Store(1 << 30)
	s := &Store{db: db, fs: usageFS, dir: "/store", writeOptions: pebble.NoSync}
	_ = s.SetRelayoutLimits(1, 1)
	_ = s.SetRelayoutReclaimBytes(1024)
	key := append(continuationPrefix(1, 1), 'c')
	spans := map[byte]*relayoutSpan{'u': {start: key, end: key}}
	journal := binary.BigEndian.AppendUint64(append([]byte(nil), relayoutReclaimPrefix...), 1)
	b := db.NewBatch()
	_ = b.Set(key, make([]byte, 64<<10), nil)
	_ = b.Set(journal, encodeRelayoutReclaim(4096, spans), nil)
	if err := b.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte{0}, []byte{255}, false); err != nil {
		t.Fatal(err)
	}
	calls.Store(0)
	for i := 0; i < 3; i++ {
		if err := db.Set(key, []byte{byte(i)}, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if debt := db.Metrics().Compact.EstimatedDebt; debt <= 1 {
		t.Fatalf("fixture has no debt: %d", debt)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.reclaimRelayout(ctx, true, 0) }()
	for deadline := time.Now().Add(3 * time.Second); !s.RelayoutStats().Paused; {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("not debt-paused")
		}
		time.Sleep(time.Millisecond)
	}
	if calls.Load() != 0 {
		t.Fatal("compaction started above debt limit")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	keys, _, _, err := s.readRelayoutReclaims(context.Background())
	if err != nil || len(keys) != 1 {
		t.Fatal("cancelled debt wait lost obligation")
	}
	usageFS.free.Store(0)
	_ = s.SetRelayoutLimits(1, math.MaxUint64)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- s.reclaimRelayout(ctx2, true, 0) }()
	for deadline := time.Now().Add(3 * time.Second); s.RelayoutStats().PauseReason != "low_free_space"; {
		if time.Now().After(deadline) {
			cancel2()
			t.Fatal("operator reserve did not pause reclaim")
		}
		time.Sleep(time.Millisecond)
	}
	if calls.Load() != 0 {
		t.Fatal("reclaim started below reserve")
	}
	usageFS.free.Store(1 << 30)
	_ = s.SetRelayoutLimits(1, math.MaxUint64)
	select {
	case err := <-done2:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not wake reclaim")
	}

	if calls.Load() == 0 || s.RelayoutStats().UnreclaimedBytes != 0 {
		t.Fatal("recovery did not reclaim")
	}
}

func TestRelayoutReclaimCrashAfterCursor(t *testing.T) {
	const exitCode = 74
	opts := Options{ExactBlocks: true, TimeBuckets: true, Continuation: relayoutCodec{}, RelayoutReclaimBytes: 1 << 30, SyncInterval: time.Hour}
	if dir := os.Getenv("GO_CARBON_RECLAIM_CRASH_DIR"); dir != "" {
		s, _ := openTestStore(t, dir, opts)
		for _, name := range []string{"a", "b"} {
			m, err := s.Metadata(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			moveReclaimMetric(t, s, relayoutCodec{}, m)
			if err := s.setRelayoutCursor(m.ID, false); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.db.LogData(nil, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		os.Exit(exitCode)
	}
	t.Parallel()
	dir := t.TempDir()
	s, _ := openTestStore(t, dir, opts)
	for _, name := range []string{"a", "b", "c"} {
		createTestMetric(t, s, name)
	}
	closeTestStore(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRelayoutReclaimCrashAfterCursor$")
	cmd.Env = append(os.Environ(), "GO_CARBON_RECLAIM_CRASH_DIR="+dir)
	out, err := cmd.CombinedOutput()
	var status *exec.ExitError
	if !errors.As(err, &status) || status.ExitCode() != exitCode {
		t.Fatalf("child: %v\n%s", err, out)
	}
	s, _ = openTestStore(t, dir, opts)
	defer closeTestStore(t, s)
	if n, done, err := s.relayoutCursor(); err != nil || done || n != 2 {
		t.Fatalf("cursor after crash: %d %v %v", n, done, err)
	}
	keys, _, n, err := s.readRelayoutReclaims(ctx)
	if err != nil || len(keys) != 2 || n != 10 {
		t.Fatalf("pending after crash: %d %d %v", len(keys), n, err)
	}
	if err := s.RelayoutContinuation(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if st := s.RelayoutStats(); !st.Done || st.Blocks != 1 || st.ReclaimedBytes != 15 || st.UnreclaimedBytes != 0 {
		t.Fatalf("resume: %+v", st)
	}
	for _, name := range []string{"a", "b", "c"} {
		m, err := s.Metadata(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		v, err := s.newContinuationIO(ctx, s.db, nil, m).Get(ContinuationBucketKey(0, []byte("c")))
		if err != nil || string(v) != "group" {
			t.Fatalf("%s copy: %q %v", name, v, err)
		}
	}
}

func TestRelayoutReclaimRejectsCorruptJournal(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true, Continuation: relayoutCodec{}})
	defer closeTestStore(t, s)
	key := append(append([]byte(nil), relayoutReclaimPrefix...), 0)
	if err := s.db.Set(key, []byte("broken"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := s.RelayoutContinuation(context.Background(), 0); err == nil || !strings.Contains(err.Error(), "journal") {
		t.Fatalf("corruption ignored: %v", err)
	}
	if s.RelayoutStats().Done {
		t.Fatal("corrupt journal marked done")
	}
}
