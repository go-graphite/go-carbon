package v2core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func coreTestConfig() coreConfig {
	return coreConfig{AppendParts: true, Shards: 4, Producers: 2, MaxRows: 64, PartitionSeconds: 60}
}
func coreRows(producer, batch int) []row {
	var out []row
	for i := 0; i < 43; i++ {
		seq := uint64(batch*1000 + producer*100 + i)
		bits := math.Float64bits(float64(seq))
		switch i % 9 {
		case 0:
			bits = 0x8000000000000000
		case 1:
			bits = 0x7ff8000000000042
		case 2:
			bits = 1
		case 3:
			bits = 0xfff0000000000000
		}
		out = append(out, row{uint64(i % 11), int64(-90 + batch*13 + i%7), bits, seq})
	}
	return out
}
func coreDigest(root string, cfg coreConfig) (string, error) {
	h := sha256.New()
	for s := 0; s < cfg.Shards; s++ {
		db, e := pebble.Open(filepath.Join(root, fmt.Sprintf("owner-%03d", s)), &pebble.Options{})
		if e != nil {
			return "", e
		}
		snap := db.NewSnapshot()
		for id := uint64(0); id < 11; id++ {
			if coreShard(id, cfg.Shards) != s {
				continue
			}
			rs, e := coreRead(snap, id)
			if e != nil {
				return "", e
			}
			for _, r := range rs {
				var b [32]byte
				putRow(b[:], r)
				_, _ = h.Write(b[:])
			}
		}
		_ = snap.Close()
		if e = db.Close(); e != nil {
			return "", e
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
func TestCoreReplayAndProducerRouting(t *testing.T) {
	t.Parallel()
	for _, blind := range []bool{false, true} {
		t.Run(fmt.Sprint(blind), func(t *testing.T) {
			t.Parallel()
			cfg := coreTestConfig()
			cfg.BlindBatch = blind
			root := t.TempDir()
			e, err := openCore(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for p := 0; p < cfg.Producers; p++ {
				wg.Add(1)
				go func(p int) {
					defer wg.Done()
					for b := 1; b <= 9; b++ {
						if err := e.submit(p, coreRows(p, b), uint64(b), b%3 == 0); err != nil {
							t.Error(err)
							return
						}
					}
				}(p)
			}
			wg.Wait()
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
			before, err := coreDigest(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			e, err = openCore(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(e.globalFrontier(), []uint64{9, 9}) {
				t.Fatal(e.globalFrontier())
			}
			// Replaying the complete input prefix must neither duplicate nor alter bits.
			for p := 0; p < cfg.Producers; p++ {
				for b := 1; b <= 9; b++ {
					if err = e.submit(p, coreRows(p, b), uint64(b), true); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err = e.close(); err != nil {
				t.Fatal(err)
			}
			after, err := coreDigest(root, cfg)
			if err != nil || before != after {
				t.Fatalf("hash mismatch %s %s %v", before, after, err)
			}
			for s := 0; s < cfg.Shards; s++ {
				db, err := pebble.Open(filepath.Join(root, fmt.Sprintf("owner-%03d", s)), &pebble.Options{})
				if err != nil {
					t.Fatal(err)
				}
				snap := db.NewSnapshot()
				for id := uint64(0); id < 11; id++ {
					if coreShard(id, cfg.Shards) != s {
						continue
					}
					got, err := coreRead(snap, id)
					if err != nil {
						t.Fatal(err)
					}
					var want []row
					for p := 0; p < cfg.Producers; p++ {
						for b := 1; b <= 9; b++ {
							for _, r := range coreRows(p, b) {
								if r.ID == id {
									want = append(want, r)
								}
							}
						}
					}
					slices.SortFunc(want, less)
					if !slices.Equal(got, want) {
						t.Fatalf("ID%d raw mismatch got%d want%d", id, len(got), len(want))
					}
				}
				_ = snap.Close()
				_ = db.Close()
			}
		})
	}
}
func TestCoreSubmitDetachesCallerRows(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	e, err := openCore(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := e.close(); err != nil {
			t.Error(err)
		}
	}()
	var want []row
	for batch := 1; batch <= 2; batch++ {
		rows := coreRows(0, batch)
		want = append(want, rows...)
		if err := e.submit(0, rows, uint64(batch), false); err != nil {
			t.Fatal(err)
		}
		// Callers may immediately reuse their input after submit returns.
		clear(rows)
	}
	if err := e.submit(0, nil, 3, true); err != nil {
		t.Fatal(err)
	}
	for owner, o := range e.owners {
		snap := o.db.NewSnapshot()
		for id := uint64(0); id < 11; id++ {
			if coreShard(id, cfg.Shards) != owner {
				continue
			}
			var expected []row
			for _, r := range want {
				if r.ID == id {
					expected = append(expected, r)
				}
			}
			slices.SortFunc(expected, less)
			got, err := coreRead(snap, id)
			if err != nil || !slices.Equal(got, expected) {
				t.Errorf("ID%d caller reuse changed durable rows: %v", id, err)
			}
		}
		if err := snap.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCoreIdleOneSecondSeal(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	// The default time threshold is longer; this test owns a one-second one.
	cfg.SealInterval = time.Second
	e, err := openCore(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	if err = e.submit(0, coreRows(0, 1), 1, false); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		var n uint64
		for _, o := range e.owners {
			n += o.committed.Load()
		}
		if n == 43 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("idle heads were not sealed by timer")
}
func TestCoreWholePartitionExpiryPinnedSnapshot(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	root := t.TempDir()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rs := []row{{0, -61, 1, 1}, {0, -60, 2, 2}, {0, -1, 3, 3}, {0, 0, 4, 4}}
	if err = e.submit(0, rs, 1, true); err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(filepath.Join(root, fmt.Sprintf("owner-%03d", coreShard(0, cfg.Shards))), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	snap := db.NewSnapshot()
	if err = coreExpire(db, 0); err != nil {
		t.Fatal(err)
	}
	old, err := coreRead(snap, 0)
	if err != nil || len(old) != 4 {
		t.Fatalf("pinned snapshot changed: %v %v", old, err)
	}
	fresh := db.NewSnapshot()
	got, err := coreRead(fresh, 0)
	if err != nil || len(got) != 1 || got[0].Time != 0 {
		t.Fatalf("expiry %v %v", got, err)
	}
	_ = fresh.Close()
	_ = snap.Close()
	_ = db.Close()
}
func TestCoreCrashReplay(t *testing.T) {
	// The child performs real durable writes before signaling readiness. Keep
	// subprocess crash tests outside the parallel disk-heavy fixture pool.
	if root := os.Getenv("IMMUTABLE_CORE_CRASH_ROOT"); root != "" {
		cfg := coreTestConfig()
		e, err := openCore(root, cfg)
		if err != nil {
			panic(err)
		}
		for b := 1; b <= 12; b++ {
			for p := 0; p < cfg.Producers; p++ {
				if err = e.submit(p, coreRows(p, b), uint64(b), true); err != nil {
					panic(err)
				}
			}
			if b == 5 {
				// Simulate a crash between shard publications: one owner commits the
				// next input position, while all others retain the prior frontier.
				o := e.owners[0]
				var ahead []row
				for _, r := range coreRows(0, 6) {
					if coreShard(r.ID, cfg.Shards) == 0 {
						ahead = append(ahead, r)
					}
				}
				ack := make(chan error, 1)
				if err = o.queues[0].push(&corePacket{rows: ahead, cursor: 6, seal: true, ack: ack}, o.dead); err != nil {
					panic(err)
				}
				if err = <-ack; err != nil {
					panic(err)
				}
				_ = os.WriteFile(filepath.Join(root, "crash-ready"), []byte("ready"), 0600)
				select {}
			}
		}
		panic("unreachable")
	}
	cfg := coreTestConfig()
	root := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCoreCrashReplay$")
	cmd.Env = append(os.Environ(), "IMMUTABLE_CORE_CRASH_ROOT="+root)
	log, err := os.Create(filepath.Join(t.TempDir(), "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	until := time.Now().Add(15 * time.Second)
	for {
		if _, err = os.Stat(filepath.Join(root, "crash-ready")); err == nil {
			break
		}
		if time.Now().After(until) {
			t.Fatal("child did not reach crash point")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(e.globalFrontier(), []uint64{5, 5}) {
		t.Fatalf("unsafe global frontier %v", e.globalFrontier())
	}
	for b := 1; b <= 12; b++ {
		for p := 0; p < cfg.Producers; p++ {
			if err = e.submit(p, coreRows(p, b), uint64(b), true); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	got, err := coreDigest(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	clean := t.TempDir()
	e, err = openCore(clean, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for b := 1; b <= 12; b++ {
		for p := 0; p < cfg.Producers; p++ {
			if err = e.submit(p, coreRows(p, b), uint64(b), true); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	want, err := coreDigest(clean, cfg)
	if err != nil || got != want {
		t.Fatalf("crash hash %s clean %s %v", got, want, err)
	}
	t.Logf("SIGKILL/replay raw archive SHA256 %s", got)
}
func TestCoreRejectChangedConfig(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	root := t.TempDir()
	e, err := openCore(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, nil, 1, true); err != nil {
		t.Fatal(err)
	}
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
	cfg.Producers++
	if e, err = openCore(root, cfg); err == nil {
		_ = e.close()
		t.Fatal("accepted changed producer cardinality")
	}
}

func TestCoreJournalKillReplay(t *testing.T) {
	// The child performs real durable writes before signaling readiness. Keep
	// subprocess crash tests outside the parallel disk-heavy fixture pool.
	if root := os.Getenv("IMMUTABLE_CORE_JOURNAL_ROOT"); root != "" {
		err := coreReplayWithHook(filepath.Join(root, "input.log"), filepath.Join(root, "crashed"), coreTestConfig(), func(shard int, snap *pebble.Snapshot, _ []row) error {
			var meta coreFrontier
			if b, c, e := snap.Get(coreMetaKey); e == nil {
				_ = json.Unmarshal(b, &meta)
				_ = c.Close()
			}
			_ = snap.Close()
			if shard == 0 && len(meta.Cursors) > 0 && meta.Cursors[0] >= 256 {
				if e := os.WriteFile(filepath.Join(root, "journal-crash-ready"), []byte("durable frontier"), 0600); e != nil {
					return e
				}
				select {}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		return
	}
	root := t.TempDir()
	journal := filepath.Join(root, "input.log")
	f, err := os.Create(journal)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := inputlog.NewSegmentWriter(f, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Sixteen groups cover all names/IEEE cases and multiple time partitions.
	// At 220 points/group the crash at cursor >=256 leaves both a durable
	// prefix and a longer tail. The total remains a multiple of the 64-row
	// replay batch, retaining empty-final-flush coverage without 80 repeats.
	for b := 1; b <= 16; b++ {
		var groups []*points.Points
		for id := 0; id < 11; id++ {
			p := &points.Points{Metric: fmt.Sprintf("sample.%d;b=2;a=1", id)}
			for i := 0; i < 20; i++ {
				bits := uint64(0x8000000000000000)
				if i%2 == 1 {
					bits = 0x7ff8000000000042
				}
				p.Data = append(p.Data, points.Point{Timestamp: int64(1800000000 + b*20 + i), Value: math.Float64frombits(bits)})
			}
			groups = append(groups, p)
		}
		if _, err = writer.Append(groups, nil); err != nil {
			t.Fatal(err)
		}
	}
	writer.Close()
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	// Crash while the actual journal decoder, router and SST publication path run.
	cmd := exec.Command(os.Args[0], "-test.run=^TestCoreJournalKillReplay$")
	cmd.Env = append(os.Environ(), "IMMUTABLE_CORE_JOURNAL_ROOT="+root)
	log, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(15 * time.Second)
	for {
		if _, e := os.Stat(filepath.Join(root, "journal-crash-ready")); e == nil {
			break
		}
		if time.Now().After(until) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("journal did not reach durable frontier")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	cfg := coreTestConfig()
	if err = coreReplay(journal, filepath.Join(root, "crashed"), cfg); err != nil {
		t.Fatal(err)
	}
	if err = coreReplay(journal, filepath.Join(root, "clean"), cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Producers = 1
	got, err := coreDigest(filepath.Join(root, "crashed"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	want, err := coreDigest(filepath.Join(root, "clean"), cfg)
	if err != nil || got != want {
		t.Fatalf("actual journal replay %s vs %s %v", got, want, err)
	}
	t.Logf("actual journal SIGKILL/replay raw archive SHA256 %s", got)
}

func TestCorePublishesDetachedFrozenHead(t *testing.T) {
	t.Parallel()
	cfg := coreTestConfig()
	cfg.MaxRows = 1024
	type view struct {
		shard int
		snap  *pebble.Snapshot
		head  []row
	}
	views := make(chan view, 64)
	e, err := openCoreWithHook(t.TempDir(), cfg, func(s int, snap *pebble.Snapshot, head []row) error { views <- view{s, snap, head}; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = e.submit(0, coreRows(0, 1), 1, false); err != nil {
		t.Fatal(err)
	}
	var frozen []view
	for len(frozen) < cfg.Shards {
		select {
		case v := <-views:
			frozen = append(frozen, v)
		case <-time.After(5 * time.Second):
			t.Fatal("head not published")
		}
	}
	var n int
	for _, v := range frozen {
		n += len(v.head)
	}
	if n != 43 {
		t.Fatal(n)
	}
	before := make([][]row, len(frozen))
	for i, v := range frozen {
		before[i] = slices.Clone(v.head)
	}
	if err = e.submit(0, coreRows(0, 2), 2, true); err != nil {
		t.Fatal(err)
	}
	for i, v := range frozen {
		if !slices.Equal(before[i], v.head) {
			t.Fatal("frozen head mutated")
		}
		_ = v.snap.Close()
	}
	// Hooks retain snapshots independently; release all before closing storage.
	for {
		select {
		case v := <-views:
			_ = v.snap.Close()
		default:
			goto drained
		}
	}
drained:
	if err = e.close(); err != nil {
		t.Fatal(err)
	}
}

func TestCoreSortMatchesOriginalComparator(t *testing.T) {
	rng := rand.New(rand.NewPCG(41, 93))
	for trial := range 100 {
		var rows []row
		for i := range 1024 {
			stamp := rng.Int64N(7200) - 3600
			if trial%2 == 0 {
				stamp = 1800000000 + rng.Int64N(30)
			}
			rows = append(rows, row{rng.Uint64N(10), stamp, rng.Uint64(), uint64(i)})
		}
		if trial == 0 {
			rows = append(rows, row{Time: math.MinInt64}, row{Time: math.MaxInt64})
		}
		want := slices.Clone(rows)
		slices.SortFunc(want, func(a, b row) int {
			pa, pb := corePartition(a.Time, 3600), corePartition(b.Time, 3600)
			if pa < pb {
				return -1
			}
			if pa > pb {
				return 1
			}
			return less(a, b)
		})
		sortCoreRows(rows, 3600)
		if !slices.Equal(rows, want) {
			t.Fatalf("partition sort differs trial%d", trial)
		}
	}
}

// A seal owns detached input. Its producer lease must be reusable even while
// durable planning blocks; the next packet must not overwrite the sealed rows.
func TestCoreSealReturnsCopiedPacketLease(t *testing.T) {
	t.Setenv("CORE_STREAM_BYTES", "256")
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.MaxRows = 1, 1, 1
	entered, resume := make(chan struct{}), make(chan struct{})
	var calls int
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(_ int, in coreSealInput) (coreSealPlan, error) {
		calls++
		if calls == 1 {
			close(entered)
			<-resume
			if len(in.Rows) != 1 || in.Rows[0].Bits != 42 || in.Batches[0].Points[0].Bits != 42 {
				return coreSealPlan{}, fmt.Errorf("leased input overwritten")
			}
		}
		return coreSealPlan{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.AppendBatches(0, []Batch{{ID: 9, Points: []Row{{ID: 9, Bits: 42}}}}, 1, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("seal not entered")
	}
	next := make(chan error, 1)
	go func() { next <- e.AppendBatches(0, []Batch{{ID: 9, Points: []Row{{ID: 9, Bits: 99}}}}, 2, false) }()
	var nextErr error
	select {
	case nextErr = <-next:
	case <-time.After(5 * time.Second):
		nextErr = fmt.Errorf("seal retained producer lease")
	}
	close(resume)
	if err = e.Close(); err != nil {
		t.Error(err)
	}
	if nextErr != nil {
		t.Fatal(nextErr)
	}
}

func TestCoreDefaultCompactCodec(t *testing.T) {
	e, err := openCore(t.TempDir(), coreTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.cfg.RawCodec != "run1" {
		t.Fatalf("default codec %q", e.cfg.RawCodec)
	}
}
