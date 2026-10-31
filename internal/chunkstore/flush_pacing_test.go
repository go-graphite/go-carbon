package chunkstore

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func sampleFlushRates(c *compactionController, dt time.Duration, ingress uint64, memtables int64) {
	time.Sleep(dt)
	s := compactionSample{ingress: c.previous.ingress + ingress*uint64(dt/time.Second),
		output: c.previous.output, debt: c.previous.debt, memtables: memtables}
	c.sample(time.Now(), s)
}

func TestFlushPacingControllerAuto(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newPacingController(newCompactionPacer(0), newCompactionPacer(0), 0, -1, time.Now())
		if st := c.stats().flush; st.rate != 0 || st.bypass != "warmup" || st.target != flushPacingFloor || !st.auto {
			t.Fatalf("new controller: %+v", st)
		}
		// No flush observed yet: a store that has not flushed stays unpaced.
		sampleFlushRates(c, time.Minute, 0, 1)
		if st := c.stats().flush; st.rate != 0 || st.bypass != "warmup" {
			t.Fatalf("paced before observing ingress: %+v", st)
		}
		for range 60 {
			sampleFlushRates(c, time.Minute, 2<<20, 1)
		}
		st := c.stats().flush
		if st.bypass != "" || st.rate < 8<<20*98/100 || st.rate > 8<<20 || st.target != st.rate {
			t.Fatalf("learned 2 MiB/s ingress, want 4x: %+v", st)
		}
		// Compaction pacing stays off: the modes are independent.
		if cs := c.stats(); cs.rate != 0 || cs.bypass != "" || cs.auto {
			t.Fatalf("flush mode changed compaction pacing: %+v", cs)
		}
		// Little ingress keeps the floor, not a crawl.
		for range 600 {
			sampleFlushRates(c, time.Minute, 64<<10, 1)
		}
		if st := c.stats().flush; st.rate != flushPacingFloor || st.bypass != "" {
			t.Fatalf("low ingress floor: %+v", st)
		}
		c.setFlushMode(0)
		if st := c.stats().flush; st.rate != 0 || st.bypass != "" || st.auto {
			t.Fatalf("disabled: %+v", st)
		}
	})
}

func TestFlushPacingControllerBypasses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, mode := range []int64{-1, 3 << 20} {
			c := newPacingController(newCompactionPacer(0), newCompactionPacer(0), 0, mode, time.Now())
			for range 60 {
				sampleFlushRates(c, time.Minute, 1<<20, 2)
			}
			paced := c.stats().flush.rate
			if paced == 0 || mode > 0 && paced != mode {
				t.Fatalf("mode %d: not pacing: %+v", mode, c.stats().flush)
			}
			expect := func(reason string) {
				t.Helper()
				st := c.stats().flush
				if st.bypass != reason || reason != "" && st.rate != 0 || reason == "" && st.rate != paced {
					t.Fatalf("mode %d: want bypass %q, got %+v", mode, reason, st)
				}
			}
			// A caller waiting on a flush gets device speed until it returns.
			c.urgentFlush(1)
			c.urgentFlush(1)
			expect("urgent")
			c.urgentFlush(-1)
			expect("urgent")
			c.urgentFlush(-1)
			expect("")
			// A second queued memtable means the flush is falling behind.
			sampleFlushRates(c, time.Second, 1<<20, 3)
			expect("queue")
			sampleFlushRates(c, time.Second, 1<<20, 2)
			expect("")
			time.Sleep(time.Second)
			c.sample(time.Now(), compactionSample{ingress: c.previous.ingress, memtables: 1, memtablePressure: true})
			expect("memtable")
			sampleFlushRates(c, time.Second, 1<<20, 1)
			expect("")
			// Stalls bypass and hold for the cooldown after they end.
			c.stall(true)
			expect("stall")
			c.stall(false)
			sampleFlushRates(c, compactionStallHold-time.Second, 1<<20, 1)
			expect("stall")
			sampleFlushRates(c, 2*time.Second, 1<<20, 1)
			expect("")
			var active int64 = 1
			c.setConversionSource(func() (int64, uint64) { return active, 0 })
			expect("conversion")
			active = 0
			sampleFlushRates(c, compactionConversionHold+time.Second, 1<<20, 1)
			expect("")
			st := c.stats().flush
			for _, reason := range []string{"urgent", "queue", "memtable", "stall", "conversion"} {
				if st.bypasses[reason] != 1 {
					t.Fatalf("mode %d: bypass counts %v", mode, st.bypasses)
				}
			}
			c.stop()
			expect("shutdown")
			c.urgentFlush(1)
			expect("shutdown")
		}
		// Fixed budgets need no learned ingress.
		c := newPacingController(newCompactionPacer(0), newCompactionPacer(0), 0, 5<<20, time.Now())
		if st := c.stats().flush; st.rate != 5<<20 || st.bypass != "" || st.auto {
			t.Fatalf("fixed: %+v", st)
		}
	})
}

func TestCompactionPacingFSPacesFlushes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs := newCompactionPacingFS(vfs.NewMem(), 0)
		fs.flush.setRate(10000)
		payload := bytes.Repeat([]byte("data"), 250)
		for _, c := range []struct{ name, reason string }{
			{"000001.log", ""}, {"000003.sst", "flushing"}, {"000004.sst", "compacting"},
		} {
			f, err := fs.Create(c.name)
			if err != nil {
				t.Fatal(err)
			}
			fs.tableCreated(pebble.TableCreateInfo{Path: c.name, Reason: c.reason})
			start := time.Now()
			if n, err := f.Write(bytes.Clone(payload)); err != nil || n != len(payload) {
				t.Fatalf("%s write = %d, %v", c.name, n, err)
			}
			elapsed := time.Since(start)
			if c.reason == "flushing" {
				// Enabled from zero: no burst credit, 1000 bytes at 10000 B/s.
				if elapsed < 99*time.Millisecond || elapsed > 101*time.Millisecond {
					t.Fatalf("flush took %v", elapsed)
				}
			} else if elapsed != 0 {
				t.Fatalf("%s was paced for %v", c.name, elapsed)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if fs.flush.bytes.Load() != uint64(len(payload)) || fs.flush.wait.Load() == 0 || fs.pacer.bytes.Load() != 0 {
			t.Fatalf("flush paced=%d wait=%d, compaction paced=%d", fs.flush.bytes.Load(), fs.flush.wait.Load(), fs.pacer.bytes.Load())
		}
		if fs.output.Load() != uint64(len(payload)) {
			t.Fatalf("compaction output %d counted flush bytes", fs.output.Load())
		}
	})
}

func TestFlushRateRejectsInvalid(t *testing.T) {
	t.Parallel()
	if db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), FlushRate: -2}); err == nil {
		_ = db.Close()
		t.Fatal("negative flush rate accepted")
	}
	// Without exact blocks Pebble queues one memtable behind a flush, so a
	// paced flush would stall writers: the rate stays off.
	db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), FlushRate: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetFlushRate(-2); err == nil {
		t.Fatal("negative reload accepted")
	}
	if err := db.SetFlushRate(-1); err != nil {
		t.Fatal(err)
	}
	if st := db.Stats(); st.FlushRate != 0 || st.FlushPacerAuto || st.FlushPacerTargetRate != 0 {
		t.Fatalf("legacy store paced flushes: %+v", st)
	}
}

// writeFlushPayload writes n incompressible 4 KiB values. Pebble's first
// memtable holds 256 KiB, so n <= 48 keeps one memtable: a second queued one
// would release pacing.
func writeFlushPayload(t *testing.T, db *Store, generation, n int) {
	t.Helper()
	value := make([]byte, 4<<10)
	for i := range n {
		_, _ = rand.Read(value)
		if err := db.db.Set(fmt.Appendf(nil, "p%06d", i), append([]byte{byte(generation)}, value...), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFlushPacingStore(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, FlushRate: 160 << 10,
		L0CompactionThreshold: 10, L0StopWritesThreshold: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if st := db.Stats(); st.FlushRate != 160<<10 || st.FlushPacerBypass != "" {
		t.Fatalf("fixed flush rate not applied: %+v", st)
	}
	// 40 x 4 KiB = 160 KiB of flush output: about one second at the budget.
	writeFlushPayload(t, db, 0, 40)
	start := time.Now()
	done, err := db.AsyncFlush()
	if err != nil {
		t.Fatal(err)
	}
	<-done
	elapsed := time.Since(start)
	st := db.Stats()
	if st.FlushPacerBytes == 0 || st.FlushPacerBytes > st.FlushWriteBytes || st.FlushPacerWaitNanos == 0 || elapsed < 500*time.Millisecond {
		t.Fatalf("background flush was not paced in %v: paced=%d flushed=%d wait=%d", elapsed, st.FlushPacerBytes, st.FlushWriteBytes, st.FlushPacerWaitNanos)
	}
	if st.CompactionPacerBytes != 0 {
		t.Fatal("flush output counted as paced compaction")
	}
	// A caller waiting on Flush gets an unpaced flush.
	paced := st.FlushPacerBytes
	writeFlushPayload(t, db, 1, 40)
	start = time.Now()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	elapsed = time.Since(start)
	if st := db.Stats(); st.FlushPacerBytes != paced || st.FlushRate != 160<<10 || st.FlushPacerBypasses["urgent"] != 1 || elapsed > 500*time.Millisecond {
		t.Fatalf("waited flush was paced (%v): %+v", elapsed, st)
	}
	// Reload turns it off.
	if err := db.SetFlushRate(0); err != nil {
		t.Fatal(err)
	}
	if st := db.Stats(); st.FlushRate != 0 || st.FlushPacerAuto {
		t.Fatalf("flush pacing not disabled: %+v", st)
	}
}

func TestFlushPacingUrgentFlushReleasesPacedFlush(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, FlushRate: 16 << 10,
		L0CompactionThreshold: 10, L0StopWritesThreshold: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// 160 KiB at 16 KiB/s would take ten seconds.
	writeFlushPayload(t, db, 0, 40)
	background, err := db.AsyncFlush()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for db.Stats().FlushPacerWaitNanos == 0 {
		if time.Now().After(deadline) {
			t.Fatal("background flush never waited for tokens")
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("waited flush stayed behind a paced flush for %v", elapsed)
	}
	select {
	case <-background:
	default:
		t.Fatal("background flush still running after the waited flush")
	}
	for i := range 40 {
		got, closer, err := db.db.Get(fmt.Appendf(nil, "p%06d", i))
		if err != nil {
			t.Fatal(err)
		}
		ok := len(got) == 1+4<<10
		_ = closer.Close()
		if !ok {
			t.Fatal("paced flush changed a value")
		}
	}
}

func TestFlushPacingStartsAfterOpen(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	opts := Options{fs: fs, ExactBlocks: true, FlushRate: 16 << 10, L0CompactionThreshold: 10, L0StopWritesThreshold: 20}
	db, err := Open("store", opts)
	if err != nil {
		t.Fatal(err)
	}
	// Left in the WAL by Close, so the next Open replays and flushes it.
	writeFlushPayload(t, db, 0, 40)
	if err := db.db.LogData(nil, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	db, err = Open("store", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// 160 KiB at 16 KiB/s would take ten seconds.
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("WAL recovery flush was paced: open took %v", elapsed)
	}
	st := db.Stats()
	if st.FlushPacerBytes != 0 || st.FlushRate != 16<<10 {
		t.Fatalf("recovery paced or rate not applied after open: %+v", st)
	}
	got, closer, err := db.db.Get([]byte("p000039"))
	if err != nil {
		t.Fatal(err)
	}
	_ = closer.Close()
	if len(got) != 1+4<<10 {
		t.Fatal("recovered value changed")
	}
}

func TestFlushPacingColdPackDowngrade(t *testing.T) {
	t.Parallel()
	db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true,
		Continuation: testColdCodec{}, FlushRate: 16 << 10, ColdPack: ColdPackOptions{Mode: "exact"}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.stopColdPack()
	if err := db.ConfigureColdPack(ColdPackOptions{Mode: "unpack"}); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if done, err := db.ColdPackStep(ctx); err != nil || !done {
		t.Fatalf("empty unpack sweep: done=%v err=%v", done, err)
	}
	// The format downgrade waits for a flush before publishing its marker.
	// This payload would take ten seconds at the configured flush rate.
	writeFlushPayload(t, db, 0, 40)
	done := make(chan error, 1)
	go func() { done <- db.DowngradeColdPack(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		_ = db.SetFlushRate(0)
		<-done
		t.Fatal("cold-pack downgrade waited for flush pacing")
	}
	if st := db.Stats(); st.FlushPacerBytes != 0 || st.FlushPacerBypasses["urgent"] != 1 || db.TransferFormat() != 7 {
		t.Fatalf("downgrade flush was paced or format was not updated: %+v", st)
	}
}
