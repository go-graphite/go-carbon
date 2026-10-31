package chunkstore

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func sampleCompactionRates(c *compactionController, dt time.Duration, ingress, output uint64, debt uint64) {
	time.Sleep(dt)
	s := compactionSample{ingress: c.previous.ingress + ingress*uint64(dt/time.Second),
		output: c.previous.output + output*uint64(dt/time.Second), debt: debt}
	c.sample(time.Now(), s)
}

func learnedCompactionController() *compactionController {
	c := newCompactionController(newCompactionPacer(0), -1, time.Now())
	for range 60 {
		sampleCompactionRates(c, time.Minute, 20<<20, 60<<20, 1<<30)
	}
	return c
}

func TestCompactionControllerLearnsIngress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCompactionController(newCompactionPacer(0), -1, time.Now())
		if st := c.stats(); st.rate != 0 || st.bypass != "warmup" || st.target != compactionPacingFloor {
			t.Fatalf("new controller: %+v", st)
		}
		for range 60 {
			sampleCompactionRates(c, time.Minute, 20<<20, 60<<20, 1<<30)
		}
		st := c.stats()
		if st.bypass != "" || st.rate < 120<<20 || st.rate > 121<<20 {
			t.Fatalf("learned 20 MiB/s ingress, 3x WA: %+v", st)
		}
		baseline := st.rate
		// More ingress raises the model's budget before output catches up.
		for range 30 {
			sampleCompactionRates(c, time.Minute, 40<<20, 60<<20, 1<<30)
		}
		if st := c.stats(); st.rate <= baseline || st.bypass != "" {
			t.Fatalf("ingress did not raise budget before output: %+v", st)
		}
		// An idle store retains headroom for delayed work, then reaches its floor.
		sampleCompactionRates(c, 24*time.Hour, 0, 0, 0)
		if st := c.stats(); st.rate != compactionPacingFloor || st.bypass != "" {
			t.Fatalf("idle floor: %+v", st)
		}
	})
}

func TestCompactionControllerKeepsPacingOnDebt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := learnedCompactionController()
		c.setDebtBypass(false)
		sampleCompactionRates(c, time.Minute, 0, 60<<20, 3<<30)
		paced := c.stats().rate
		sampleCompactionRates(c, time.Minute, 0, 60<<20, 10<<30)
		if st := c.stats(); st.rate == 0 || st.bypass != "" || st.bypasses["debt"] != 0 {
			t.Fatalf("debt stopped pacing with the bypass off: %+v", st)
		}
		if st := c.stats(); st.rate <= paced {
			t.Fatalf("debt growth did not raise the budget: %d <= %d", st.rate, paced)
		}
		c.setDebtBypass(true)
		if st := c.stats(); st.rate != 0 || st.bypass != "debt" {
			t.Fatalf("re-enabled bypass did not apply at once: %+v", st)
		}
	})
}

func TestCompactionControllerDebtWave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := learnedCompactionController()
		baseline := c.stats().target
		// A delete wave adds debt without adding SST ingress.
		sampleCompactionRates(c, time.Minute, 0, 60<<20, 3<<30)
		if st := c.stats(); st.rate <= baseline || st.bypass != "" {
			t.Fatalf("debt growth did not add catch-up budget: %+v", st)
		}
		sampleCompactionRates(c, time.Minute, 0, 60<<20, 10<<30)
		if st := c.stats(); st.rate != 0 || st.bypass != "debt" || st.bypasses["debt"] != 1 {
			t.Fatalf("delete wave was paced: %+v", st)
		}
		for range 180 {
			sampleCompactionRates(c, time.Minute, 0, 60<<20, 10<<30)
		}
		if st := c.stats(); st.rate != 0 || st.bypasses["debt"] != 1 {
			t.Fatalf("learned dangerous debt as normal: %+v", st)
		}
		sampleCompactionRates(c, time.Minute, 0, 60<<20, 3<<30)
		if c.stats().bypass != "debt" {
			t.Fatal("debt alarm lacks recovery hysteresis")
		}
		sampleCompactionRates(c, time.Minute, 0, 60<<20, 1<<30)
		if st := c.stats(); st.rate == 0 || st.bypass != "" {
			t.Fatalf("did not resume after debt cleared: %+v", st)
		}
	})
}

func TestCompactionControllerBypassesWakeWriters(t *testing.T) {
	for _, reason := range []string{"l0", "memtable", "stall", "conversion", "shutdown"} {
		t.Run(reason, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := learnedCompactionController()
				done := make(chan bool, 1)
				go func() { _, paced := c.pacer.take(64 << 10); done <- paced }()
				synctest.Wait()
				start := time.Now()
				switch reason {
				case "l0", "memtable":
					s := c.previous
					if reason == "l0" {
						s.l0 = c.l0Limit
					} else {
						s.memtablePressure = true
					}
					c.sample(time.Now().Add(time.Nanosecond), s)
				case "stall":
					c.stall(true)
				case "conversion":
					c.setConversionSource(func() (int64, uint64) { return 1, 0 })
				case "shutdown":
					c.stop()
				}
				if paced := <-done; paced || time.Since(start) != 0 {
					t.Fatal("bypass did not immediately release writer")
				}
				st := c.stats()
				if st.rate != 0 || st.target == 0 || st.bypass != reason || st.bypasses[reason] != 1 {
					t.Fatalf("bypass %s: %+v", reason, st)
				}
				st.bypasses[reason] = 99
				if c.stats().bypasses[reason] != 1 {
					t.Fatal("stats exposed mutable controller counters")
				}
			})
		})
	}
}

func TestCompactionControllerStallCooldownAndReload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := learnedCompactionController()
		c.stall(true)
		sampleCompactionRates(c, 10*time.Minute, 20<<20, 60<<20, 1<<30)
		if c.stats().bypass != "stall" {
			t.Fatal("active stall aged out")
		}
		c.stall(false)
		c.setMode(12345)
		if st := c.stats(); st.rate != 12345 || st.auto || st.bypass != "" {
			t.Fatalf("fixed mode changed semantics: %+v", st)
		}
		c.setMode(-1)
		if c.stats().bypass != "stall" {
			t.Fatal("reload forgot recent stall")
		}
		sampleCompactionRates(c, compactionStallHold-time.Second, 20<<20, 60<<20, 1<<30)
		if c.stats().bypass != "stall" {
			t.Fatal("ended stall aged out early")
		}
		sampleCompactionRates(c, time.Second, 20<<20, 60<<20, 1<<30)
		if c.stats().bypass != "" {
			t.Fatal("stall cooldown did not expire")
		}
		c.setMode(0)
		if st := c.stats(); st.target != 0 || st.rate != 0 || st.auto {
			t.Fatalf("off mode: %+v", st)
		}
		c.setMode(-1)
		if c.stats().bypass != "" {
			t.Fatal("reload discarded learned history")
		}
		c.setConversionSource(func() (int64, uint64) { return 1, 0 })
		c.stop()
		c.setConversionSource(nil)
		c.setMode(12345)
		if st := c.stats(); st.rate != 0 || st.bypass != "shutdown" {
			t.Fatalf("reload or conversion completion rearmed shutdown: %+v", st)
		}
	})
}

// This queue model spans four hourly cycles without large fixtures or sleeps.
// The bucket's byte-for-byte output and timing have separate fake-time tests.
func TestCompactionControllerHourlyPulse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCompactionController(newCompactionPacer(0), -1, time.Now())
		var ingress, output, queued, peak, rawPeak, total, maxDebt uint64
		for minute := range 4 * 60 {
			in := uint64(10 << 20)
			if minute%60 >= 14 && minute%60 < 17 {
				in = 36 << 20
			}
			ingress += in * 60
			// Compaction follows ingress five minutes later.
			raw := uint64(30 << 20)
			if minute%60 >= 19 && minute%60 < 22 {
				raw = 108 << 20
			}
			queued += raw * 60
			st := c.stats()
			written := queued
			if st.rate > 0 {
				written = min(written, uint64(st.rate)*60)
			}
			queued -= written
			output += written
			debt := (8 << 30) + queued
			time.Sleep(time.Minute)
			c.sample(time.Now(), compactionSample{ingress: ingress, output: output, debt: debt})
			if minute >= 60 {
				if c.stats().bypass != "" {
					t.Fatalf("normal hourly cycle triggered bypass: %+v", c.stats())
				}
				peak, rawPeak, total, maxDebt = max(peak, written/60), max(rawPeak, raw), total+written, max(maxDebt, debt)
			}
		}
		if queued != 0 || peak >= rawPeak*4/5 {
			t.Fatalf("hourly pulse not smoothed/drained: peak=%d raw=%d remaining=%d", peak, rawPeak, queued)
		}
		t.Logf("local queue model: unpaced peak %.2f MiB/s, auto peak %.2f MiB/s, mean %.2f MiB/s, max debt %.2f GiB; no remaining work or pressure bypass",
			float64(rawPeak)/(1<<20), float64(peak)/(1<<20), float64(total)/(180*60*(1<<20)), float64(maxDebt)/(1<<30))
	})
}

func TestCompactionControllerStoreSignals(t *testing.T) {
	for _, threshold := range []int{0, 4} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			db, err := Open(t.TempDir(), Options{fs: vfs.NewMem(), CompactionRate: -1, L0StopWritesThreshold: threshold})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			want := int64(6)
			if threshold == 4 {
				want = 2
			}
			if db.compactionControl.l0Limit != want || !db.Stats().CompactionPacerAuto {
				t.Fatal("auto was not wired with headroom below stop-writes")
			}
			for _, reason := range []string{"memtable count limit reached", "L0 file count limit exceeded"} {
				db.writeStallBegin(pebble.WriteStallBeginInfo{Reason: reason})
				if db.Stats().CompactionPacerBypass != "stall" {
					t.Fatalf("stall event %q did not bypass", reason)
				}
				db.writeStallEnd()
			}
			db.SetCompactionPacingConversionSource(func() (int64, uint64) { return 1, 0 })
			if db.Stats().CompactionPacerBypass != "conversion" {
				t.Fatal("conversion state did not bypass")
			}
		})
	}
}

func TestCompactionControllerConversionActivity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := learnedCompactionController()
		var active atomic.Int64
		var completed atomic.Uint64
		completed.Store(100)
		// Keep the source registered throughout: the migration is still running
		// even when it has only historical completions and no work in flight.
		c.setConversionSource(func() (int64, uint64) { return active.Load(), completed.Load() })
		sampleCompactionRates(c, compactionConversionHold+time.Second, 20<<20, 60<<20, 1<<30)
		if st := c.stats(); st.rate == 0 || st.bypass != "" || st.bypasses["conversion"] != 0 {
			t.Fatalf("idle running migration bypassed pacing: %+v", st)
		}
		active.Add(1)
		c.setMode(-1)
		sampleCompactionRates(c, 2*compactionConversionHold, 20<<20, 60<<20, 1<<30)
		if st := c.stats(); st.rate != 0 || st.bypass != "conversion" {
			t.Fatalf("active conversion aged out: %+v", st)
		}
		completed.Add(1)
		active.Add(-1)
		c.setMode(-1)
		sampleCompactionRates(c, compactionConversionHold-time.Second, 20<<20, 60<<20, 1<<30)
		if c.stats().bypass != "conversion" {
			t.Fatal("conversion hold ended early")
		}
		// A short conversion can begin and finish between observations. Its
		// completed counter extends the hold without briefly rearming pacing.
		completed.Add(1)
		c.setMode(-1)
		sampleCompactionRates(c, time.Second, 20<<20, 60<<20, 1<<30)
		if c.stats().bypass != "conversion" {
			t.Fatal("late completion did not extend hold")
		}
		sampleCompactionRates(c, compactionConversionHold-time.Second, 20<<20, 60<<20, 1<<30)
		if st := c.stats(); st.rate == 0 || st.bypass != "" || st.bypasses["conversion"] != 1 {
			t.Fatalf("idle running migration did not resume pacing without flapping: %+v", st)
		}
		active.Add(1)
		c.setMode(12345)
		if st := c.stats(); st.rate != 12345 || st.bypass != "" {
			t.Fatalf("active conversion overrode fixed budget: %+v", st)
		}
		c.setMode(-1)
		if st := c.stats(); st.rate != 0 || st.bypass != "conversion" {
			t.Fatalf("auto reload forgot active conversion: %+v", st)
		}
	})
}

func TestCompactionControllerSamplesStoreIngress(t *testing.T) {
	fs := vfs.NewMem()
	db, err := Open("store", Options{fs: fs, CompactionRate: -1, L0CompactionThreshold: 10, L0StopWritesThreshold: 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for generation := range 2 {
		if err := db.db.Set([]byte("p.block"), bytes.Repeat([]byte{byte(generation)}, 1024), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	before := db.compactionSample()
	if before.ingress == 0 || before.ingress != db.Stats().FlushWriteBytes || before.output != 0 {
		t.Fatalf("flush ingress counted incorrectly: %+v", before)
	}
	f, err := fs.Create("stage.sst")
	if err != nil {
		t.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Comparer: storeComparer, MergerName: db.pebbleOptions.Merger.Name})
	if err := w.Set([]byte("z.ingested"), []byte("ingested value")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	meta, err := w.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.db.Ingest([]string{"stage.sst"}); err != nil {
		t.Fatal(err)
	}
	ingested := db.compactionSample()
	if ingested.ingress-before.ingress != meta.Size {
		t.Fatalf("ingest counted more than once or omitted: %d, want %d", ingested.ingress-before.ingress, meta.Size)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after := db.compactionSample()
	st := db.Stats()
	if after.ingress != ingested.ingress || after.output == 0 || after.output != st.CompactionWriteBytes || st.CompactionPacerBytes != 0 {
		t.Fatalf("unpaced output lost or counted as ingress: before=%+v after=%+v compacted=%d paced=%d", ingested, after, st.CompactionWriteBytes, st.CompactionPacerBytes)
	}
}

func TestCompactionControllerHeadroomScalesBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := learnedCompactionController()
		// 60 MiB/s learned output dominates 20 MiB/s ingress at the smoothed
		// amplification, and constant debt adds no trend.
		for _, tc := range []struct {
			headroom float64
			want     int64
		}{{2, 120 << 20}, {1.5, 90 << 20}, {1, 60 << 20}, {4, 240 << 20}} {
			c.setHeadroom(tc.headroom)
			st := c.stats()
			if st.bypass != "" || st.headroom != tc.headroom || st.rate < tc.want || st.rate > tc.want+1<<20 {
				t.Fatalf("headroom %v: want rate ~%d: %+v", tc.headroom, tc.want, st)
			}
		}
		// Fixed budgets ignore the headroom.
		c.setMode(12345)
		if st := c.stats(); st.rate != 12345 {
			t.Fatalf("headroom changed a fixed budget: %+v", st)
		}
	})
}

func TestCompactionControllerZeroWarmup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCompactionController(newCompactionPacer(0), -1, time.Now())
		c.setWarmup(0)
		if st := c.stats(); st.rate != 0 || st.bypass != "warmup" || st.warmup != 0 {
			t.Fatalf("zero warmup paced before any sample: %+v", st)
		}
		sampleCompactionRates(c, time.Second, 20<<20, 60<<20, 1<<30)
		// The bias-corrected EWMA reflects the first second at once.
		if st := c.stats(); st.bypass != "" || st.rate < 120<<20 || st.rate > 125<<20 {
			t.Fatalf("zero warmup did not pace after the first sample: %+v", st)
		}
	})
}

func TestCompactionControllerWarmupReloadKeepsStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		c := newCompactionController(newCompactionPacer(0), -1, started)
		for range 10 {
			sampleCompactionRates(c, time.Minute, 20<<20, 60<<20, 1<<30)
		}
		if st := c.stats(); st.bypass != "warmup" || st.warmup != time.Hour {
			t.Fatalf("default warmup: %+v", st)
		}
		// Ten minutes in, a five-minute warm-up has already elapsed.
		c.setWarmup(5 * time.Minute)
		if st := c.stats(); st.bypass != "" || st.rate <= compactionPacingFloor || !c.started.Equal(started) {
			t.Fatalf("shortened warmup did not pace at once: %+v", st)
		}
		c.setWarmup(time.Hour)
		if st := c.stats(); st.bypass != "warmup" || !c.started.Equal(started) {
			t.Fatalf("lengthened warmup did not resume the bypass: %+v", st)
		}
		c.setWarmup(20 * time.Minute)
		sampleCompactionRates(c, 10*time.Minute-time.Second, 20<<20, 60<<20, 1<<30)
		if c.stats().bypass != "warmup" {
			t.Fatal("warmup ended early")
		}
		sampleCompactionRates(c, time.Second, 20<<20, 60<<20, 1<<30)
		if st := c.stats(); st.bypass != "" || !c.started.Equal(started) {
			t.Fatalf("warmup measured from reload, not start: %+v", st)
		}
	})
}
