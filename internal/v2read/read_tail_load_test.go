package v2read

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"runtime/trace"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

// TestReadTailLoad renders single names while a paced producer writes every
// name once per pass into time-sealed owners. It reports render latency, the
// stage histogram deltas and runtime pause/scheduling histograms. Set
// READ_TAIL_LOAD=1; READ_TAIL_PROFILE=dir also writes block, mutex and trace.
func TestReadTailLoad(t *testing.T) {
	if os.Getenv("READ_TAIL_LOAD") == "" {
		t.Skip("set READ_TAIL_LOAD=1")
	}
	owners := envInt("LT_OWNERS", 8)
	nNames := envInt("LT_NAMES", 200000)
	secs := envInt("LT_SECS", 20)
	rate := envInt("LT_RATE", 400000) // points/s
	readers := envInt("LT_READERS", 4)
	seal := time.Duration(envInt("LT_SEAL_MS", 2000)) * time.Millisecond
	r, err := New(owners, 8<<30, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	r.Identities = &v2core.PolicyCatalog{}
	// LT_CACHE_KB mimics the per-owner cache limit of a production budget
	// (256 MiB / 96 owners / 32 = 85 KiB); LT_BLOCK_KB sets the block cache.
	if kb := envInt("LT_CACHE_KB", 0); kb > 0 {
		for i := range r.caches {
			r.caches[i].limit = int64(kb) << 10
			r.caches[i].ranges = v2core.NewRollupReadCache(r.caches[i].limit)
		}
	}
	r.BlockCacheBytes = int64(envInt("LT_BLOCK_KB", 0)) << 10
	cfg := testConfig()
	cfg.Shards, cfg.Owners = owners, owners
	cfg.MaxRows = 4 << 20
	if s := envInt("LT_SEAL_ROWS", 0); s > 0 {
		cfg.SealMaxRows = s
	}
	if cfg.SealMaxRows == 0 {
		cfg.SealMaxRows = 2 << 20
	}
	cfg.SealInterval = seal
	// Publication cost on the owner, split by whether the sealed generation
	// moved; a pin is a Pebble snapshot handed to the reader.
	type publishStat struct{ n, ns, pins atomic.Int64 }
	var heads, seals publishStat
	var measuring atomic.Bool
	type sealedKey struct {
		number    uint64
		inventory *v2core.RollupInventory
	}
	last := make([]sealedKey, owners)
	publish := func(owner int, snap *pebble.Snapshot, pending v2core.PendingView, number uint64, inventory *v2core.RollupInventory) error {
		st := &heads
		if k := (sealedKey{number, inventory}); last[owner] != k {
			st, last[owner] = &seals, k
		}
		started := time.Now()
		err := r.PublishPending(owner, snap, pending, number, inventory)
		if measuring.Load() {
			st.n.Add(1)
			st.ns.Add(int64(time.Since(started)))
			if snap != nil {
				st.pins.Add(1)
			}
		}
		return err
	}
	e, err := v2core.Open(t.TempDir(), cfg, v2core.Hooks{Identities: r.Identities, PublishPending: publish})
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	names := make([]string, nNames)
	policies := make([][]byte, nNames)
	for i := range names {
		names[i] = fmt.Sprintf("tail.host%04d.metric.%06d", i%4096, i)
		policies[i], err = v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(names[i]), Group: "test", Config: sealrollup.Config{Method: sealrollup.Average, XFilesFactor: .5, Archives: []sealrollup.Archive{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	base := time.Now().Unix()
	var clock atomic.Int64
	clock.Store(base)
	order, cursor := uint64(0), uint64(0)
	const chunk = 4096
	pass := func(ts int64, paced bool, started time.Time, done *int64) {
		for from := 0; from < nNames; from += chunk {
			to := min(from+chunk, nNames)
			batches := make([]v2core.Batch, 0, to-from)
			for i := from; i < to; i++ {
				id := uint64(i + 1)
				order++
				batches = append(batches, v2core.Batch{ID: id, Order: order, Clock: ts, Policy: policies[i], Points: []v2core.Row{{ID: id, Time: ts, Bits: math.Float64bits(float64(i%977) + float64(ts%61)), Seq: order}}})
			}
			cursor++
			if err := e.AppendBatches(0, batches, cursor, false); err != nil {
				t.Error(err)
				return
			}
			*done += int64(to - from)
			if paced {
				if ahead := time.Duration(float64(*done)/float64(rate)*float64(time.Second)) - time.Since(started); ahead > 0 {
					time.Sleep(ahead)
				}
			}
		}
	}
	// Install every name with one synchronous seal, then measure.
	var warm int64
	pass(base, false, time.Now(), &warm)
	cursor++
	if err := e.AppendBatches(0, nil, cursor, true); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		got, err := fetchAll(r, names[len(names)-3:], base-60, base+1, base+1)
		return err == nil && len(got) == 3
	})

	if dir := os.Getenv("READ_TAIL_PROFILE"); dir != "" {
		runtime.SetBlockProfileRate(int(envInt("LT_BLOCK_NS", 10000)))
		runtime.SetMutexProfileFraction(5)
		f, err := os.Create(dir + "/trace.out")
		if err != nil {
			t.Fatal(err)
		}
		traceSecs := envInt("LT_TRACE_SECS", 4)
		go func() {
			time.Sleep(time.Duration(secs/2) * time.Second)
			if err := trace.Start(f); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(time.Duration(traceSecs) * time.Second)
			trace.Stop()
			f.Close()
		}()
		defer func() {
			for _, p := range []string{"block", "mutex"} {
				out, err := os.Create(dir + "/" + p + ".pb.gz")
				if err == nil {
					_ = pprof.Lookup(p).WriteTo(out, 0)
					out.Close()
				}
			}
		}()
	}

	if dir := os.Getenv("READ_TAIL_PROFILE"); dir != "" {
		f, err := os.Create(dir + "/cpu.pb.gz")
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatal(err)
		}
		defer func() { pprof.StopCPUProfile(); f.Close() }()
	}
	livePerOwner := func() float64 {
		var live uint64
		for _, s := range e.PartGCStats() {
			live += s.LiveParts
		}
		return float64(live) / float64(owners)
	}
	measuring.Store(true)
	before := r.StageObservations()
	rtBefore := readRuntimeHists()
	gcBefore := e.PartGCStats()
	cpuBefore := cpuSeconds()
	stop := make(chan struct{})
	var prod sync.WaitGroup
	// Sample every owner's live parts once a second for p50/max over time.
	var liveSamples []uint64
	prod.Go(func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			for _, s := range e.PartGCStats() {
				liveSamples = append(liveSamples, s.LiveParts)
			}
		}
	})
	var written int64
	prod.Go(func() {
		started := time.Now()
		for ts := base + 1; ; ts++ {
			select {
			case <-stop:
				return
			default:
			}
			clock.Store(ts)
			pass(ts, true, started, &written)
		}
	})
	lat := make([][]time.Duration, readers)
	var rd sync.WaitGroup
	for k := range readers {
		rd.Go(func() {
			rng := rand.New(rand.NewSource(int64(k)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				name := names[rng.Intn(nNames)]
				now := clock.Load()
				t0 := time.Now()
				err := r.FetchMany(context.Background(), []string{name}, now-3600, now, now, func(string, *Series) error { return nil })
				lat[k] = append(lat[k], time.Since(t0))
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Error(err)
					return
				}
				time.Sleep(time.Duration(envInt("LT_THINK_MS", 5)) * time.Millisecond)
			}
		})
	}
	time.Sleep(time.Duration(secs) * time.Second)
	close(stop)
	rd.Wait()
	prod.Wait()
	after := r.StageObservations()
	rtAfter := readRuntimeHists()
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	slices.Sort(all)
	q := func(p float64) time.Duration { return all[min(len(all)-1, int(p*float64(len(all))))] }
	cpu := cpuSeconds() - cpuBefore
	var runs, readB, writtenB uint64
	for i, s := range e.PartGCStats() {
		runs += s.MergeRuns - gcBefore[i].MergeRuns
		readB += s.MergeReadBytes - gcBefore[i].MergeReadBytes
		writtenB += s.MergeWrittenBytes - gcBefore[i].MergeWrittenBytes
	}
	slices.Sort(liveSamples)
	if n := len(liveSamples); n > 0 {
		t.Logf("live parts per owner over time p50 %d p90 %d max %d (%d samples)", liveSamples[n/2], liveSamples[n*9/10], liveSamples[n-1], n)
	}
	t.Logf("merges %d runs, read %.2f MB/s, written %.2f MB/s; process CPU %.2f cores, %.3f cores per 1M/s", runs, float64(readB)/1e6/float64(secs), float64(writtenB)/1e6/float64(secs), cpu/float64(secs), cpu/float64(secs)/(float64(written)/float64(secs)/1e6))
	t.Logf("live parts per owner at end %.1f", livePerOwner())
	for _, p := range []struct {
		kind string
		st   *publishStat
	}{{"head-only", &heads}, {"sealed", &seals}} {
		if n := p.st.n.Load(); n > 0 {
			t.Logf("publish %-9s count %6d (%.1f/s) mean %7.1f us, reader pins %.1f/s", p.kind, n, float64(n)/float64(secs), float64(p.st.ns.Load())/float64(n)/1000, float64(p.st.pins.Load())/float64(secs))
		}
	}
	t.Logf("writes %.0f points/s (target %d), renders %d, p50 %v p90 %v p99 %v max %v", float64(written)/float64(secs), rate, len(all), q(.5), q(.9), q(.99), all[len(all)-1])
	for i := range after {
		dc := after[i].Count - before[i].Count
		if dc == 0 {
			continue
		}
		ds := after[i].Sum - before[i].Sum
		var hist []string
		prev := uint64(0)
		for _, b := range stageBounds {
			c := after[i].Buckets[b] - before[i].Buckets[b]
			hist = append(hist, fmt.Sprintf("<=%gms:%d", b*1000, c-prev))
			prev = c
		}
		hist = append(hist, fmt.Sprintf(">1s:%d", dc-prev))
		t.Logf("stage %-12s count %7d mean %8.3f ms total %8.3f s  %v", after[i].Name, dc, ds/float64(dc)*1000, ds, hist)
	}
	for name, a := range rtAfter {
		t.Logf("runtime %s: %s", name, histDelta(rtBefore[name], a))
	}
}

func cpuSeconds() float64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		return 0
	}
	return time.Duration(u.Utime.Nano() + u.Stime.Nano()).Seconds()
}

var runtimeHistNames = []string{"/sched/pauses/total/gc:seconds", "/sched/latencies:seconds"}

func readRuntimeHists() map[string]*metrics.Float64Histogram {
	s := make([]metrics.Sample, len(runtimeHistNames))
	for i, n := range runtimeHistNames {
		s[i].Name = n
	}
	metrics.Read(s)
	out := map[string]*metrics.Float64Histogram{}
	for _, x := range s {
		if x.Value.Kind() == metrics.KindFloat64Histogram {
			h := x.Value.Float64Histogram()
			out[x.Name] = &metrics.Float64Histogram{Counts: slices.Clone(h.Counts), Buckets: h.Buckets}
		}
	}
	return out
}

// histDelta reports count, p50, p99, p99.9 and max of the interval.
func histDelta(a, b *metrics.Float64Histogram) string {
	counts := make([]uint64, len(b.Counts))
	var total uint64
	for i := range counts {
		counts[i] = b.Counts[i]
		if a != nil {
			counts[i] -= a.Counts[i]
		}
		total += counts[i]
	}
	if total == 0 {
		return "none"
	}
	at := func(p float64) float64 {
		want := uint64(math.Ceil(p * float64(total)))
		var c uint64
		for i, n := range counts {
			c += n
			if c >= want {
				return b.Buckets[i+1]
			}
		}
		return b.Buckets[len(b.Buckets)-1]
	}
	maxv := 0.0
	for i, n := range counts {
		if n > 0 {
			maxv = b.Buckets[i+1]
		}
	}
	return fmt.Sprintf("n=%d p50<=%.3gms p99<=%.3gms p99.9<=%.3gms max<=%.3gms", total, at(.5)*1000, at(.99)*1000, at(.999)*1000, maxv*1000)
}
