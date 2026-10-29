// Package stagetime accounts where persister updates spend their time.
//
// Every update's total wall time feeds a cumulative counter, a histogram and a
// per-scrape maximum. One update in SampleEvery also records its stages:
// route, lock wait, load, tail read, decode, encode, out-of-order merge,
// commit and original-file write. Sampled stage counters are scaled by
// SampleEvery, so rates estimate the full workload while the clock reads stay
// off most updates. Counters are sums across workers: dividing a rate by the
// worker count estimates the wall time a stage adds per worker.
//
// A sampled update also labels its goroutine with the running stage
// ("persister_stage"), so CPU profiles taken on demand break down by stage.
package stagetime

import (
	"context"
	"math/bits"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"
)

// Stage identifies one part of a persister update.
type Stage uint8

const (
	// Total is the whole update, timed for every update.
	Total Stage = iota
	// Route covers metadata lookups that choose the backend and metric state.
	Route
	// LockWait is time waiting for the per-metric write locks.
	LockWait
	// Load reads the metric's virtual file names and header.
	Load
	// TailRead reads and decodes stored block groups the update extends.
	TailRead
	// Decode is the whisper update logic itself, excluding nested reads and
	// encodes: merging points into archives and propagating rollups.
	Decode
	// Encode serializes changed blocks and headers into the write batch.
	Encode
	// OOOMerge folds out-of-order sidecar points into the archives.
	OOOMerge
	// Commit applies the write batch to the store.
	Commit
	// OriginalWrite writes a metric still owned by its original whisper file.
	OriginalWrite
	// NumStages is the number of stages.
	NumStages
)

var stageNames = [NumStages]string{"total", "route", "lock_wait", "load", "tail_read", "decode", "encode", "ooo_merge", "commit", "original_write"}

func (s Stage) String() string { return stageNames[s] }

// SampleEvery is the fraction of updates (one in SampleEvery) whose stages are
// timed and labelled.
const SampleEvery = 8

type counter struct {
	nanos, calls atomic.Uint64
	_            [48]byte // one cache line per stage under concurrent workers
}

var counters [NumStages]counter

func add(s Stage, d time.Duration, calls uint64) {
	if d > 0 {
		counters[s].nanos.Add(uint64(d))
	}
	counters[s].calls.Add(calls)
}

// Sample is a stage's cumulative work in seconds and its number of calls.
type Sample struct {
	Stage   Stage
	Seconds float64
	Calls   uint64
}

// Snapshot returns the cumulative counters of every stage.
func Snapshot() [NumStages]Sample {
	var out [NumStages]Sample
	for s := range NumStages {
		out[s] = Sample{Stage: s, Seconds: float64(counters[s].nanos.Load()) / 1e9, Calls: counters[s].calls.Load()}
	}
	return out
}

// UpdateBounds are the upper bounds, in seconds, of the update-duration
// histogram buckets: 64µs doubling to about 8.4s.
var UpdateBounds = func() []float64 {
	out := make([]float64, 18)
	for i := range out {
		out[i] = float64(int64(64)<<i) / 1e6
	}
	return out
}()

var (
	updateBuckets [19]atomic.Uint64 // last bucket is +Inf
	updateMax     atomic.Int64
	sampleTick    atomic.Uint64
)

func bucketOf(d time.Duration) int {
	us := uint64(d / time.Microsecond)
	if us <= 64 {
		return 0
	}
	i := bits.Len64((us - 1) >> 6) // 64<<i >= us
	return min(i, len(updateBuckets)-1)
}

// ObserveUpdate records one update's total wall time.
func ObserveUpdate(d time.Duration) {
	observeBenchmarkUpdate(d)
	add(Total, d, 1)
	updateBuckets[bucketOf(d)].Add(1)
	for cur := updateMax.Load(); int64(d) > cur && !updateMax.CompareAndSwap(cur, int64(d)); cur = updateMax.Load() {
	}
}

// UpdateHistogram returns cumulative bucket counts by upper bound, the total
// count and the sum in seconds.
func UpdateHistogram() (buckets map[float64]uint64, count uint64, sum float64) {
	buckets = make(map[float64]uint64, len(UpdateBounds))
	var cumulative uint64
	for i, bound := range UpdateBounds {
		cumulative += updateBuckets[i].Load()
		buckets[bound] = cumulative
	}
	count = cumulative + updateBuckets[len(UpdateBounds)].Load()
	return buckets, count, float64(counters[Total].nanos.Load()) / 1e9
}

// TakeUpdateMax returns the slowest update since the previous call and resets
// it, so each scrape reports the maximum of its own interval.
func TakeUpdateMax() time.Duration { return time.Duration(updateMax.Swap(0)) }

// Update holds the stage timing of one sampled update. It belongs to the
// goroutine running the update.
type Update struct {
	base     context.Context
	stage    Stage
	started  time.Time
	children time.Duration // nested time inside the running stage
	frames   [2]frame      // decode > encode > tail read at most
	depth    int
}

type frame struct {
	stage    Stage
	start    time.Time
	children time.Duration
}

type contextKey struct{}

// Start begins an update. One update in SampleEvery returns a context
// carrying its stage timer; others return ctx and a nil timer, on which every
// method is a no-op.
func Start(ctx context.Context) (context.Context, *Update) {
	if sampleTick.Add(1)%SampleEvery != 0 {
		return ctx, nil
	}
	u := &Update{stage: NumStages, base: ctx}
	setLabel(Total)
	return context.WithValue(ctx, contextKey{}, u), u
}

// FromContext returns the update timer carried by ctx, or nil.
func FromContext(ctx context.Context) *Update {
	u, _ := ctx.Value(contextKey{}).(*Update)
	return u
}

// Enter ends the running stage, if any, and starts s.
func (u *Update) Enter(s Stage) {
	if u == nil {
		return
	}
	now := time.Now()
	u.leave(now)
	u.stage, u.started = s, now
	setLabel(s)
}

// Leave ends the running stage; later work is in the update's total only.
func (u *Update) Leave() {
	if u == nil {
		return
	}
	u.leave(time.Now())
	u.stage = NumStages
	setLabel(Total)
}

func (u *Update) leave(now time.Time) {
	if u.stage >= NumStages {
		return
	}
	// A stage reports exclusive time: reads and encodes it issued were counted
	// under their own stages.
	d := now.Sub(u.started) - u.children
	u.children, u.depth = 0, 0
	add(u.stage, d*SampleEvery, SampleEvery)
}

// Nested starts a read or encode issued during the decode stage, possibly
// inside another nested stage, and returns its start; Unnest ends it, records
// its exclusive time and resumes the enclosing stage. Elsewhere (loading,
// merging) the enclosing stage keeps the time and Nested returns zero.
func (u *Update) Nested(s Stage) time.Time {
	if u == nil || u.stage != Decode || u.depth == len(u.frames) {
		return time.Time{}
	}
	now := time.Now()
	u.frames[u.depth] = frame{stage: s, start: now}
	u.depth++
	setLabel(s)
	return now
}

// Unnest ends the nested stage s started at start.
func (u *Update) Unnest(s Stage, start time.Time) {
	if u == nil || start.IsZero() || u.depth == 0 {
		return
	}
	u.depth--
	f := u.frames[u.depth]
	d := time.Since(f.start)
	add(s, (d-f.children)*SampleEvery, SampleEvery)
	enclosing := u.stage
	if u.depth > 0 {
		u.frames[u.depth-1].children += d
		enclosing = u.frames[u.depth-1].stage
	} else {
		u.children += d
	}
	setLabel(enclosing)
}

// Finish ends the update and clears its goroutine label.
func (u *Update) Finish() {
	if u == nil {
		return
	}
	u.leave(time.Now())
	u.stage = NumStages
	pprof.SetGoroutineLabels(u.base)
}

var labelContexts = func() [NumStages + 1]context.Context {
	var out [NumStages + 1]context.Context
	for s := range NumStages {
		out[s] = pprof.WithLabels(phasecpu.Context(phasecpu.Persister), pprof.Labels("persister_stage", stageNames[s]))
	}
	out[NumStages] = context.Background()
	return out
}()

func setLabel(s Stage) { pprof.SetGoroutineLabels(labelContexts[min(s, NumStages)]) }
