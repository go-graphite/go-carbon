// Package stagetime partitions sampled persister batch wall time into exclusive
// stages. Total uses the same sample as its parts, including Other, so they add
// up. All stage seconds are scaled by SampleEvery. The duration histogram and
// maximum still measure every batch, including failures and no-work attempts.
// Queue, throttle and batch-collection waits precede this measurement.
// Sampled goroutines carry a persister_stage CPU profile label.
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
	// Total is the whole sampled batch, including otherwise unlabelled work.
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
	// Publish installs committed state and releases mutation reservations/batches.
	Publish
	// Acknowledge confirms or requeues cached input and updates write statistics.
	Acknowledge
	// CachePop collects cached input after acquiring the persister locks.
	CachePop
	// Other covers time between named stages, including instrumentation overhead.
	Other
	// NumStages is the number of stages.
	NumStages
)

var stageNames [NumStages]string = phasecpu.PersisterStageNames

func (s Stage) String() string { return stageNames[s] }

// SampleEvery is the fraction of updates (one in SampleEvery) whose stages are
// timed and labelled.
const SampleEvery = phasecpu.PersisterStageSampleEvery

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
	updateSum     atomic.Uint64
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
	counters[Total].calls.Add(1)
	if d > 0 {
		updateSum.Add(uint64(d))
	}
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
	return buckets, count, float64(updateSum.Load()) / 1e9
}

// TakeUpdateMax returns the slowest update since the previous call and resets
// it, so each scrape reports the maximum of its own interval.
func TakeUpdateMax() time.Duration { return time.Duration(updateMax.Swap(0)) }

// Update holds the stage timing of one sampled update. It belongs to the
// goroutine running the update.
type Update struct {
	begin   time.Time
	elapsed [NumStages]time.Duration
	visited uint16
	context.Context
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
	return StartAt(ctx, time.Now())
}

// StartAt reuses the caller's batch start clock reading.
func StartAt(ctx context.Context, start time.Time) (context.Context, *Update) {
	return StartReusable(ctx, start, nil)
}

// StartReusable uses goroutine-owned scratch for the lifetime of this batch.
// A worker may reuse it after FinishUpdate; no per-batch allocation is needed.
func StartReusable(ctx context.Context, start time.Time, scratch *Update) (context.Context, *Update) {
	if sampleTick.Add(1)%SampleEvery != 0 {
		return ctx, nil
	}
	u := scratch
	if u == nil {
		u = new(Update)
	}
	*u = Update{stage: Other, Context: ctx, begin: start, started: start}
	setLabel(Other)
	return u, u
}

// Value exposes the timer while preserving all other parent context values.
func (u *Update) Value(key any) any {
	if _, ok := key.(contextKey); ok {
		return u
	}
	return u.Context.Value(key)
}

// FromContext returns the update timer carried by ctx, or nil.
func FromContext(ctx context.Context) *Update {
	u, _ := ctx.Value(contextKey{}).(*Update)
	return u
}

// Enter ends the running stage, if any, and starts s.
func (u *Update) Enter(s Stage) {
	if u == nil || u.stage == s {
		return
	}
	u.EnterAt(s, time.Now())
}

// EnterAt reuses an existing boundary clock read.
func (u *Update) EnterAt(s Stage, now time.Time) {
	if u == nil {
		return
	}
	u.leave(now)
	u.stage, u.started = s, now
	setLabel(s)
}

// Leave ends the running stage; subsequent work belongs to Other.
func (u *Update) Leave() {
	if u == nil {
		return
	}
	now := time.Now()
	u.leave(now)
	u.stage = Other
	u.started = now
	setLabel(Other)
}

func (u *Update) leave(now time.Time) {
	if u.stage >= NumStages {
		return
	}
	// A stage reports exclusive time: reads and encodes it issued were counted
	// under their own stages.
	d := now.Sub(u.started) - u.children
	u.children, u.depth = 0, 0
	u.elapsed[u.stage] += d
	u.visited |= 1 << u.stage
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
	u.elapsed[s] += d - f.children
	u.visited |= 1 << s
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
	if u != nil {
		u.finish(time.Now())
	}
}

// FinishUpdate shares one final clock read between the partition and the exact
// all-batch duration histogram. It also works for an unsampled (nil) timer.
func (u *Update) FinishUpdate(start time.Time) {
	end := time.Now()
	if u != nil {
		u.finish(end)
	}
	ObserveUpdate(end.Sub(start))
}

func (u *Update) finish(end time.Time) {
	if u.stage == NumStages {
		return
	}
	u.leave(end)
	// Exclusive stage intervals partition the same start/end as Total.
	// No-work gaps remain Other rather than disappearing from the breakdown.
	var accounted time.Duration
	for s := Route; s < NumStages; s++ {
		accounted += u.elapsed[s]
	}
	u.elapsed[Other] += end.Sub(u.begin) - accounted
	for s := Route; s < NumStages; s++ {
		if u.visited&(1<<s) != 0 || s == Other {
			add(s, u.elapsed[s]*SampleEvery, SampleEvery)
		}
	}
	counters[Total].nanos.Add(uint64(end.Sub(u.begin)) * SampleEvery)
	u.stage = NumStages
	pprof.SetGoroutineLabels(u.Context)
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
