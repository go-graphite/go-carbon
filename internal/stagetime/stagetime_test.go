package stagetime

import (
	"bytes"
	"context"
	"math"
	"runtime/pprof"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func sampled(t *testing.T) (context.Context, *Update) {
	t.Helper()
	for range SampleEvery {
		if ctx, u := Start(context.Background()); u != nil {
			return ctx, u
		}
	}
	t.Fatal("no update sampled in SampleEvery starts")
	return nil, nil
}

func TestStartSamplesOneInEvery(t *testing.T) {
	got := 0
	for range 10 * SampleEvery {
		ctx, u := Start(context.Background())
		if u != nil {
			got++
			if FromContext(ctx) != u {
				t.Fatal("sampled timer not carried by its context")
			}
			u.Finish()
		} else if FromContext(ctx) != nil {
			t.Fatal("unsampled context carries a timer")
		}
	}
	if got != 10 {
		t.Fatalf("sampled %d of %d starts, want 10", got, 10*SampleEvery)
	}
}

func TestStagesAreScaledAndDecodeIsExclusive(t *testing.T) {
	synctest.Test(t, testStagesAreScaledAndDecodeIsExclusive)
}

func testStagesAreScaledAndDecodeIsExclusive(t *testing.T) {
	before := Snapshot()
	_, u := sampled(t)
	u.Enter(Load)
	time.Sleep(2 * time.Millisecond)
	u.Enter(Decode)
	encodeStart := u.Nested(Encode)
	time.Sleep(time.Millisecond)
	readStart := u.Nested(TailRead)
	time.Sleep(5 * time.Millisecond)
	if third := u.Nested(Encode); !third.IsZero() {
		t.Fatal("nesting deeper than two levels")
	}
	u.Unnest(TailRead, readStart)
	u.Unnest(Encode, encodeStart)
	time.Sleep(time.Millisecond)
	u.Enter(Commit)
	if !u.Nested(TailRead).IsZero() {
		t.Fatal("nested timing outside decode")
	}
	u.Finish()
	after := Snapshot()
	delta := func(s Stage) (float64, uint64) {
		// Snapshot converts integer nanoseconds to seconds; undo subtraction
		// rounding at that same resolution before checking exact fake time.
		seconds := math.Round((after[s].Seconds-before[s].Seconds)*1e9) / 1e9
		return seconds, after[s].Calls - before[s].Calls
	}
	for _, s := range []Stage{Load, Decode, TailRead, Encode, Commit} {
		if _, calls := delta(s); calls != SampleEvery {
			t.Fatalf("%s calls %d, want %d", s, calls, SampleEvery)
		}
	}
	load, _ := delta(Load)
	read, _ := delta(TailRead)
	encode, _ := delta(Encode)
	decode, _ := delta(Decode)
	if load < 0.002*SampleEvery || read < 0.005*SampleEvery {
		t.Fatalf("scaled load %.4fs read %.4fs too small", load, read)
	}
	if encode >= read || decode >= read {
		t.Fatalf("encode %.4fs or decode %.4fs includes the nested read %.4fs", encode, decode, read)
	}
}

func TestUpdateHistogramAndMax(t *testing.T) {
	TakeUpdateMax()
	_, before, sumBefore := UpdateHistogram()
	ObserveUpdate(50 * time.Microsecond)
	ObserveUpdate(3 * time.Millisecond)
	ObserveUpdate(20 * time.Second)
	buckets, count, sum := UpdateHistogram()
	if count-before != 3 {
		t.Fatalf("count grew by %d, want 3", count-before)
	}
	if sum-sumBefore < 20 {
		t.Fatalf("sum grew by %.3fs", sum-sumBefore)
	}
	if buckets[64e-6] < 1 || buckets[4.096e-3] < 2 {
		t.Fatalf("buckets %v", buckets)
	}
	if got := TakeUpdateMax(); got != 20*time.Second {
		t.Fatalf("max %v, want 20s", got)
	}
	if got := TakeUpdateMax(); got != 0 {
		t.Fatalf("max not reset: %v", got)
	}
}

func goroutineProfile(t *testing.T) string {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// Labels mark the running stage and are gone once the update finishes.
func TestLabelsFollowStagesAndDoNotLeak(t *testing.T) {
	step, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		var u *Update
		for u == nil {
			_, u = Start(context.Background())
		}
		u.Enter(Decode)
		step <- struct{}{}
		<-step
		u.Finish()
		step <- struct{}{}
		<-step
	}()
	<-step
	if !strings.Contains(goroutineProfile(t), `"persister_stage":"decode"`) {
		t.Fatal("decode stage label missing during the update")
	}
	step <- struct{}{}
	<-step
	if strings.Contains(goroutineProfile(t), "persister_stage") {
		t.Fatal("stage label survived Finish")
	}
	step <- struct{}{}
	<-done
}

func TestNilUpdateIsInert(t *testing.T) {
	var u *Update
	u.Enter(Load)
	u.Unnest(TailRead, u.Nested(TailRead))
	u.Leave()
	u.Finish()
	if FromContext(context.Background()) != nil {
		t.Fatal("empty context carries a timer")
	}
}

// BenchmarkUpdateOverhead is the instrumentation cost the persister adds per
// update: the total timer for every update, plus a sampled update's stage
// transitions (about ten enters and three nested reads/encodes) once in
// SampleEvery.
func BenchmarkUpdateOverhead(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, u := Start(ctx)
		for _, s := range []Stage{LockWait, Route, LockWait, Route, Load, Decode} {
			u.Enter(s)
		}
		for range 3 {
			u.Unnest(Encode, u.Nested(Encode))
		}
		u.Enter(Encode)
		u.Enter(Commit)
		u.Finish()
		ObserveUpdate(time.Since(start))
	}
}
