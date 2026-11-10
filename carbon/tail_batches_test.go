package carbon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-graphite/go-carbon/internal/restartcpu"
	"github.com/go-graphite/go-carbon/points"
)

func TestBudgetedTailPreservesCorrectionsAndTornPrefix(t *testing.T) {
	var raw []byte
	// Repeat the same metric/timestamp with different values across many batch
	// boundaries. Reordering any worker's queue changes the final correction.
	for n := range 4096 {
		p := points.Points{Metric: fmt.Sprintf("tail.%d", n%17), Data: []points.Point{{Timestamp: int64(n % 3), Value: float64(n)}}}
		raw = p.AppendBinary(raw)
	}
	p := points.Points{Metric: "partial", Data: []points.Point{{Timestamp: 1, Value: 7}, {Timestamp: 2, Value: 19}}}
	raw = p.AppendBinary(raw)
	raw = raw[:len(raw)-1]
	want := map[string][]points.Point{}
	wantErr := points.ReadBinary(bytes.NewReader(raw), func(p *points.Points) {
		want[p.Metric] = append(want[p.Metric], p.Data...)
	})
	for _, workers := range []int{1, 4, 32} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			got := map[string][]points.Point{}
			var mu sync.Mutex
			b := restartcpu.NewWithCPU(32, func() float64 { return 0 })
			count, err := replayInputTailBatchesWithBudget(context.Background(), bytes.NewReader(raw), workers, func(p *points.Points) {
				mu.Lock()
				defer mu.Unlock()
				got[p.Metric] = append(got[p.Metric], p.Data...)
			}, b)
			if !errors.Is(err, wantErr) || !reflect.DeepEqual(got, want) {
				t.Fatalf("torn-prefix replay differs: error %v (want %v), metrics %d/%d", err, wantErr, len(got), len(want))
			}
			expected := 0
			for _, data := range want {
				expected += len(data)
			}
			if count != expected {
				t.Fatalf("replayed %d points, want %d", count, expected)
			}
		})
	}
}

func TestBudgetedTailSplitsLargeLegacyRecordBeforeApplying(t *testing.T) {
	p := &points.Points{Metric: "tail.large"}
	for i := range 16513 {
		p.Add(float64(i), int64(i%3))
	}
	raw := points.OnePoint(p.Metric, -1, -1).AppendBinary(nil)
	raw = p.AppendBinary(raw)
	var got []points.Point
	maxBatch := 0
	count, err := replayInputTailBatches(bytes.NewReader(raw), 4, func(next *points.Points) {
		// One metric always uses one FIFO, including across record boundaries.
		maxBatch = max(maxBatch, len(next.Data))
		got = append(got, next.Data...)
	})
	want := append([]points.Point{{Value: -1, Timestamp: -1}}, p.Data...)
	if err != nil || count != len(want) || !reflect.DeepEqual(got, want) {
		t.Fatalf("large-record replay changed points: count=%d/%d error=%v", count, len(want), err)
	}
	// At most one almost-full queue window plus one decoded window.
	if maxBatch > 255 {
		t.Fatalf("legacy record escaped bounded replay: batch=%d", maxBatch)
	}
}

func TestBudgetedTailCancellationJoinsQueuesWithoutAcceptableEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var raw []byte
		for n := range 10000 {
			p := points.Points{Metric: "tail", Data: []points.Point{{Timestamp: int64(n), Value: float64(n)}}}
			raw = p.AppendBinary(raw)
		}
		b := restartcpu.NewWithCPU(.5, func() float64 { return 0 })
		var applied atomic.Int64
		done := make(chan error, 1)
		go func() {
			_, err := replayInputTailBatchesWithBudget(ctx, bytes.NewReader(raw), 32, func(p *points.Points) {
				applied.Add(int64(len(p.Data)))
				time.Sleep(time.Millisecond) // A measurable batch, without burning CPU.
			}, b)
			done <- err
		}()
		time.Sleep(20 * time.Millisecond)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("canceled history could be retired as a torn tail: %v", err)
		}
		before := applied.Load()
		if before == 0 || before == 10000 {
			t.Fatalf("did not interrupt a partly replayed tail: %d", before)
		}
		time.Sleep(time.Second)
		if applied.Load() != before {
			t.Fatal("replay returned with an applying worker still running")
		}
	})
}

func TestBudgetedTailBoundsAppliedWorkUnderCPUDebt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var raw []byte
		for n := range 512 {
			p := points.OnePoint("tail", float64(n), int64(n))
			raw = p.AppendBinary(raw)
		}
		run := func(budget *restartcpu.Budget) time.Duration {
			started := time.Now()
			applied := 0
			count, err := replayInputTailBatchesWithBudget(context.Background(), bytes.NewReader(raw), 32, func(p *points.Points) {
				applied += len(p.Data) // All records hash to the same FIFO.
				time.Sleep(50 * time.Microsecond)
			}, budget)
			if err != nil || count != 512 || applied != 512 {
				t.Fatalf("replay lost work: decoded=%d applied=%d error=%v", count, applied, err)
			}
			return time.Since(started)
		}
		unpaced := run(nil)
		started := time.Now()
		budget := restartcpu.NewWithCPU(2, func() float64 { return 3 * time.Since(started).Seconds() })
		time.Sleep(time.Second) // Mandatory work has exhausted the CPU credit.
		paced := run(budget)
		// The worker and decoder share a quarter core, at most one eighth each.
		// One uncharged 5ms duty remainder is allowed; no wall-clock CPU burn.
		if paced < 8*unpaced-5*time.Millisecond {
			t.Fatalf("historical insertion escaped its duty share: paced=%v unpaced=%v", paced, unpaced)
		}
	})
}
