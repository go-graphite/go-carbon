package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func pendingFixture(t *testing.T) *recovery.Bundle {
	t.Helper()
	dir, root := t.TempDir(), t.TempDir()
	builder := recovery.NewBuilder(nil)
	var source [2][]byte
	for file, values := range [][]*points.Points{
		{points.OnePoint("metric", 1, 1), points.OnePoint("other", 5, 1), points.OnePoint("metric", 2, 2)},
		{points.OnePoint("metric", 3, 1), points.OnePoint("other", 6, 2)},
	} {
		for _, p := range values {
			raw := p.AppendBinary(nil)
			source[file] = append(source[file], raw...)
			if err := builder.Add(file, p, len(raw)); err != nil {
				t.Fatal(err)
			}
		}
	}
	var index bytes.Buffer
	if err := builder.Write(&index); err != nil {
		t.Fatal(err)
	}
	var files []recovery.File
	for i, name := range []string{"cache.1.2.bin", "input.1.2.bin", ".pending-index-2.bin"} {
		raw := index.Bytes()
		if i < 2 {
			raw = source[i]
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		info, err := recovery.Describe(path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, info)
	}
	if err := recovery.Publish(dir, root, files[0], files[1], files[2], "test-read-index"); err != nil {
		t.Fatal(err)
	}
	bundle, err := recovery.OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bundle.Close() })
	return bundle
}

func TestPendingReadHandoffAndPersistence(t *testing.T) {
	bundle := pendingFixture(t)
	c := New()
	if err := c.AttachPendingRecovery(bundle); err != nil {
		t.Fatal(err)
	}
	want := []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 1}}
	if got := c.Get("metric"); !reflect.DeepEqual(got, want) {
		t.Fatal("unclaimed history", got)
	}
	if c.IsEmpty() {
		t.Fatal("unclaimed source reported empty")
	}
	// Reads overlap the actual source-to-cache handoff and repeated failed writes.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var reads atomic.Int64
	ready := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if got := c.Get("metric"); !reflect.DeepEqual(got, want) {
				t.Errorf("history gap during handoff: %v", got)
				return
			}
			if reads.Add(1) == 1 {
				close(ready)
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("concurrent reader did not start")
	}
	if err := c.RecoverPending(nil, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		batch, ok := c.PopNotConfirmed("metric")
		if !ok {
			t.Fatal("missing queued recovery batch")
		}
		if got := c.Get("metric"); !reflect.DeepEqual(got, want) {
			t.Fatal("inflight history missing", got)
		}
		c.Requeue(batch)
	}
	close(stop)
	wg.Wait()
	if reads.Load() == 0 {
		t.Fatal("no concurrent recovery read")
	}
	if err := c.FinishPendingRecovery(); err == nil {
		t.Fatal("source retired before persistence")
	}
	for _, name := range []string{"metric", "other"} {
		p, ok := c.PopNotConfirmed(name)
		if !ok {
			t.Fatal(name)
		}
		c.Confirm(p)
	}
	if !c.IsEmpty() {
		t.Fatal("confirmed recovery not empty")
	}
	if err := c.FinishPendingRecovery(); err != nil {
		t.Fatal(err)
	}
	if c.pending.Load() != nil {
		t.Fatal("source retained after durable drain")
	}
	c.Add(points.OnePoint("metric", 4, 1))
	if got := c.Get("metric"); !reflect.DeepEqual(got, []points.Point{{Value: 4, Timestamp: 1}}) {
		t.Fatal("new live input", got)
	}
}

func TestPendingRecoveryCapacityAndCancellation(t *testing.T) {
	c := New()
	c.SetMaxSize(2)
	if err := c.AttachPendingRecovery(pendingFixture(t)); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- c.RecoverPending(stop, 0) }()
	deadline := time.Now().Add(time.Second)
	for c.Size() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Size() == 0 {
		t.Fatal("one oversized metric must make progress")
	}
	close(stop)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected incomplete recovery")
		}
	case <-time.After(time.Second):
		t.Fatal("recovery ignored cancellation")
	}
	if c.IsEmpty() {
		t.Fatal("cancellation lost source")
	}
	if len(c.Get("metric")) != 3 || len(c.Get("other")) != 2 {
		t.Fatal("cancellation lost readable history")
	}
}

func TestPendingRecoveryRejectsInvalidAttach(t *testing.T) {
	c := New()
	c.Add(points.OnePoint("existing", 1, 1))
	bundle := pendingFixture(t)
	if err := c.AttachPendingRecovery(bundle); err == nil {
		t.Fatal("attached to nonempty cache")
	}
	c.Pop("existing")
	if err := c.AttachPendingRecovery(bundle); err != nil {
		t.Fatal(err)
	}
	if err := c.AttachPendingRecovery(bundle); err == nil {
		t.Fatal("attached twice")
	}
}

// A live write may arrive before RecoverPending reaches the metric. Its saved
// history must enter the same item first, and the sources may retire only after
// that item (not merely the claim) is confirmed on disk.
func TestLiveWriteClaimsSavedHistoryFirst(t *testing.T) {
	bundle := pendingFixture(t)
	c := New()
	if err := c.AttachPendingRecovery(bundle); err != nil {
		t.Fatal(err)
	}
	if !c.Has("metric") || !c.Has("other") || c.Has("absent") {
		t.Fatal("pending metrics invisible to Has")
	}
	c.Add(points.OnePoint("metric", 9, 10))
	want := []points.Point{{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 1}, {Value: 9, Timestamp: 10}}
	if got := c.Get("metric"); !reflect.DeepEqual(got, want) {
		t.Fatal("live write did not follow saved history", got)
	}
	if got := c.PendingOutstanding(); got != 2 { // "other" unclaimed + "metric" unpersisted
		t.Fatal("outstanding", got)
	}
	batch, ok := c.PopNotConfirmed("metric")
	if !ok || !reflect.DeepEqual(batch.Data, want) {
		t.Fatal("persister batch differs", batch)
	}
	// A later live point creates a new item; a failed write merges back.
	c.Add(points.OnePoint("metric", 11, 12))
	c.Requeue(batch)
	if err := c.RecoverPending(nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.FinishPendingRecovery(); err == nil {
		t.Fatal("retired with unpersisted saved history")
	}
	for _, name := range []string{"metric", "other"} {
		p, ok := c.PopNotConfirmed(name)
		if !ok {
			t.Fatal(name)
		}
		if name == "metric" && len(p.Data) != 5 {
			t.Fatal("requeued batch lost points", p.Data)
		}
		c.Confirm(p)
	}
	if got := c.PendingOutstanding(); got != 0 {
		t.Fatal("outstanding after confirm", got)
	}
	if err := c.FinishPendingRecovery(); err != nil {
		t.Fatal(err)
	}
}

// A dump taken mid-recovery must hold unclaimed saved metrics as well as
// claimed ones still in cache, each exactly once and in replay order.
func TestDumpDuringPendingRecovery(t *testing.T) {
	bundle := pendingFixture(t)
	c := New()
	if err := c.AttachPendingRecovery(bundle); err != nil {
		t.Fatal(err)
	}
	c.Add(points.OnePoint("metric", 9, 10))
	got := map[string][]points.Point{}
	if err := c.DumpPoints(func(p *points.Points) error {
		got[p.Metric] = append(got[p.Metric], p.Data...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string][]points.Point{
		"metric": {{Value: 1, Timestamp: 1}, {Value: 2, Timestamp: 2}, {Value: 3, Timestamp: 1}, {Value: 9, Timestamp: 10}},
		"other":  {{Value: 5, Timestamp: 1}, {Value: 6, Timestamp: 2}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("dump during recovery", got)
	}
}

// Raw-copied unclaimed records must restore exactly like the decoded ones,
// each metric once, across any number of parts.
func TestDumpPendingRangesCoverEachMetricOnce(t *testing.T) {
	bundle := pendingFixture(t)
	c := New()
	if err := c.AttachPendingRecovery(bundle); err != nil {
		t.Fatal(err)
	}
	c.Add(points.OnePoint("metric", 9, 10)) // claimed: must come from the cache instead
	for _, parts := range []int{1, 2, 3, 7} {
		path := filepath.Join(t.TempDir(), "dump.bin")
		w, err := recovery.NewWriter(path, 0, 1<<20, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.WriteSegments(parts, func(seg int, out *recovery.Segment) error { return c.DumpPendingRange(seg, parts, out) }); err != nil {
			t.Fatal(err)
		}
		if _, err = w.Close(); err != nil {
			t.Fatal(err)
		}
		got := map[string][]points.Point{}
		f, _ := os.Open(path)
		err = points.ReadBinary(f, func(p *points.Points) { got[p.Metric] = append(got[p.Metric], p.Data...) })
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string][]points.Point{"other": {{Value: 5, Timestamp: 1}, {Value: 6, Timestamp: 2}}}
		if !reflect.DeepEqual(got, want) {
			t.Fatal("pending parts", parts, got)
		}
	}
}
