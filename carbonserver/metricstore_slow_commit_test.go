package carbonserver

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

type pausedReadHandoffStore struct {
	*store.Store
	entered, allowCommit, committed, allowReturn chan struct{}
	commitError                                  error
}

func (s *pausedReadHandoffStore) update(commit func() error) error {
	close(s.entered)
	<-s.allowCommit
	err := s.commitError
	if err == nil {
		err = commit()
	}
	close(s.committed)
	<-s.allowReturn
	return err
}

func (s *pausedReadHandoffStore) UpdateMany(ctx context.Context, name string, input []points.Point) error {
	return s.update(func() error { return s.Store.UpdateMany(ctx, name, input) })
}

func (s *pausedReadHandoffStore) UpdateManyMaintained(ctx context.Context, name string, input []store.Point, maintenance store.ContinuationMaintenance) error {
	return s.update(func() error { return s.Store.UpdateManyMaintained(ctx, name, input, maintenance) })
}

// A slow commit must not prevent reads of accepted in-flight points. Compare
// the actual public-read implementation against its pre-write result, including
// an incoming correction after PopNotConfirmed and the failed-write requeue.
func TestSharedReadDoesNotWaitForCommit(t *testing.T) {
	t.Parallel()
	for _, maintained := range []bool{false, true} {
		for _, phase := range []string{"before-commit", "after-commit", "failed-commit"} {
			label := phase
			if maintained {
				label += "/maintained"
			}
			t.Run(label, func(t *testing.T) { checkReadDuringSlowSharedCommit(t, phase, maintained) })
		}
	}
}

func checkReadDuringSlowSharedCommit(t *testing.T, phase string, maintained bool) {
	root, destination := t.TempDir(), t.TempDir()
	const metric = "slow.commit.handoff"
	now := time.Now().Unix()
	path := filepath.Join(root, "slow", "commit", "handoff.wsp")
	// Keep source creation independent of the change being tested.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	w, err := whisper.CreateWithOptions(path, whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(1, 3600)}), whisper.Average, .5, &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now - 100), Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(destination, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: root})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := cache.New()
	queue := make(chan string, 1)
	finished := make(chan struct{})
	recv := func(exit chan bool) string {
		select {
		case name := <-queue:
			return name
		case <-exit:
			return ""
		}
	}
	p := persister.NewWhisper(root, nil, persister.NewWhisperAggregation(), recv, c.PopNotConfirmed, func(batch *points.Points) { c.Confirm(batch); close(finished) }, c.Pop)
	p.SetRequeue(func(batch *points.Points) { c.Requeue(batch); close(finished) })
	p.SetMetricStore(db)
	if err = p.SetIncrementalMigration(db, destination); err != nil {
		t.Fatal(err)
	}
	if _, err = p.ConvertFile(context.Background(), metric); err != nil {
		t.Fatal(err)
	}
	if maintained {
		p.EnableOutOfOrder(0, 65536)
	}
	listener := newMetricStoreTestListener(t, c, db, false)
	input := []points.Point{{Timestamp: now - 4, Value: math.Copysign(0, -1)}, {Timestamp: now - 10, Value: 42}, {Timestamp: now - 6, Value: math.Nextafter(1, 2)}}
	addBatch := func() { c.Add(&points.Points{Metric: metric, Data: append([]points.Point(nil), input...)}) }
	addBatch()
	c.Add(points.OnePoint(metric, 66, now-10))
	golden, err := listener.fetchSingleMetric(metric, "", int32(now-20), int32(now))
	if err != nil {
		t.Fatal(err)
	}
	for _, point := range []points.Point{{Timestamp: now - 4, Value: math.Copysign(0, -1)}, {Timestamp: now - 10, Value: 66}, {Timestamp: now - 6, Value: math.Nextafter(1, 2)}} {
		index := (point.Timestamp - golden.StartTime) / golden.StepTime
		if index < 0 || index >= int64(len(golden.Values)) || math.Float64bits(golden.Values[index]) != math.Float64bits(point.Value) {
			t.Fatalf("oracle missing exact queued value at %d", point.Timestamp)
		}
	}
	c.Pop(metric)
	addBatch()
	gate := &pausedReadHandoffStore{Store: db, entered: make(chan struct{}), allowCommit: make(chan struct{}), committed: make(chan struct{}), allowReturn: make(chan struct{})}
	if phase == "failed-commit" {
		gate.commitError = errors.New("injected write failure")
	}
	p.SetMetricStore(gate)
	var allowCommit, allowReturn sync.Once
	releaseCommit := func() { allowCommit.Do(func() { close(gate.allowCommit) }) }
	releaseReturn := func() { allowReturn.Do(func() { close(gate.allowReturn) }) }
	if err = p.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { releaseCommit(); releaseReturn(); p.Stop() }()
	queue <- metric
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not reach held commit")
	}
	// An arrival correcting the popped batch must win on either persistent view.
	c.Add(points.OnePoint(metric, 66, now-10))
	if phase != "before-commit" {
		releaseCommit()
		select {
		case <-gate.committed:
		case <-time.After(3 * time.Second):
			t.Fatal("commit did not finish")
		}
	}
	type readResult struct {
		response response
		err      error
	}
	read := make(chan readResult, 1)
	go func() {
		r, e := listener.fetchSingleMetric(metric, "", int32(now-20), int32(now))
		read <- readResult{r, e}
	}()
	select {
	case result := <-read:
		if result.err != nil {
			t.Fatal(result.err)
		}
		assertReadHandoffBits(t, golden, result.response)
	case <-time.After(2 * time.Second):
		releaseCommit()
		releaseReturn()
		<-read
		t.Fatal("read waited for the held storage commit despite visible accepted cache points")
	}
	releaseCommit()
	releaseReturn()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("cache handoff did not finish")
	}
	after, err := listener.fetchSingleMetric(metric, "", int32(now-20), int32(now))
	if err != nil {
		t.Fatal(err)
	}
	assertReadHandoffBits(t, golden, after)
}

func assertReadHandoffBits(t *testing.T, a, b response) {
	t.Helper()
	if a.StartTime != b.StartTime || a.StopTime != b.StopTime || a.StepTime != b.StepTime || len(a.Values) != len(b.Values) {
		t.Fatal("public read changed timestamps or step")
	}
	for i := range a.Values {
		if math.IsNaN(a.Values[i]) && math.IsNaN(b.Values[i]) {
			continue
		}
		if math.Float64bits(a.Values[i]) != math.Float64bits(b.Values[i]) {
			t.Fatalf("public read changed bits at offset %d: %x != %x", i, math.Float64bits(a.Values[i]), math.Float64bits(b.Values[i]))
		}
	}
}
