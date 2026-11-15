package persister

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/stagetime"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

type gatedBatchStore struct {
	failedSharedStore
	entered chan []store.MetricUpdate
	commit  chan error
}

func (g *gatedBatchStore) UpdateBatch(_ context.Context, updates []store.MetricUpdate, _ *store.ContinuationMaintenance) error {
	g.entered <- updates
	return <-g.commit
}

func TestSharedBatchWorkerCommitAndInputLog(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			c := cache.New()
			c.EnableInputLog()
			names := []string{"batch.worker.a", "batch.worker.b"}
			for _, name := range names {
				c.Add(points.OnePoint(name, 3, 1790000000))
			}
			for shard := range cache.ShardCount {
				c.TakeLogEntries(shard, 9, nil)
			}
			initial := c.LogLowWater()
			if initial == math.MaxUint64 {
				t.Fatal("input log has no pending generation")
			}
			exit := make(chan bool)
			received := false
			recv := func(abort chan bool) string {
				if !received {
					received = true
					return names[0]
				}
				<-abort
				return ""
			}
			next := names[1]
			var calls []string
			g := &gatedBatchStore{failedSharedStore: failedSharedStore{calls: &calls}, entered: make(chan []store.MetricUpdate), commit: make(chan error)}
			confirmed := make(chan string, 2)
			requeued := make(chan string, 2)
			p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), recv, c.PopNotConfirmed, func(batch *points.Points) { c.Confirm(batch); confirmed <- batch.Metric }, c.Pop)
			p.SetRequeue(func(batch *points.Points) { c.Requeue(batch); requeued <- batch.Metric })
			p.SetMetricStore(g)
			p.SetBacklogged(func() bool { return true })
			p.SetStoreUpdateBatchSize(8)
			p.SetWriteoutBatchReceiver(func() string { result := next; next = ""; return result })
			beforeCalls := stagetime.Snapshot()[stagetime.Total].Calls
			done := make(chan struct{})
			go func() { p.worker(exit); close(done) }()
			select {
			case updates := <-g.entered:
				if len(updates) != 2 || updates[0].Name != names[0] || updates[1].Name != names[1] {
					t.Fatalf("updates %+v", updates)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not group queued work")
			}
			if c.LogLowWater() != initial {
				t.Fatal("input log advanced before commit")
			}
			for _, name := range names {
				if len(c.Get(name)) != 1 {
					t.Fatal("in-flight points not visible")
				}
			}
			select {
			case <-confirmed:
				t.Fatal("ack before commit")
			default:
			}
			if fail {
				g.commit <- errors.New("injected commit error")
			} else {
				g.commit <- nil
			}
			for range names {
				if fail {
					select {
					case <-requeued:
					case <-time.After(5 * time.Second):
						t.Fatal("not requeued")
					}
				} else {
					select {
					case <-confirmed:
					case <-time.After(5 * time.Second):
						t.Fatal("not acknowledged")
					}
				}
			}
			close(exit)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not stop")
			}
			if calls := stagetime.Snapshot()[stagetime.Total].Calls - beforeCalls; calls != 1 {
				t.Fatalf("one worker batch recorded %d total durations", calls)
			}
			if fail {
				if c.LogLowWater() != initial {
					t.Fatal("failed input lost its generation")
				}
			} else if c.LogLowWater() <= initial {
				t.Fatal("confirmed inputs still pin the input log")
			}
			if len(calls) != 0 {
				t.Fatal("worker used individual commits")
			}
		})
	}
}

func TestSharedBatchRateTokens(t *testing.T) {
	ticker := &helper.ThrottleTicker{C: make(chan bool, 2)}
	ticker.C <- true
	p := &Whisper{updateTicker: ticker}
	if !p.takeBatchToken() || p.takeBatchToken() {
		t.Fatal("batching exceeded available rate tokens")
	}
}

func TestSharedBatchDisabledForOtherRoutes(t *testing.T) {
	var calls []string
	g := &gatedBatchStore{failedSharedStore: failedSharedStore{calls: &calls}}
	p := &Whisper{storeUpdateBatchSize: 8, metricStore: g, tryRecv: func() string { return "" }, requeue: func(*points.Points) {}}
	if !p.canBatchShared() {
		t.Fatal("shared route did not batch")
	}
	for _, flags := range [][2]bool{{true, false}, {false, true}} {
		p.mirrorFiles, p.mirrorImportMissing = flags[0], flags[1]
		if p.canBatchShared() {
			t.Fatal("mirror route batched")
		}
	}
	if !reflect.DeepEqual(calls, []string(nil)) {
		t.Fatal(calls)
	}
}

func TestSharedBatchFallsBackWithoutLosingPoints(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1790000000, 0)
	c := cache.New()
	db, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, Now: func() time.Time { return now }, SyncInterval: time.Hour, MetadataCacheSize: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	retentions := whisper.MustParseRetentionDefs("1s:2m")
	p := NewWhisper(t.TempDir(), WhisperSchemas{{Pattern: regexp.MustCompile(".*"), Retentions: retentions}}, NewWhisperAggregation(), nil, c.PopNotConfirmed, c.Confirm, c.Pop)
	p.SetMetricStore(db)
	p.SetRequeue(c.Requeue)
	if _, err := db.Create(ctx, store.MetricConfig{Name: "existing", Retentions: []store.Retention{{Step: 1, Count: 120}}, AggregationMethod: store.Average}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"existing", "new"} {
		c.Add(points.OnePoint(name, 3, now.Unix()-1))
	}
	p.storeSharedBatch([]string{"existing", "new", "existing"})
	if stats, _ := p.WriteStats(); stats.Failures != 0 || stats.FailedPoints != 0 || stats.Points != 2 {
		t.Fatalf("fallback accounting %+v", stats)
	}
	if p.SharedBatchFallbacks()["absent"] != 1 {
		t.Fatal(p.SharedBatchFallbacks())
	}

	for _, name := range []string{"existing", "new"} {
		snapshot, err := db.Snapshot(ctx, name)
		if err != nil || len(snapshot.Archives[0].Points) != 1 || snapshot.Archives[0].Points[0].Value != 3 {
			t.Fatalf("%s snapshot %+v error %v", name, snapshot, err)
		}
		if len(c.Get(name)) != 0 {
			t.Fatalf("%s remained in flight", name)
		}
	}
}

type importingMetricStore struct {
	failedSharedStore
	pending bool
}

func (s *importingMetricStore) UpdateBatch(context.Context, []store.MetricUpdate, *store.ContinuationMaintenance) error {
	return store.ErrImportPending
}
func (s *importingMetricStore) TryUpdateMany(_ context.Context, name string, _ []points.Point) error {
	if name == "pending" && s.pending {
		return store.ErrImportPending
	}
	*s.calls = append(*s.calls, name)
	return nil
}

func TestSharedImportPendingRetainsInputAndCommitsOtherNames(t *testing.T) {
	c := cache.New()
	c.EnableInputLog()
	for _, name := range []string{"pending", "free"} {
		c.Add(points.OnePoint(name, 3, 1790000000))
	}
	for shard := range cache.ShardCount {
		c.TakeLogEntries(shard, 9, nil)
	}
	before := c.LogLowWater()
	var calls []string
	db := &importingMetricStore{failedSharedStore: failedSharedStore{calls: &calls}, pending: true}
	p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, c.PopNotConfirmed, c.Confirm, c.Pop)
	p.SetRequeue(c.Requeue)
	p.SetMetricStore(db)
	p.storeSharedBatch([]string{"pending", "free"})
	if !reflect.DeepEqual(calls, []string{"free"}) {
		t.Fatalf("commits %v", calls)
	}
	if len(c.Get("pending")) != 1 || len(c.Get("free")) != 0 || c.NotConfirmedLength() != 0 {
		t.Fatal("deferred/committed cache state incorrect")
	}
	if c.LogLowWater() != before {
		t.Fatal("deferred input lost its original log generation")
	}
	db.pending = false
	p.store("pending")
	if len(c.Get("pending")) != 0 || c.NotConfirmedLength() != 0 || c.LogLowWater() <= before {
		t.Fatal("retry was not committed/confirmed")
	}
	if st, _ := p.WriteStats(); st.Failures != 0 {
		t.Fatal("expected deferral counted as storage failure")
	}
}

func TestSharedBatchFallbackClassification(t *testing.T) {
	ioErr := errors.New("disk write failed")
	for _, tc := range []struct {
		err  error
		want int
	}{
		{nil, -1}, {store.ErrImportPending, 0}, {store.ErrNotFound, 1},
		{store.ErrFormat, 2}, {store.ErrReanchorDeferred, 3}, {ioErr, -1},
		{fmt.Errorf("wrapped: %w", store.ErrReanchorDeferred), 3},
		{errors.Join(store.ErrNotFound, ioErr), -1},
		{errors.Join(store.ErrImportPending, store.ErrImportPending), 0},
		{errors.Join(store.ErrImportPending, store.ErrNotFound), -1},
	} {
		if got := sharedBatchFallbackReason(tc.err); got != tc.want {
			t.Errorf("%v: got %d want %d", tc.err, got, tc.want)
		}
	}
}

type fallbackAccountingStore struct {
	failedSharedStore
	batchError error
}

func (s *fallbackAccountingStore) UpdateBatch(context.Context, []store.MetricUpdate, *store.ContinuationMaintenance) error {
	return s.batchError
}

func TestSharedBatchFallbackAccounting(t *testing.T) {
	for i, err := range []error{store.ErrImportPending, store.ErrNotFound, store.ErrFormat, store.ErrReanchorDeferred} {
		t.Run(sharedBatchFallbackReasons[i], func(t *testing.T) {
			c := cache.New()
			c.EnableInputLog()
			c.Add(points.OnePoint("metric", 3, 1790000000))
			for shard := range cache.ShardCount {
				c.TakeLogEntries(shard, 9, nil)
			}
			before := c.LogLowWater()
			var calls []string
			db := &fallbackAccountingStore{failedSharedStore: failedSharedStore{calls: &calls}, batchError: err}
			p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, c.PopNotConfirmed, c.Confirm, c.Pop)
			p.SetMetricStore(db)
			p.SetRequeue(c.Requeue)
			p.storeSharedBatch([]string{"metric"})
			stats, _ := p.WriteStats()
			if stats.Failures != 0 || stats.FailedPoints != 0 || stats.Points != 1 || p.SharedBatchFallbacks()[sharedBatchFallbackReasons[i]] != 1 {
				t.Fatalf("stats %+v fallbacks %v", stats, p.SharedBatchFallbacks())
			}
			if len(c.Get("metric")) != 0 || c.NotConfirmedLength() != 0 || c.LogLowWater() <= before {
				t.Fatal("successful fallback did not confirm original input")
			}
		})
	}
}

// Include cache pop and acknowledgement around the real continuation codec.
func TestSharedBatchStagePartition(t *testing.T) {
	update := sharedTimingWorkload(t, 210)
	before := stagetime.Snapshot()
	for i := range 128 {
		update(i)
	}
	after := stagetime.Snapshot()
	total := after[stagetime.Total].Seconds - before[stagetime.Total].Seconds
	var sum float64
	for s := stagetime.Route; s < stagetime.NumStages; s++ {
		d := after[s].Seconds - before[s].Seconds
		sum += d
		t.Logf("%s %.6fs %.3f%%", s, d, 100*d/total)
	}
	if total <= 0 || math.Abs(sum-total) > total*.01 {
		t.Fatalf("stages %.9f total %.9f", sum, total)
	}
	for _, s := range []stagetime.Stage{stagetime.CachePop, stagetime.Publish, stagetime.Acknowledge, stagetime.Other} {
		if after[s].Calls == before[s].Calls {
			t.Fatalf("stage %s absent", s)
		}
	}
}

func sharedTimingWorkload(tb testing.TB, count int) func(int) {
	tb.Helper()
	now := time.Unix(1790000000, 0)
	db, err := store.Open(tb.TempDir(), store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}, TimeBuckets: true, Now: func() time.Time { return now }, SyncInterval: time.Hour, CacheSize: 64 << 20, MemTableSize: 256 << 20, MetadataCacheSize: 128 << 20})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = db.Close() })
	c := cache.New()
	p := NewWhisper(tb.TempDir(), nil, NewWhisperAggregation(), nil, c.PopNotConfirmed, c.Confirm, c.Pop)
	p.SetMetricStore(db)
	names := make([]string, 8)
	for i := range names {
		names[i] = fmt.Sprintf("batch.timing.%d", i)
		if _, err := db.Create(context.Background(), store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 86400}, {Step: 60, Count: 43200}, {Step: 3600, Count: 8760}}, AggregationMethod: store.Average, XFilesFactor: .5}); err != nil {
			tb.Fatal(err)
		}
	}
	var scratch stagetime.Update
	update := func(round int) {
		now = now.Add(time.Duration(count) * time.Second)
		for _, name := range names {
			batch := &points.Points{Metric: name, Data: make([]points.Point, count)}
			for j := range batch.Data {
				batch.Data[j] = points.Point{Timestamp: now.Unix() - int64(count-j), Value: float64((round+j)%97) + .125}
			}
			c.Add(batch)
		}
		p.storeSharedBatchTimed(names, &scratch)
	}
	return update
}
