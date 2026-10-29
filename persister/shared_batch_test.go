package persister

import (
	"context"
	"errors"
	"math"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
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
