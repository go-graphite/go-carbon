package persister

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
)

// Hooks surround real store operations; no storage result is simulated here.
type transferLocalStore struct {
	*store.Store
	before, after func()
	result        error
}

func (s *transferLocalStore) UpdateBatch(ctx context.Context, values []store.MetricUpdate, m *store.ContinuationMaintenance) error {
	if s.before != nil {
		s.before()
	}
	s.result = s.Store.UpdateBatch(ctx, values, m)
	if s.after != nil {
		s.after()
	}
	return s.result
}

type transferLocalCodec struct {
	whisperio.ContinuationCodec
	before func()
}

func (c *transferLocalCodec) Update(ctx context.Context, io store.ContinuationIO, m store.Metadata, p []store.Point, r int, now func() time.Time, maintenance *store.ContinuationMaintenance) error {
	if c.before != nil {
		c.before()
	}
	return c.ContinuationCodec.Update(ctx, io, m, p, r, now, maintenance)
}

// An HTTP donor export, real cache ownership and real Pebble/Whisper updates.
// Every pop and confirmation boundary, each member's preparation and the commit
// boundary are exercised without sleeps. The absent cached name reproduces the
// same eight-failure symptom without any request cancellation.
func TestTransferFlushLocalInterleavings(t *testing.T) {
	stages := []string{"before_request", "before_update", "after_commit", "absent_cached", "absent_unqueued", "success"}
	for i := 1; i <= 8; i++ {
		stages = append(stages, fmt.Sprintf("pop_%d", i), fmt.Sprintf("prepare_%d", i), fmt.Sprintf("confirm_%d", i))
	}
	for _, deadline := range []bool{false, true} {
		for _, stage := range stages {
			t.Run(fmt.Sprintf("deadline=%v/%s", deadline, stage), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if deadline {
					ctx = transferDeadlineContext{ctx}
				}
				codec := &transferLocalCodec{}
				now := time.Unix(1_790_000_000, 0)
				opts := store.Options{ExactBlocks: true, Continuation: codec, Now: func() time.Time { return now }, CacheSize: 1 << 20, MemTableSize: 1 << 20}
				db, err := store.Open(t.TempDir(), opts)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				controlOpts := opts
				controlOpts.Continuation = &whisperio.ContinuationCodec{}
				control, err := store.Open(t.TempDir(), controlOpts)
				if err != nil {
					t.Fatal(err)
				}
				defer control.Close()
				core, oracle := cache.New(), cache.New()
				p := newOOOTestPersister(t, t.TempDir(), &fakeCache{})
				p.pop = core.PopNotConfirmed
				p.confirm = core.Confirm
				p.SetRequeue(core.Requeue)
				cp := newOOOTestPersister(t, t.TempDir(), &fakeCache{})
				cp.pop = oracle.PopNotConfirmed
				cp.confirm = oracle.Confirm
				cp.SetRequeue(oracle.Requeue)
				cp.SetMetricStore(control)
				wrapped := &transferLocalStore{Store: db}
				p.SetMetricStore(wrapped)
				names := make([]string, 8)
				for i := range names {
					names[i] = fmt.Sprintf("transfer.%d", i)
					config := store.MetricConfig{Name: names[i], Retentions: []store.Retention{{Step: 1, Count: 7200}}, AggregationMethod: store.Average}
					if stage != "absent_cached" || i != 7 {
						if _, err := db.Create(context.Background(), config); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := control.Create(context.Background(), config); err != nil {
						t.Fatal(err)
					}
					batch := &points.Points{Metric: names[i], Data: []points.Point{{Timestamp: now.Unix() - 3, Value: float64(i) + 0.125}, {Timestamp: now.Unix() - 2, Value: float64(i) + 0.5}}}
					core.Add(batch)
					oracle.Add(batch)
				}
				pops, prepares, confirms := 0, 0, 0
				p.pop = func(name string) (*points.Points, bool) {
					v, ok := core.PopNotConfirmed(name)
					if ok {
						pops++
						if stage == fmt.Sprintf("pop_%d", pops) {
							cancel()
						}
					}
					return v, ok
				}
				p.confirm = func(v *points.Points) {
					confirms++
					if stage == fmt.Sprintf("confirm_%d", confirms) {
						cancel()
					}
					core.Confirm(v)
				}
				wrapped.before = func() {
					if stage == "before_update" {
						// New input arrives while the old batches are in flight.
						done := make(chan struct{})
						go func() {
							defer close(done)
							for _, name := range names {
								v := points.OnePoint(name, 99, now.Unix()-2)
								core.Add(v)
								oracle.Add(v)
							}
						}()
						<-done
						cancel()
					}
				}
				wrapped.after = func() {
					if stage == "after_commit" {
						cancel()
					}
				}
				codec.before = func() {
					prepares++
					if stage == fmt.Sprintf("prepare_%d", prepares) {
						cancel()
					}
				}
				service, err := buckyd.New(buckyd.Config{TmpDir: t.TempDir()}, db)
				if err != nil {
					t.Fatal(err)
				}
				service.SetTransferFlush(p.FlushForTransfer)
				requestNames := append([]string(nil), names...)
				if stage == "absent_unqueued" {
					requestNames = append(requestNames, "absent")
				}
				body, _ := json.Marshal(requestNames)
				if stage == "before_request" {
					cancel()
				}
				request := httptest.NewRequest(http.MethodPost, "/v1/transfer/export?wire=2", bytes.NewReader(body)).WithContext(ctx)
				response := httptest.NewRecorder()
				service.Handler().ServeHTTP(response, request)
				stats, _ := p.WriteStats()
				t.Logf("HTTP=%d UpdateBatch=%v failures=%d pops=%d confirmed=%d queued=%d in-flight=%d", response.Code, wrapped.result, stats.Failures, pops, confirms, core.Size(), core.NotConfirmedLength())
				if core.NotConfirmedLength() != 0 {
					t.Fatal("orphaned popped points")
				}
				if wrapped.result != nil {
					reason := "canceled"
					if deadline {
						reason = "deadline"
					}
					if errors.Is(wrapped.result, store.ErrNotFound) {
						reason = "absent"
					}
					if got := p.TransferFlushRequeues()[reason]; got != 1 {
						t.Fatalf("requeue groups %s=%d", reason, got)
					}
					if !errors.Is(wrapped.result, context.Canceled) && !errors.Is(wrapped.result, context.DeadlineExceeded) && !errors.Is(wrapped.result, store.ErrNotFound) {
						t.Fatal(wrapped.result)
					}
					queued := int64(16)
					if stage == "before_update" {
						queued = 24
					}
					if stats.Failures != 0 || confirms != 0 || core.Size() != queued {
						t.Fatalf("failed group accounting: %+v confirms=%d queued=%d", stats, confirms, core.Size())
					}
				} else {
					for _, n := range p.TransferFlushRequeues() {
						if n != 0 {
							t.Fatal("requeued a committed or untouched group")
						}
					}
					if stats.Failures != 0 {
						t.Fatal(stats)
					}
				}
				// New input during a failed flush must follow the older requeued batch.
				// Same timestamp correction detects a reversed merge as well as lost input.
				if wrapped.result != nil && stage != "before_update" {
					for _, name := range names {
						v := points.OnePoint(name, 99, now.Unix()-2)
						core.Add(v)
						oracle.Add(v)
					}
				}
				// Ordinary persistence creates an absent metric and drains retries exactly once.
				codec.before = nil
				wrapped.before = nil
				wrapped.after = nil
				for _, name := range names {
					p.store(name)
					cp.store(name)
				}
				if core.Size() != 0 || core.NotConfirmedLength() != 0 {
					t.Fatal("retry did not drain")
				}
				for _, name := range names {
					got, err := db.Fetch(context.Background(), name, int(now.Unix()-10), int(now.Unix()))
					if err != nil {
						t.Fatal(err)
					}
					want, err := control.Fetch(context.Background(), name, int(now.Unix()-10), int(now.Unix()))
					if err != nil {
						t.Fatal(err)
					}
					gb, _ := json.Marshal(got)
					wb, _ := json.Marshal(want)
					if !bytes.Equal(gb, wb) {
						t.Fatalf("render differs: %s\n%s\n%s", name, gb, wb)
					}
					gm, _ := db.Metadata(context.Background(), name)
					wm, _ := control.Metadata(context.Background(), name)
					if gm.Revision != wm.Revision {
						t.Fatalf("duplicate update %s revisions %d/%d", name, gm.Revision, wm.Revision)
					}
				}
			})
		}
	}
}

// Done is controlled deterministically; Err has the deadline semantics a real
// server deadline supplies, with no timing-dependent test sleeps.
type transferDeadlineContext struct{ context.Context }

func (c transferDeadlineContext) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}
