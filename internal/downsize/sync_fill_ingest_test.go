package downsize

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/buckyd"
	"github.com/go-graphite/go-carbon/carbon"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

type ingestFixture struct {
	b                        *syncLiveBackend
	source, target           *store.Store
	donor, destination, addr string
	writes                   atomic.Int64
	imports                  atomic.Int64
	reads                    atomic.Int64
	accept                   func(*points.Points) bool
}

// The fixture uses the production plaintext receiver, real native stores and
// transfer handlers. Only the protobuf HTTP projection of metadata/Fetch is a
// test adapter. Confirmation reads observe actual UpdateMany persistence.
func newIngestFixture(t *testing.T, continuation bool) *ingestFixture {
	t.Helper()
	f := &ingestFixture{}
	clock := func() time.Time { return time.Unix(testMetricNow, 0) }
	open := func() *store.Store {
		o := store.Options{Now: clock, ExactBlocks: true, TimeBuckets: true, CacheSize: 1 << 20, MemTableSize: 1 << 20, SyncInterval: time.Hour}
		if continuation {
			o.Continuation = &whisperio.ContinuationCodec{}
		}
		s, err := store.Open("/db", o.WithFS(vfs.NewMem()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	}
	f.source, f.target = open(), open()
	service := func(db *store.Store) http.Handler {
		s, err := buckyd.New(buckyd.Config{TmpDir: t.TempDir(), MaxTransfers: 2, MaxBodyBytes: 1 << 20, TransferSources: []string{"127.0.0.1", "localhost"}}, db)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Stop() })
		return s.Handler()
	}
	sourceHandler, targetHandler := service(f.source), service(f.target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, handler := f.target, targetHandler
		if strings.HasPrefix(r.Host, "localhost:") {
			s, handler = f.source, sourceHandler
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		switch r.URL.Path {
		case "/info/":
			var req protov3.MultiGlobRequest
			_ = req.UnmarshalVT(body)
			var resp protov3.MultiMetricsInfoResponse
			for _, name := range req.Metrics {
				m, err := s.Metadata(r.Context(), name)
				if err != nil {
					continue
				}
				out := &protov3.MetricsInfoResponse{Name: name}
				for _, a := range m.Retentions {
					out.Retentions = append(out.Retentions, &protov3.Retention{SecondsPerPoint: int64(a.Step), NumberOfPoints: int64(a.Count)})
				}
				resp.Metrics = append(resp.Metrics, out)
			}
			if len(resp.Metrics) == 0 {
				http.NotFound(w, r)
				return
			}
			out, _ := resp.MarshalVT()
			_, _ = w.Write(out)
		case "/render/":
			f.reads.Add(1)
			var req protov3.MultiFetchRequest
			_ = req.UnmarshalVT(body)
			var resp protov3.MultiFetchResponse
			for _, m := range req.Metrics {
				s, err := s.Fetch(r.Context(), m.Name, int(m.StartTime), int(m.StopTime))
				if err != nil {
					continue
				}
				resp.Metrics = append(resp.Metrics, &protov3.FetchResponse{Name: m.Name, StartTime: int64(s.FromTime), StepTime: int64(s.Step), Values: s.Values})
			}
			out, _ := resp.MarshalVT()
			_, _ = w.Write(out)
		default:
			if s == f.target && r.URL.Path == "/v1/transfer/pull" {
				f.imports.Add(1)
			}
			handler.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(server.Close)
	f.destination = server.URL
	f.donor = strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	f.b = newSyncLiveBackend(syncLiveOptions{carbonPort: mustAtoi(t, port), copyStreams: 2, copyAttempts: 1, hostWait: time.Second})
	t.Cleanup(f.b.client.CloseIdleConnections)
	t.Cleanup(f.b.m.client.CloseIdleConnections)
	f.b.health = newTargetHealthGate(f.destination, .75, .6)
	f.b.health.update(targetHealthSample{limit: 100}, nil)
	// Share the application's registration guard with whole-process tests.
	_ = carbon.New("")
	r, err := receiver.New("sync-ingest-test", map[string]interface{}{"protocol": "tcp", "listen": "127.0.0.1:0", "enabled": true}, func(p *points.Points) {
		if f.accept != nil && !f.accept(p) {
			return
		}
		f.writes.Add(int64(len(p.Data)))
		if err := f.target.UpdateMany(context.Background(), p.Metric, p.Data); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	f.addr = r.(interface{ Addr() net.Addr }).Addr().String()
	return f
}

func seedIngestSnapshot(t *testing.T, db *store.Store, snap store.Snapshot, continuation bool) {
	t.Helper()
	ctx := context.Background()
	if !continuation {
		if _, err := db.CreateFromSnapshot(ctx, snap); err != nil {
			t.Fatal(err)
		}
		return
	}
	snap.Metadata.Continuation = true
	mem := policyMemory{}
	if err := (&whisperio.ContinuationCodec{}).RestoreArchives(ctx, mem, snap, func() time.Time { return time.Unix(testMetricNow, 0) }); err != nil {
		t.Fatal(err)
	}
	record := store.TransferMetric{Config: snap.Metadata.MetricConfig, Version: store.TransferVersion{Name: snap.Metadata.Name, ID: 1, Generation: 1, Revision: 1}, Continuation: true}
	if err := mem.Visit(nil, func(k, v []byte) error {
		record.Keys = append(record.Keys, store.RelayoutKV{Key: bytes.Clone(k), Value: bytes.Clone(v)})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ImportTransfer(ctx, []store.TransferMetric{record}); err != nil {
		t.Fatal(err)
	}
}

func ingestSnapshotBits(t *testing.T, db *store.Store, name string) map[[2]int64]uint64 {
	t.Helper()
	s, err := db.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	out := map[[2]int64]uint64{}
	for ai, a := range s.Archives {
		for _, p := range a.Points {
			out[[2]int64{int64(ai), p.Timestamp}] = math.Float64bits(p.Value)
		}
	}
	return out
}

func TestSyncIngestDifferentialNativeArchiveRouting(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "continuation"}[continuation], func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				ret        []store.Retention
				archive    int
				age        int64
				absent     bool
				nan        bool
				wantIngest bool
			}{
				{"partial", []store.Retention{{Step: 10, Count: 1000}}, 0, 600, false, false, true},
				{"nan", []store.Retention{{Step: 10, Count: 1000}}, 0, 600, false, true, true},
				{"infinite-target", []store.Retention{{Step: 10, Count: 1000}}, 0, 600, false, false, false},
				{"absent", []store.Retention{{Step: 10, Count: 1000}}, 0, 600, true, false, false},
				{"past-archive-zero", []store.Retention{{Step: 10, Count: 1000}, {Step: 60, Count: 1000}}, 1, 12000, false, false, true},
				{"coarse-inside-fine-retention", []store.Retention{{Step: 10, Count: 1000}, {Step: 60, Count: 1000}}, 1, 600, false, false, false},
				{"fine-would-propagate", []store.Retention{{Step: 10, Count: 1000}, {Step: 60, Count: 1000}}, 0, 600, false, false, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var results []map[[2]int64]uint64
					for _, auto := range []bool{false, true} {
						f := newIngestFixture(t, continuation)
						ts := (int64(testMetricNow) - tc.age) / 60 * 60
						cfg := store.MetricConfig{Name: "metric", Retentions: tc.ret, AggregationMethod: store.Average, XFilesFactor: .5}
						donor := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}}
						target := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}}
						for ai, r := range tc.ret {
							base := ts - int64(r.Step)
							a := store.Archive{Retention: r, Points: []store.Point{{Timestamp: base, Value: 12}, {Timestamp: ts, Value: math.Copysign(0, -1)}, {Timestamp: ts + int64(r.Step), Value: math.SmallestNonzeroFloat64}}}
							b := store.Archive{Retention: r, Points: []store.Point{{Timestamp: base, Value: 42}, {Timestamp: ts + int64(r.Step), Value: math.MaxFloat64}}}
							if ai != tc.archive {
								b.Points = append(b.Points, store.Point{Timestamp: ts, Value: 73})
							}
							if tc.nan && ai == tc.archive {
								b.Points = append(b.Points, store.Point{Timestamp: ts, Value: math.Float64frombits(0x7ff8000000001234)})
							}
							if tc.name == "infinite-target" && ai == tc.archive {
								b.Points = append(b.Points, store.Point{Timestamp: ts, Value: math.Inf(1)})
							}
							donor.Archives = append(donor.Archives, a)
							target.Archives = append(target.Archives, b)
						}
						seedIngestSnapshot(t, f.source, donor, continuation)
						if !tc.absent {
							seedIngestSnapshot(t, f.target, target, continuation)
						}
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						path := filepath.Join(t.TempDir(), "fill.jsonl")
						var res syncCopyResult
						var err error
						if auto {
							o := syncIngestOptions{addr: f.addr, now: func() time.Time { return time.Unix(testMetricNow, 0) }, holes: map[string][]syncHole{"metric": {{Name: "metric", Source: f.donor, Step: int64(tc.ret[tc.archive].Step), First: ts, Last: ts, Points: 1}}}}
							res, err = f.b.Fill(ctx, f.donor, f.destination, []string{"metric"}, path, o)
						} else {
							res, err = f.b.Copy(ctx, f.donor, f.destination, []string{"metric"}, path)
						}
						cancel()
						if err != nil || res.Copied != 1 || len(res.Failed) > 0 {
							t.Fatalf("auto=%v res=%+v err=%v", auto, res, err)
						}
						if auto && (f.writes.Load() > 0) != tc.wantIngest {
							t.Fatalf("ingest=%d expected %v", f.writes.Load(), tc.wantIngest)
						}
						results = append(results, ingestSnapshotBits(t, f.target, "metric"))
					}
					if !reflect.DeepEqual(results[0], results[1]) {
						t.Fatalf("transfer %v != auto %v", results[0], results[1])
					}
				})
			}
		})
	}
}

func TestSyncIngestResumeOldFillJournal(t *testing.T) {
	f := newIngestFixture(t, false)
	ctx := context.Background()
	names := []string{"first", "second"}
	ts := int64(testMetricNow) - 600
	for _, name := range names {
		cfg := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 1000}}, AggregationMethod: store.Sum}
		for _, db := range []*store.Store{f.source, f.target} {
			if _, err := db.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.source.UpdateMany(ctx, name, []store.Point{{Timestamp: ts, Value: 123.00000000000001}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.target.UpdateMany(ctx, "first", []store.Point{{Timestamp: ts, Value: 123.00000000000001}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fill.jsonl")
	input, err := syncCopyInput(path, names)
	if err != nil {
		t.Fatal(err)
	}
	id, err := copyIdentity(f.b.copyOptions(f.donor, f.destination, input, path, 2))
	if err != nil {
		t.Fatal(err)
	}
	j, err := openCopyJournal(path, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.done("", "first", "first", 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	j.close()
	o := syncIngestOptions{addr: f.addr, now: func() time.Time { return time.Unix(testMetricNow, 0) }, holes: map[string][]syncHole{}}
	for _, name := range names {
		o.holes[name] = []syncHole{{Name: name, Source: f.donor, Step: 10, First: ts, Last: ts, Points: 1}}
	}
	for attempt := range 2 {
		res, err := f.b.Fill(ctx, f.donor, f.destination, names, path, o)
		if err != nil || res.Copied != 2 {
			t.Fatalf("%+v %v", res, err)
		}
		if f.writes.Load() != 1 {
			t.Fatalf("attempt %d rewrote finished work: %d", attempt, f.writes.Load())
		}
	}
	if f.imports.Load() != 0 {
		t.Fatal("used old transfer for unfinished eligible name")
	}
}

func TestSyncIngestReceiptWaitsForAcceptance(t *testing.T) {
	f := newIngestFixture(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.accept = func(*points.Points) bool { cancel(); return false }
	for _, db := range []*store.Store{f.source, f.target} {
		if _, err := db.Create(context.Background(), store.MetricConfig{Name: "metric", Retentions: []store.Retention{{Step: 10, Count: 1000}}, AggregationMethod: store.Sum}); err != nil {
			t.Fatal(err)
		}
	}
	ts := int64(testMetricNow) - 600
	if err := f.source.UpdateMany(context.Background(), "metric", []store.Point{{Timestamp: ts, Value: 1}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "fill.jsonl")
	o := syncIngestOptions{addr: f.addr, now: func() time.Time { return time.Unix(testMetricNow, 0) }, holes: map[string][]syncHole{"metric": {{Source: f.donor, Step: 10, First: ts, Last: ts}}}}
	o.windowOnly, o.windows = true, syncRanges{{From: ts, Until: ts + 10}}
	if _, err := f.b.Fill(ctx, f.donor, f.destination, []string{"metric"}, path, o); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path + ".ingest")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"kind":"done"`) {
		t.Fatal("unaccepted socket write got a done receipt")
	}
	counts, err := collectWindowPointCounts(filepath.Dir(path))
	if err != nil || counts.Unknown != 1 || counts.Added != nil {
		t.Fatalf("interrupted replay count %+v: %v", counts, err)
	}
}

func TestSyncIngestHealthPauseAndOverflowStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newTargetHealthGate("target", .75, .60)
		g.update(targetHealthSample{points: 75, limit: 100}, nil)
		var writes atomic.Int64
		go func() {
			if g.waitIngest(context.Background()) == nil {
				writes.Add(1)
			}
		}()
		synctest.Wait()
		g.update(targetHealthSample{points: 60, limit: 100}, nil)
		synctest.Wait()
		if writes.Load() != 0 {
			t.Fatal("admitted before low watermark")
		}
		g.update(targetHealthSample{points: 59, limit: 100}, nil)
		synctest.Wait()
		if writes.Load() != 1 {
			t.Fatal("did not resume")
		}
		g.update(targetHealthSample{points: 1, limit: 100, dropped: 1}, nil)
		g.update(targetHealthSample{points: 1, limit: 100, dropped: 1}, nil)
		if err := g.waitIngest(context.Background()); err == nil {
			t.Fatal("overflow stop was not sticky")
		}
	})
}

func TestSyncIngestOperationalFlagsOldConfig(t *testing.T) {
	c, err := parseSyncCommand([]string{"--source", syncD1, "--to", syncT, "--state", t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.saveConfig(); err != nil {
		t.Fatal(err)
	}
	// Remove new fields to model today's saved config exactly.
	delete(c.config.Flags, "fill-path")
	delete(c.config.Flags, "fill-ingest")
	if err := atomicStateJSON(filepath.Join(c.sync.state, "config.json"), c.config); err != nil {
		t.Fatal(err)
	}
	next, err := parseSyncCommand([]string{"--resume", c.sync.state})
	if err != nil || next.sync.fillPath != "auto" {
		t.Fatal(next.sync.fillPath, err)
	}
	before, _ := syncIdentity(next.sync)
	next, err = parseSyncCommand([]string{"--resume", c.sync.state, "--fill-path=transfer", "--fill-ingest=127.0.0.1:2013"})
	after, _ := syncIdentity(next.sync)
	if err != nil || before != after {
		t.Fatal("transport changed data identity", err)
	}
}

type switchingFillBackend struct {
	*interruptAfterFill
	ingestCalls int
}

func (b *switchingFillBackend) Copy(ctx context.Context, source, target string, names []string, path string) (syncCopyResult, error) {
	if err := ctx.Err(); err != nil {
		return syncCopyResult{}, err
	}
	return b.interruptAfterFill.Copy(ctx, source, target, names, path)
}

func (b *switchingFillBackend) Fill(ctx context.Context, source, target string, names []string, path string, _ syncIngestOptions) (syncCopyResult, error) {
	b.ingestCalls++
	return b.fakeSync.Copy(ctx, source, target, names, path)
}

func TestSyncIngestResumesOldFillPhase(t *testing.T) {
	f := &switchingFillBackend{interruptAfterFill: &interruptAfterFill{fakeSync: newFakeSync()}}
	for i, source := range []string{syncD1, syncD2} {
		name := []string{"first", "second"}[i]
		for _, host := range []string{source, syncT} {
			f.put(host, name, []syncArchive{{3600, 240}})
		}
		delete(f.store(syncT).points[name][3600], syncTestNow-5*3600)
	}
	o := syncOptions{fillPath: "transfer", to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir(), batch: 2, streams: 1, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.cancel = cancel
	if err := runSync(ctx, o, f); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(f.copies) != 1 || f.ingestCalls != 0 {
		t.Fatal(f.copies, f.ingestCalls)
	}
	o.fillPath = "auto"
	if err := runSync(context.Background(), o, f); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != 2 || f.ingestCalls != 1 {
		t.Fatal("finished source replayed or new fill path unused", f.copies, f.ingestCalls)
	}
}

func TestSyncIngestWindowDifferentialAndFallbackResume(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "continuation"}[continuation], func(t *testing.T) {
			var results []map[string]map[[2]int64]uint64
			for _, auto := range []bool{false, true} {
				f := newIngestFixture(t, continuation)
				ts := int64(testMetricNow) - 600
				names := []string{"absent", "partial"}
				for _, name := range names {
					cfg := store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 10, Count: 1000}}, AggregationMethod: store.Sum}
					src := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: cfg.Retentions[0], Points: []store.Point{{Timestamp: ts - 100, Value: math.MaxFloat64}, {Timestamp: ts, Value: math.Copysign(0, -1)}, {Timestamp: ts + 10, Value: 12}}}}}
					seedIngestSnapshot(t, f.source, src, continuation)
					if name == "partial" {
						dst := src
						dst.Archives = []store.Archive{{Retention: cfg.Retentions[0], Points: []store.Point{{Timestamp: ts + 10, Value: 99}}}}
						seedIngestSnapshot(t, f.target, dst, continuation)
					}
				}
				o := syncIngestOptions{addr: f.addr, windowOnly: true, windows: syncRanges{{From: ts - 1, Until: ts + 20}}, now: func() time.Time { return time.Unix(testMetricNow, 0) }, holes: map[string][]syncHole{}}
				for _, name := range names {
					o.holes[name] = []syncHole{{Source: f.donor, Step: 10, First: ts, Last: ts}}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				path := filepath.Join(t.TempDir(), "fill.jsonl")
				var err error
				if auto {
					_, err = f.b.Fill(ctx, f.donor, f.destination, names, path, o)
					if err == nil {
						before := f.writes.Load()
						_, err = f.b.Fill(ctx, f.donor, f.destination, names, path, o)
						if f.writes.Load() != before {
							t.Fatal("window resume replayed completed ingest")
						}
					}
				} else {
					_, err = f.b.CopyWindow(ctx, f.donor, f.destination, names, path, o.windows)
				}
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				if auto {
					counts, err := collectWindowPointCounts(filepath.Dir(path))
					if err != nil || counts.Added == nil || *counts.Added != 3 || counts.Unknown != 0 {
						t.Fatalf("window replay count %+v: %v", counts, err)
					}
				}
				out := map[string]map[[2]int64]uint64{}
				for _, name := range names {
					out[name] = ingestSnapshotBits(t, f.target, name)
				}
				results = append(results, out)
			}
			if !reflect.DeepEqual(results[0], results[1]) {
				t.Fatalf("transfer %v != auto %v", results[0], results[1])
			}
		})
	}
}
