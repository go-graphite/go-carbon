package persister

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

type mirrorFailureStore struct {
	*store.Store
	err error
}

type boundaryMirrorStore struct {
	*store.Store
	replacements int
}

func (s *boundaryMirrorStore) Replace(ctx context.Context, snapshot store.Snapshot) (store.Metadata, error) {
	s.replacements++
	return s.Store.Replace(ctx, snapshot)
}

func TestMirrorArchiveCorrectionsAcrossStoreClockBoundary(t *testing.T) {
	now := time.Unix(time.Now().Unix()/60*60, 0)
	previous := whisper.Now
	whisper.Now = func() time.Time { return now }
	defer func() { whisper.Now = previous }()
	root := t.TempDir()
	cache := &fakeCache{}
	p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
	p.schemas[0].Retentions = whisper.MustParseRetentionDefs("1s:2m,5s:10m")
	cross := false
	db, err := openTestStore(t, filepath.Join(root, "exact"), store.Options{ExactBlocks: true, Now: func() time.Time {
		if cross {
			now = now.Add(time.Second)
			cross = false
		}
		return now
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	wrapped := &boundaryMirrorStore{Store: db}
	p.SetMetricStore(wrapped)
	p.SetMirrorFiles(true)
	cache.add("boundary", now.Unix()-1, 1)
	p.store("boundary")
	cache.add("boundary", now.Unix()-120, 111.25)
	cross = true
	p.store("boundary")
	if wrapped.replacements != 0 || cache.confirmedPoints() != 2 {
		t.Fatalf("repair=%d confirmed=%d", wrapped.replacements, cache.confirmedPoints())
	}
	want, err := whisperio.ReadSnapshot(context.Background(), "boundary", filepath.Join(root, "original", "boundary.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(context.Background(), "boundary")
	if err != nil {
		t.Fatal(err)
	}
	for i := range want.Archives {
		a, b := want.Archives[i], got.Archives[i]
		if a.Retention != b.Retention || len(a.Points) != len(b.Points) {
			t.Fatalf("retention admission differs after crossing second: original=%+v shared=%+v", want.Archives[i], got.Archives[i])
		}
		for j, point := range a.Points {
			if point.Timestamp != b.Points[j].Timestamp || math.Float64bits(point.Value) != math.Float64bits(b.Points[j].Value) {
				t.Fatalf("retention boundary point differs: original=%+v shared=%+v", point, b.Points[j])
			}
		}
	}
}

func (s mirrorFailureStore) UpdateMany(ctx context.Context, name string, input []points.Point) error {
	if s.err != nil {
		return s.err
	}
	return s.Store.UpdateMany(ctx, name, input)
}

func (s mirrorFailureStore) UpdateManyWithArchiveValues(ctx context.Context, name string, input []points.Point, values []store.ArchiveValue) error {
	if s.err != nil {
		return s.err
	}
	return s.Store.UpdateManyWithArchiveValues(ctx, name, input, values)
}

func TestMirrorFailureRepairSurvivesPersisterReplacement(t *testing.T) {
	now := time.Unix(time.Now().Unix()/60*60, 0)
	previous := whisper.Now
	whisper.Now = func() time.Time { return now }
	defer func() { whisper.Now = previous }()
	root := t.TempDir()
	cache := &fakeCache{}
	db, err := openTestStore(t, filepath.Join(root, "exact"), store.Options{ExactBlocks: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	newWriter := func(state *MirrorState) *Whisper {
		p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
		p.schemas[0].Retentions = whisper.MustParseRetentionDefs("1s:2m,5s:10m")
		p.SetMirrorFiles(true)
		if state != nil {
			p.SetMirrorState(state)
		}
		p.SetMetricStore(db)
		return p
	}
	p := newWriter(nil)
	cache.add("retry", now.Unix()-1, 1)
	p.store("retry")
	p.SetMetricStore(mirrorFailureStore{db, errors.New("injected commit failure")})
	cache.add("retry", now.Unix()-120, 111.25)
	p.store("retry")
	if cache.confirmedPoints() != 1 {
		t.Fatal("failed write confirmed")
	}
	now = now.Add(time.Second)
	replacement := newWriter(p.mirrorState)
	wrapped := &boundaryMirrorStore{Store: db}
	replacement.SetMetricStore(wrapped)
	replacement.store("retry")
	if wrapped.replacements != 1 || cache.confirmedPoints() != 2 {
		t.Fatalf("repair=%d confirmed=%d", wrapped.replacements, cache.confirmedPoints())
	}
	want, err := whisperio.ReadSnapshot(context.Background(), "retry", filepath.Join(root, "original", "retry.wsp"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Snapshot(context.Background(), "retry")
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range want.Archives {
		b := got.Archives[i]
		if a.Retention != b.Retention || len(a.Points) != len(b.Points) {
			t.Fatalf("archive %d differs after retry", i)
		}
		for j, point := range a.Points {
			if point.Timestamp != b.Points[j].Timestamp || math.Float64bits(point.Value) != math.Float64bits(b.Points[j].Value) {
				t.Fatalf("archive %d point %d differs", i, j)
			}
		}
	}
}

func TestMirrorKeepsRollbackCurrent(t *testing.T) {
	if storageClockMatrix(t) {
		return
	}
	now := time.Unix(time.Now().Unix()/60*60, 0)
	previous := whisper.Now
	whisper.Now = func() time.Time { return now }
	defer func() { whisper.Now = previous }()
	ctx := context.Background()
	for _, method := range []whisper.AggregationMethod{whisper.Average, whisper.Sum, whisper.First, whisper.Last, whisper.Min, whisper.Max} {
		for _, xff := range []float32{0, 0.5, 1} {
			t.Run(method.String()+"/"+fmtFloat(xff), func(t *testing.T) {
				root := t.TempDir()
				cache := &fakeCache{}
				p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
				p.schemas[0].Retentions = whisper.MustParseRetentionDefs("1s:2m,5s:10m,60s:1h")
				p.aggregation.Default.aggregationMethod = method
				p.aggregation.Default.xFilesFactor = float64(xff)
				dir := filepath.Join(root, "exact")
				db, err := openTestStore(t, dir, store.Options{ExactBlocks: true, Now: func() time.Time { return now }})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				p.SetMetricStore(db)
				p.SetMirrorFiles(true)
				metric := "canary.rollback"
				path := filepath.Join(root, "original", "canary", "rollback.wsp")
				assertEqual := func() {
					t.Helper()
					want, err := whisperio.ReadSnapshot(ctx, metric, path)
					if err != nil {
						t.Fatal(err)
					}
					got, err := db.Snapshot(ctx, metric)
					if err != nil {
						t.Fatal(err)
					}
					for i, archive := range want.Archives {
						if len(archive.Points) != len(got.Archives[i].Points) {
							t.Fatalf("archive %d count: original=%d shared=%d", i, len(archive.Points), len(got.Archives[i].Points))
						}
						for j, point := range archive.Points {
							actual := got.Archives[i].Points[j]
							if point.Timestamp != actual.Timestamp || math.Float64bits(point.Value) != math.Float64bits(actual.Value) {
								t.Fatalf("archive %d point %d: original=%+v shared=%+v", i, j, point, actual)
							}
						}
					}
				}
				for ts := now.Unix() - 100; ts < now.Unix()-20; ts++ {
					cache.add(metric, ts, float64(ts%13))
				}
				p.store(metric)
				assertEqual()
				for _, batch := range [][]points.Point{
					{{Timestamp: now.Unix() - 70, Value: 111.25}, {Timestamp: now.Unix() - 95, Value: 7}, {Timestamp: now.Unix() - 70, Value: 222.5}},
					{{Timestamp: now.Unix() - 19, Value: math.Float64frombits(1 << 63)}, {Timestamp: now.Unix() - 18, Value: math.SmallestNonzeroFloat64}, {Timestamp: now.Unix() - 17, Value: math.MaxFloat64}},
					{{Timestamp: now.Unix() - 70, Value: 3}, {Timestamp: now.Unix() - 16, Value: 10}},
				} {
					for _, point := range batch {
						cache.add(metric, point.Timestamp, point.Value)
					}
					p.store(metric)
					assertEqual()
				}
				// A shared commit failure must retain the cache batch, while the
				// original already contains the write needed by stock rollback.
				p.SetMetricStore(mirrorFailureStore{db, errors.New("injected shared failure")})
				cache.add(metric, now.Unix()-69, 111.25)
				confirmed := cache.confirmedPoints()
				p.store(metric)
				if cache.confirmedPoints() != confirmed || cache.notConfirmedPoints() != 0 {
					t.Fatal("failed shared commit confirmed or pinned points")
				}
				if cache.data[metric] == nil {
					t.Fatal("failed shared batch was not requeued")
				}
				p.SetMetricStore(db)
				p.store(metric)
				assertEqual()
				if err := db.Compact(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = openTestStore(t, dir, store.Options{ExactBlocks: true, Now: func() time.Time { return now }})
				if err != nil {
					t.Fatal(err)
				}
				assertEqual()
				// A stock file-only persister can continue from the same tree.
				p.SetMetricStore(nil)
				cache.add(metric, now.Unix()-68, 333.5)
				p.store(metric)
				w, err := whisper.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				series, err := w.Fetch(int(now.Unix()-69), int(now.Unix()-67))
				if err != nil {
					t.Fatal(err)
				}
				_ = w.Close()
				if !reflect.DeepEqual(series.Values(), []float64{333.5, float64((now.Unix() - 67) % 13)}) {
					t.Fatalf("stock rollback values=%v", series.Values())
				}
			})
		}
	}
}

func fmtFloat(value float32) string {
	if value == 0 {
		return "xff0"
	}
	if value == 1 {
		return "xff1"
	}
	return "xff0.5"
}

func TestMirrorOpenFailureDoesNotPop(t *testing.T) {
	root := t.TempDir()
	blocked := filepath.Join(root, "blocked")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	cache := &fakeCache{}
	p := newOOOTestPersister(t, blocked, cache)
	db, err := openTestStore(t, filepath.Join(root, "exact"), store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p.SetMetricStore(db)
	p.SetMirrorFiles(true)
	cache.add("canary.failed", time.Now().Unix()-1, 7)
	p.store("canary.failed")
	if cache.confirmedPoints() != 0 || cache.notConfirmedPoints() != 0 || cache.data["canary.failed"] == nil {
		t.Fatal("mirror open failure lost or confirmed points")
	}
}
