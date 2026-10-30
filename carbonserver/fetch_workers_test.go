package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

func fetchWorkersFixture(t testing.TB, count int) (*CarbonserverListener, []string, []bool) {
	t.Helper()
	const now = 1790000000
	db, err := store.Open(t.TempDir(), store.Options{
		ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{},
		SyncInterval: time.Hour, CacheSize: 4 << 20, MemTableSize: 1 << 20,
		Now: func() time.Time { return time.Unix(now, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	l := NewCarbonserverListener(func(string) []points.Point { return nil })
	l.logger = zap.NewNop()
	l.SetMetricStore(db)
	files, leafs := make([]string, count), make([]bool, count)
	input := make([]store.Point, 128)
	for i := range input {
		input[i] = store.Point{Timestamp: now - 128 + int64(i), Value: float64(i) + .1}
	}
	for i := range files {
		files[i], leafs[i] = fmt.Sprintf("fetch.metric%d", i), true
		if _, err := db.Create(context.Background(), store.MetricConfig{
			Name: files[i], Retentions: []store.Retention{{Step: 1, Count: 600}}, AggregationMethod: store.Average,
		}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(context.Background(), files[i], input); err != nil {
			t.Fatal(err)
		}
	}
	return l, files, leafs
}

func TestFetchWorkersMatchSerial(t *testing.T) {
	l, files, leafs := fetchWorkersFixture(t, 17)
	files = append(files, "missing", "directory", files[0])
	leafs = append(leafs, true, false, true)
	var want [][]byte
	for i, name := range files {
		if leafs[i] {
			r, err := l.fetchSingleMetric(name, "alias", 1789999872, 1790000000)
			if err == nil {
				data, err := r.proto2().MarshalVT()
				if err != nil {
					t.Fatal(err)
				}
				want = append(want, data)
			}
		}
	}
	slices.SortFunc(want, bytes.Compare)
	for _, limit := range []int{0, 1, 3, 32} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			l.maxFetchDataGoroutines = limit
			for _, n := range []int{0, 1, len(files)} {
				got, err := l.fetchData("fetch.*", "alias", files[:n], leafs[:n], nil, 1789999872, 1790000000)
				if err != nil {
					t.Fatal(err)
				}
				var wire [][]byte
				for _, r := range got {
					data, err := r.proto2().MarshalVT()
					if err != nil {
						t.Fatal(err)
					}
					wire = append(wire, data)
				}
				if n == len(files) {
					slices.SortFunc(wire, bytes.Compare)
					if !slices.EqualFunc(wire, want, bytes.Equal) {
						t.Fatal("parallel fetch differs from serial output")
					}
				} else if len(got) != n {
					t.Fatalf("got %d responses, want %d", len(got), n)
				}
			}
		})
	}
}

func TestFetchWorkersBoundConcurrency(t *testing.T) {
	l, files, leafs := fetchWorkersFixture(t, 12)
	l.maxFetchDataGoroutines = 3
	var mu sync.Mutex
	var active, peak, calls int
	entered, release := make(chan struct{}, 3), make(chan struct{})
	l.cacheGet = func(string) []points.Point {
		mu.Lock()
		calls++
		active++
		peak = max(peak, active)
		mu.Unlock()
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		mu.Lock()
		active--
		mu.Unlock()
		return nil
	}
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan int, 1)
	go func() {
		got, _ := l.fetchData("fetch.*", "", files, leafs, nil, 1789999872, 1790000000)
		done <- len(got)
	}()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for range 3 {
		select {
		case <-entered:
		case <-timer.C:
			t.Fatal("fetch workers did not start")
		}
	}
	once.Do(func() { close(release) })
	select {
	case n := <-done:
		if n != len(files) || calls != n || peak != 3 {
			t.Fatalf("responses=%d calls=%d peak=%d", n, calls, peak)
		}
	case <-timer.C:
		t.Fatal("fetch workers did not finish")
	}
}

func BenchmarkFetchWorkers(b *testing.B) {
	for _, count := range []int{1, 32, 256} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			l, files, leafs := fetchWorkersFixture(b, count)
			l.maxFetchDataGoroutines = 8
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					got, err := l.fetchData("fetch.*", "", files, leafs, nil, 1789999872, 1790000000)
					if err != nil || len(got) != count {
						b.Fatal("incomplete fetch", len(got), err)
					}
				}
			})
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/metric")
		})
	}
}
