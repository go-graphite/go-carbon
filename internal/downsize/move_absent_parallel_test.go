package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParallelAbsentProofsBoundAcrossBatchesAndIndependentSources(t *testing.T) {
	t.Setenv("PARALLEL_ABSENCE_TOKEN", "proof-token")
	m := &mover{client: &http.Client{Timeout: time.Second}, options: options{tokenEnv: "PARALLEL_ABSENCE_TOKEN"}}
	var releaseOnce sync.Once
	release := make(chan struct{})
	defer releaseOnce.Do(func() { close(release) })
	var servers []*httptest.Server
	var counters [2]struct{ active, peak, heads, catalogs atomic.Int32 }
	entered := [2]chan struct{}{make(chan struct{}, 128), make(chan struct{}, 128)}
	for i := range 2 {
		index := i
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c := &counters[index]
			current := c.active.Add(1)
			defer c.active.Add(-1)
			for old := c.peak.Load(); current > old && !c.peak.CompareAndSwap(old, current); old = c.peak.Load() {
			}
			if r.Header.Get("X-Buckyd-Authorization") != "proof-token" || r.Header.Get("Cache-Control") != "no-cache, no-store" {
				t.Error("missing authenticated fresh proof request")
			}
			if r.Method == http.MethodHead {
				c.heads.Add(1)
				entered[index] <- struct{}{}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				w.WriteHeader(http.StatusNotFound)
				return
			}
			c.catalogs.Add(1)
			if r.URL.Path != "/v1/transfer/list" || !strings.HasPrefix(r.URL.Query().Get("prefix"), "metric.") || r.URL.Query().Get("limit") != "1" {
				t.Error("wrong exact-prefix proof")
			}
			_, _ = w.Write([]byte(`[]`))
		}))
		servers = append(servers, server)
		defer server.Close()
	}
	var jobs sync.WaitGroup
	for _, server := range servers {
		for batch := range 3 {
			names := []string{}
			for i := range 16 {
				names = append(names, fmt.Sprintf("metric.%d.%d", batch, i))
			}
			jobs.Add(1)
			go func(source string, names []string) {
				defer jobs.Done()
				absent, err := m.parallelAbsentSourceProofs(context.Background(), source, names)
				if err != nil || !reflect.DeepEqual(absent, names) {
					t.Error("proof order/completeness", err, len(absent))
				}
			}(server.URL, names)
		}
	}
	for host := range 2 {
		for range maxAbsentProofsPerSource {
			select {
			case <-entered[host]:
			case <-time.After(time.Second):
				releaseOnce.Do(func() { close(release) })
				jobs.Wait()
				t.Fatal("source did not run eight proofs concurrently", host)
			}
		}
		select {
		case <-entered[host]:
			releaseOnce.Do(func() { close(release) })
			jobs.Wait()
			t.Fatal("batch workers multiplied the per-source budget", host)
		case <-time.After(10 * time.Millisecond):
		}
	}
	releaseOnce.Do(func() { close(release) })
	jobs.Wait()
	for i := range 2 {
		c := &counters[i]
		if c.peak.Load() != 8 || c.heads.Load() != 48 || c.catalogs.Load() != 48 || c.active.Load() != 0 {
			t.Fatal("source budget/proof pairing", i, c.peak.Load(), c.heads.Load(), c.catalogs.Load(), c.active.Load())
		}
	}
}

func parallelAbsentFixture(t *testing.T, handler http.HandlerFunc) (*mover, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	j, err := openJournal(filepath.Join(t.TempDir(), "state.jsonl"), "same-plan")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(j.close)
	j.maxSpeed = true
	return &mover{client: server.Client(), journal: j}, server
}

func TestParallelAbsentProofsPreserveOrderedDurableNamesAndFreshPairing(t *testing.T) {
	var heads sync.Map
	m, server := parallelAbsentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			name := strings.TrimPrefix(r.URL.Path, "/metrics/")
			if strings.HasPrefix(name, "gone.") {
				if name == "gone.z" {
					time.Sleep(15 * time.Millisecond)
				}
				heads.Store(name, true)
				w.WriteHeader(404)
			}
			return
		}
		name := r.URL.Query().Get("prefix")
		if _, ok := heads.Load(name); !ok {
			t.Error("catalog proof preceded its own HEAD404", name)
		}
		if name == "gone.a" {
			_, _ = w.Write([]byte(`["gone.a.longer"]`))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	})
	names := []string{"present", "gone.z", "gone.a", "gone.escaped/name?+", "another.present"}
	batch := task{Source: server.URL, Stream: "stream", Start: 100, End: 105, Names: names}
	if err := m.skipAbsentSource(context.Background(), batch, names); err != nil {
		t.Fatal(err)
	}
	want := []string{"gone.z", "gone.a", "gone.escaped/name?+"}
	saved := m.journal.absent[absentSourceSkip{Source: batch.Source, Stream: batch.Stream, Start: batch.Start, End: batch.End}.key()]
	if saved.BatchMetrics != 5 || saved.Count != 3 || !reflect.DeepEqual(saved.Names, want) || !reflect.DeepEqual(saved.CatalogConfirmedNames, want) {
		t.Fatal("out-of-order completion changed journal identity/order", saved)
	}
	if err := m.journal.compact(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(m.journal.path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"CatalogConfirmedNames"`) {
		t.Fatal("compaction lost fresh proof markers")
	}
}

func TestParallelAbsentProofsErrorsDoNotJournalPartialProofs(t *testing.T) {
	for _, mode := range []string{"head503", "catalog503", "still listed", "malformed", "wrong prefix", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			m, server := parallelAbsentFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					if mode == "head503" {
						w.WriteHeader(503)
					} else if mode == "redirect" {
						w.Header().Set("Location", "/other")
						w.WriteHeader(302)
					} else {
						w.WriteHeader(404)
					}
					return
				}
				switch mode {
				case "catalog503":
					w.WriteHeader(503)
				case "still listed":
					_ = json.NewEncoder(w).Encode([]string{r.URL.Query().Get("prefix")})
				case "malformed":
					_, _ = w.Write([]byte(`null`))
				case "wrong prefix":
					_, _ = w.Write([]byte(`["unrelated"]`))
				default:
					_, _ = w.Write([]byte(`[]`))
				}
			})
			before, _ := os.ReadFile(m.journal.path)
			names := []string{"metric.a", "metric.b", "metric.c"}
			err := m.skipAbsentSource(context.Background(), task{Source: server.URL, Stream: "stream", End: 3, Names: names}, names)
			after, _ := os.ReadFile(m.journal.path)
			if err == nil || len(m.journal.absent) != 0 || !reflect.DeepEqual(before, after) {
				t.Fatal("partial/invalid proof was durably skipped", mode, err)
			}
		})
	}
}

func TestParallelAbsentProofsFirstOrdinalErrorAndCancellation(t *testing.T) {
	var calls atomic.Int32
	m, server := parallelAbsentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		name := strings.TrimPrefix(r.URL.Path, "/metrics/")
		if name == "metric.0" {
			time.Sleep(15 * time.Millisecond)
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(401)
	})
	names := []string{}
	for i := range 64 {
		names = append(names, fmt.Sprintf("metric.%d", i))
	}
	_, err := m.parallelAbsentSourceProofs(context.Background(), server.URL, names)
	var api *apiError
	if !errors.As(err, &api) || api.Status != 503 || calls.Load() > 8 {
		t.Fatal("parallel completion changed first ordinal error or failed to stop scheduling", err, calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	before := calls.Load()
	if _, err := m.parallelAbsentSourceProofs(ctx, server.URL, names); !errors.Is(err, context.Canceled) || calls.Load() != before {
		t.Fatal("cancelled proof issued requests", err)
	}
	value, _ := m.absentProofSlots.Load(server.URL)
	if len(value.(chan struct{})) != 0 {
		t.Fatal("proof/error leaked a source slot")
	}
}

func TestParallelAbsentProofsCancelInflightReleasesAllSourceSlots(t *testing.T) {
	entered := make(chan struct{}, 8)
	var healthy atomic.Bool
	m, server := parallelAbsentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			return
		}
		entered <- struct{}{}
		<-r.Context().Done()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := m.parallelAbsentSourceProofs(ctx, server.URL, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"})
		done <- err
	}()
	for range 8 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("proofs failed to start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight cancellation leaked workers")
	}
	value, _ := m.absentProofSlots.Load(server.URL)
	if len(value.(chan struct{})) != 0 {
		t.Fatal("cancelled proof leaked a source slot")
	}
	healthy.Store(true)
	if absent, err := m.parallelAbsentSourceProofs(context.Background(), server.URL, []string{"present"}); err != nil || len(absent) != 0 {
		t.Fatal("released source could not make fresh progress", err)
	}
}
