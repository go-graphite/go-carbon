package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

func pendingRetryPipeline(t *testing.T, b *pipelineWindowBackend, o syncOptions) *syncer {
	t.Helper()
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), "retry-pipeline")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(j.close)
	if len(j.rows) == 1 {
		var names []string
		for n := range b.store(syncD1).points {
			names = append(names, n)
		}
		sort.Strings(names)
		for i := 0; i < len(names); i += o.batch {
			for _, source := range o.from {
				row := syncRow{Kind: "window-selected", Round: i/o.batch + 1, Source: source, SelectionComplete: true}
				for _, name := range names[i:min(i+o.batch, len(names))] {
					row.Skipped = append(row.Skipped, syncSkip{name, syncWindowSelectionReason})
				}
				if err := j.append(row); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	return &syncer{o: o, b: b, j: j, forbid: map[string]bool{}}
}

func retryPointBits(b *pipelineWindowBackend) map[string]map[int64]map[int64]uint64 {
	out := map[string]map[int64]map[int64]uint64{}
	for name, archives := range b.store(syncT).points {
		out[name] = map[int64]map[int64]uint64{}
		for step, points := range archives {
			out[name][step] = map[int64]uint64{}
			for ts, v := range points {
				out[name][step][ts] = math.Float64bits(v)
			}
		}
	}
	return out
}

func TestWindowRetryParallelExactResume(t *testing.T) {
	t.Parallel()
	serial, so := pipelineFixture(t, 1)
	oracle := pendingRetryPipeline(t, serial, so)
	if err := oracle.retryWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, o := pipelineFixture(t, 8)
	s := pendingRetryPipeline(t, b, o)
	b.arrived = make(chan string, 64)
	b.release = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.retryWindowSelections(ctx) }()
	seen := map[string]bool{}
	for range 2 {
		select {
		case name := <-b.arrived:
			seen[name] = true
		case <-time.After(5 * time.Second):
			cancel()
			close(b.release)
			t.Fatal("disjoint retry groups did not overlap")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(b.release)
	if len(seen) != 2 || b.active.Load() != 0 {
		t.Fatal("overlap/join", seen, b.active.Load())
	}
	b.arrived = nil
	if err := s.retryWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retryPointBits(serial), retryPointBits(b)) {
		t.Fatal("parallel retry changed timestamp/value bits or donor priority")
	}
	copies := len(b.copies)
	before := s.prog.done.Load()
	if err := s.retryWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(b.copies) != copies || s.prog.done.Load() != before || before != 48 {
		t.Fatal("resume repeated work or lost progress", before, s.prog.done.Load(), len(b.copies), copies)
	}
}

func TestWindowRetryBatchedPendingRefresh(t *testing.T) {
	t.Parallel()
	calls := 0
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/transfer/export" {
			t.Error("unexpected path", r.URL.Path)
		}
		if r.URL.Query().Get("max_bytes") != "65536" {
			t.Error("unbounded discarded export")
		}
		var names []string
		if err := json.NewDecoder(r.Body).Decode(&names); err != nil {
			t.Error(err)
		}
		if len(names) > 500 || len(names) < 2 {
			t.Error("per-name/unbounded refresh", len(names))
		}
		calls++
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	defer donor.Close()
	names := make([]string, 1200)
	for i := range names {
		names[i] = fmt.Sprintf("pending.%04d", i)
	}
	b := newSyncLiveBackend(syncLiveOptions{})
	if err := b.prepareWindowSelection(context.Background(), donor.URL, names); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatal("refresh requests", calls)
	}
}

func TestWindowRetryLegacyProgressIsObservationOnly(t *testing.T) {
	t.Parallel()
	b, o := pipelineFixture(t, 8)
	s := pendingRetryPipeline(t, b, o)
	path := filepath.Join(o.state, "progress.json")
	if err := os.WriteFile(path, []byte(`{"phase":"window retry","done":37,"total":48}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.preserveWindowRetryProgress(); err != nil {
		t.Fatal(err)
	}
	if len(s.resolvedWindowSelections()) != 0 {
		t.Fatal("display floor became coverage proof")
	}
	if err := s.retryWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(b.copies) != 24 || s.prog.done.Load() != 48 || s.prog.durableFloor != 37 {
		t.Fatal("legacy adoption skipped work/lost floor", len(b.copies), s.prog.done.Load(), s.prog.durableFloor)
	}
	kept := verificationCheckpointRows(s.j.rows)
	floor, visits := 0, 0
	for _, r := range kept {
		if r.Kind == "window-retry-floor" {
			floor++
		}
		if r.Kind == "window-retry-visited" {
			visits++
		}
	}
	if floor != 1 || visits != 24 {
		t.Fatal("checkpoint dropped retry cursor", floor, visits)
	}
}

func TestWindowRetryResumesUnvisitedGroupsFirst(t *testing.T) {
	t.Parallel()
	b, o := pipelineFixture(t, 1)
	s := pendingRetryPipeline(t, b, o)
	for _, source := range o.from {
		if err := s.j.append(syncRow{Kind: "window-retry-visited", Round: 1, Source: source, Names: 2}); err != nil {
			t.Fatal(err)
		}
	}
	b.arrived = make(chan string, 64)
	b.release = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.retryWindowSelections(ctx) }()
	select {
	case name := <-b.arrived:
		if name != "metric.002" {
			cancel()
			<-done
			t.Fatal("resume started with visited uncertainty", name)
		}
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("retry stalled")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(b.release)
	b.arrived = nil
	if err := s.retryWindowSelections(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(b.copies) != 24 || s.prog.done.Load() != 48 {
		t.Fatal("resume skipped old uncertainty", len(b.copies), s.prog.done.Load())
	}
}
