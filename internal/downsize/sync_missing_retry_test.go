package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type missingRetryBackend struct {
	*precheckBackend
	live *syncLiveBackend
}

func (b *missingRetryBackend) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	return b.live.Copy(ctx, source, target, names, journal)
}

// Exercise real pull/verify journals under the parent controller. A cancellation
// after two names must keep both prechecks and partial transfers, even when the
// next process changes its metadata batch size and concurrency budget.
func TestSyncMissingRetryRetainsPrecheckAndPartialTransfers(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, failure := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%v/failure=%v", legacy, failure), func(t *testing.T) { testSyncMissingRetry(t, legacy, failure) })
		}
	}
}
func testSyncMissingRetry(t *testing.T, legacy, failure bool) {
	names := []string{"a", "b", "c", "d", "e", "f"}
	f, source, target := newFakeCopy(t, names)
	dir := t.TempDir()
	var input strings.Builder
	for _, n := range names {
		fmt.Fprintf(&input, "%s\t1\n", n)
	}
	if err := os.WriteFile(filepath.Join(dir, "missing.tsv"), []byte(input.String()), 0600); err != nil {
		t.Fatal(err)
	}
	metadata := newFakeSync()
	for _, n := range names {
		metadata.put(source, n, syncTestSchema)
	}
	checked := map[string]int{}
	pre := &precheckBackend{fakeSync: metadata}
	// Metadata workers are serialized by streams=1 below after the first pass;
	// use the fake backend lock for the initial parallel prechecks.
	pre.info = func(ctx context.Context, store string, chunk []string) (map[string][]syncArchive, error) {
		metadata.mu.Lock()
		for _, n := range chunk {
			checked[n]++
		}
		metadata.mu.Unlock()
		return metadata.Info(ctx, store, chunk)
	}
	template := copyTestOptions(t, source, target)
	template.maxNames = 1
	live := newSyncLiveBackend(syncLiveOptions{copyStreams: 1, copyAttempts: 1, hostWait: time.Second, copyTemplate: &template})
	b := &missingRetryBackend{pre, live}
	open := func(batch, streams int) *syncer {
		t.Helper()
		j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), "retry")
		if err != nil {
			t.Fatal(err)
		}
		return &syncer{o: syncOptions{to: target, from: []string{source}, state: dir, batch: batch, streams: streams}, b: b, j: j, copied: map[string]bool{}, failedCp: map[string]bool{}, forbid: map[string]bool{}, sum: syncSummary{phases: map[string]*syncPhase{}}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.block = func(attempt int) bool {
		if !failure && attempt == 3 {
			cancel()
			return true
		}
		return false
	}
	if failure {
		f.inject = func(_ int, names []string) int {
			if names[0] == "c" {
				return http.StatusNotFound
			}
			return 0
		}
	}
	s := open(2, 2)
	err := s.copyMissing(ctx)
	if failure && err != nil || !failure && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	expectedDone, expectedWaves, expectedReplay := int64(2), 1, int64(2)
	if failure {
		expectedDone, expectedWaves, expectedReplay = 6, 2, 5
	}
	if s.prog.done.Load() != expectedDone || s.prog.total != 6 || s.prog.phase != "copy" {
		t.Fatalf("partial progress: %s %d/%d", s.prog.phase, s.prog.done.Load(), s.prog.total)
	}
	waves := s.j.find(func(r syncRow) bool { return r.Kind == "missing-wave" })
	if len(waves) != expectedWaves || !reflect.DeepEqual(waves[0].Selected, names[:4]) {
		t.Fatal("pending wave lost", waves)
	}
	s.j.close()
	if legacy {
		// Simulate a client predating exact wave manifests and precheck receipts.
		path := filepath.Join(dir, "sync.jsonl")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var old strings.Builder
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			var row map[string]any
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				t.Fatal(err)
			}
			if row["kind"] == "missing-wave" {
				continue
			}
			delete(row, "selected")
			encoded, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintln(&old, string(encoded))
		}
		if err := os.WriteFile(path, []byte(old.String()), 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.block = nil
	f.inject = nil
	s = open(3, 1)
	if err := s.copyMissing(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.prog.done.Load() != 6 || s.prog.total != 6 || s.sum.Copied != 6 {
		t.Fatalf("final progress %d/%d", s.prog.done.Load(), s.prog.total)
	}
	if s.prog.replayed.Load() != expectedReplay {
		t.Fatalf("replay reported as throughput: %d", s.prog.replayed.Load())
	}
	s.j.close()
	for _, n := range names {
		if checked[n] != 1 {
			t.Errorf("%s prechecked %d times", n, checked[n])
		}
		if f.imported[n] != 1 {
			t.Errorf("%s imported %d times", n, f.imported[n])
		}
	}
	// Completed waves remain completed after another restart.
	s = open(1, 3)
	if err := s.copyMissing(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.prog.done.Load() != 6 || s.prog.replayed.Load() != 6 {
		t.Fatalf("completed replay: %d (%d replay)", s.prog.done.Load(), s.prog.replayed.Load())
	}
	s.j.close()
	for _, n := range names {
		if checked[n] != 1 || f.imported[n] != 1 {
			t.Fatalf("completed work repeated for %s", n)
		}
	}
}

func TestCopyRetryDurableProgressNeverDecreases(t *testing.T) {
	names := []string{"a", "b", "c"}
	f, source, target := newFakeCopy(t, names)
	o := copyTestOptions(t, source, target)
	o.streams, o.maxNames = 1, 1
	var last int64
	var replayed []int64
	o.onProgress = func(done int64, replay bool) {
		if done < last {
			t.Errorf("durable progress fell from %d to %d", last, done)
		}
		last = done
		if replay {
			replayed = append(replayed, done)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.inject = func(attempt int, _ []string) int {
		if attempt == 2 {
			return 503
		}
		return 0
	}
	f.block = func(attempt int) bool {
		if attempt == 4 {
			cancel()
			return true
		}
		return false
	}
	if err := planFreeCopy(ctx, o); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if last != 2 {
		t.Fatal("lost partial progress", last)
	}
	f.block = nil
	if err := planFreeCopy(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if last != 3 || !reflect.DeepEqual(replayed, []int64{0, 2}) {
		t.Fatalf("progress %d, replay %v", last, replayed)
	}
	for _, n := range names {
		if f.imported[n] != 1 {
			t.Errorf("%s imported %d times", n, f.imported[n])
		}
	}
	if f.attempts != 5 {
		t.Fatalf("unexpected attempts %d", f.attempts)
	}
}

func TestSyncPrecheckErrorRetainsCompletedNames(t *testing.T) {
	dir := t.TempDir()
	checked := map[string]int{}
	fail := true
	b := &precheckBackend{fakeSync: newFakeSync()}
	b.info = func(_ context.Context, _ string, names []string) (map[string][]syncArchive, error) {
		out := map[string][]syncArchive{}
		for _, n := range names {
			checked[n]++
			if n == "b" && fail {
				return nil, fmt.Errorf("source unavailable")
			}
			out[n] = syncTestSchema
		}
		return out, nil
	}
	j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), "precheck")
	if err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: syncOptions{from: []string{syncD1}, batch: 1, streams: 1}, j: j, b: b}
	if _, _, _, err := s.dropShort(context.Background(), 0, []string{"a", "b", "c"}); err == nil {
		t.Fatal("failure lost")
	}
	j.close()
	j, err = openSyncJournal(filepath.Join(dir, "sync.jsonl"), "precheck")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	s.j, s.o.batch = j, 2
	fail = false
	keep, _, _, err := s.dropShort(context.Background(), 0, []string{"a", "b", "c"})
	if err != nil || !reflect.DeepEqual(keep, []string{"a", "b", "c"}) {
		t.Fatalf("%v %v", keep, err)
	}
	if checked["a"] != 1 || checked["b"] != 2 || checked["c"] != 1 {
		t.Fatal("completed precheck repeated", checked)
	}
}
