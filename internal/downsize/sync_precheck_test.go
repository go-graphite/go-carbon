package downsize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestSyncPrecheckLeavesInvalidNamesForCopyFailure(t *testing.T) {
	j, err := openSyncJournal(filepath.Join(t.TempDir(), "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	invalid := strings.Repeat("x", chunkstore.MaxMetricNameBytes+1)
	calls := 0
	b := &precheckBackend{fakeSync: newFakeSync()}
	b.info = func(_ context.Context, _ string, names []string) (map[string][]syncArchive, error) {
		calls++
		if !reflect.DeepEqual(names, []string{"a"}) {
			t.Fatal("unimportable names reached metadata precheck")
		}
		return map[string][]syncArchive{"a": syncTestSchema}, nil
	}
	s := &syncer{o: syncOptions{from: []string{syncD1}, batch: 1, streams: 1}, b: b, j: j}
	for range 2 {
		keep, short, absent, err := s.dropShort(context.Background(), 0, []string{"a", invalid})
		if err != nil || !reflect.DeepEqual(keep, []string{"a", invalid}) || len(short)+len(absent) != 0 {
			t.Fatalf("keep=%d short=%d absent=%d err=%v", len(keep), len(short), len(absent), err)
		}
	}
	if calls != 1 {
		t.Fatalf("metadata calls=%d; resume checkpoint not reused", calls)
	}
}

type precheckBackend struct {
	*fakeSync
	info func(context.Context, string, []string) (map[string][]syncArchive, error)
	copy func()
}

func (b *precheckBackend) Info(ctx context.Context, source string, names []string) (map[string][]syncArchive, error) {
	return b.info(ctx, source, names)
}

func (b *precheckBackend) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	if b.copy != nil {
		b.copy()
	}
	return b.fakeSync.Copy(ctx, source, target, names, journal)
}

func TestSyncPrecheckBoundedCheckpoint(t *testing.T) {
	dir := t.TempDir()
	j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 2)
	release := make(chan struct{})
	var active, peak atomic.Int64
	b := &precheckBackend{fakeSync: newFakeSync()}
	b.info = func(ctx context.Context, _ string, names []string) (map[string][]syncArchive, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		started <- names[0]
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if names[0] == "a" {
			return map[string][]syncArchive{"a": syncTestSchema}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s := &syncer{o: syncOptions{from: []string{syncD1}, batch: 1, streams: 2}, b: b, j: j}
	result := make(chan error, 1)
	go func() { _, _, _, err := s.dropShort(ctx, 0, []string{"a", "b", "c"}); result <- err }()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("workers did not start together")
		}
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for len(j.find(func(r syncRow) bool { return r.Kind == "prechecked" })) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("checkpoint missing")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency %d", peak.Load())
	}
	j.close()
	j, err = openSyncJournal(filepath.Join(dir, "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	s.j = j
	var calls atomic.Int64
	b.info = func(_ context.Context, _ string, names []string) (map[string][]syncArchive, error) {
		calls.Add(1)
		if names[0] == "a" {
			t.Error("completed precheck repeated")
		}
		if names[0] == "b" {
			return map[string][]syncArchive{"b": {{Step: 60, Points: 1440}}}, nil
		}
		return map[string][]syncArchive{}, nil
	}
	keep, short, absent, err := s.dropShort(context.Background(), 0, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || !reflect.DeepEqual(keep, []string{"a"}) || !reflect.DeepEqual(short, []string{"b"}) || !reflect.DeepEqual(absent, []string{"c"}) {
		t.Fatalf("calls=%d keep=%v short=%v absent=%v", calls.Load(), keep, short, absent)
	}
}

func TestSyncMissingBatchResumeAndAbsentFallback(t *testing.T) {
	f, o, _ := threeSources(t)
	// Catalog still lists the name, but its first source has lost the metadata.
	f.put(syncP, "d.missing", syncTestSchema)
	b := &precheckBackend{fakeSync: f}
	b.info = func(ctx context.Context, source string, names []string) (map[string][]syncArchive, error) {
		out, err := f.Info(ctx, source, names)
		if source == syncD1 {
			delete(out, "d.missing")
		}
		return out, err
	}
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	for _, call := range f.copies {
		if call == syncD1+" d.missing" {
			t.Fatal("absent source copied")
		}
	}
	before := len(f.copies)
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if len(f.copies) != before {
		t.Fatalf("resume copied again: %v", f.copies[before:])
	}
	if _, ok := f.store(syncT).archives["d.missing"]; !ok {
		t.Fatal("later source was not tried")
	}
}

func TestSyncMissingCopiesBeforePrecheckEnds(t *testing.T) {
	dir := t.TempDir()
	j, err := openSyncJournal(filepath.Join(dir, "sync.jsonl"), "test")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := os.WriteFile(filepath.Join(dir, "missing.tsv"), []byte("a\t1\nb\t1\nc\t1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f := newFakeSync()
	for _, n := range []string{"a", "b", "c"} {
		f.put(syncD1, n, syncTestSchema)
	}
	var copies int
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &precheckBackend{fakeSync: f, copy: func() { copies++ }}
	b.info = func(ctx context.Context, source string, names []string) (map[string][]syncArchive, error) {
		if names[0] == "b" && copies == 0 {
			t.Error("all prechecks ran before first import")
		}
		if names[0] == "c" {
			cancel()
			return nil, ctx.Err()
		}
		return f.Info(ctx, source, names)
	}
	newSyncer := func() *syncer {
		return &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: dir, streams: 1, batch: 1}, b: b, j: j, copied: map[string]bool{}, failedCp: map[string]bool{}, forbid: map[string]bool{}, sum: syncSummary{phases: map[string]*syncPhase{}}}
	}
	if err := newSyncer().copyMissing(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if copies != 2 {
		t.Fatalf("copied %d waves before interruption", copies)
	}
	b.info = f.Info
	if err := newSyncer().copyMissing(context.Background()); err != nil {
		t.Fatal(err)
	}
	if copies != 3 {
		t.Fatalf("repeated imported waves: %d", copies)
	}
}
