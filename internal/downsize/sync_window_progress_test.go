package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A real resume must retain durable progress without presenting replay as work.
func TestSyncWindowResumedThroughput(t *testing.T) {
	b, o := pipelineFixture(t, 8)
	o.noVerify = true // observe selection progress independently of copy/verify counts
	// Catalog names whose requested windows are empty still constitute new
	// selection work. No copied/discovered journal counter changes on resume.
	for _, source := range []string{syncD1, syncD2} {
		for name := range b.store(source).points {
			b.store(source).points[name] = nil
		}
	}
	for run := range 2 {
		if err := runSync(context.Background(), o, b); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(o.state, "progress.json"))
		if err != nil {
			t.Fatal(err)
		}
		var p progressSnapshot
		if err := json.Unmarshal(data, &p); err != nil {
			t.Fatal(err)
		}
		if p.Done != 24 {
			t.Fatalf("run %d: progress %+v", run, p)
		}
		if run == 0 && p.Rate <= 0 {
			t.Fatal("new work has no live throughput", p)
		}
		if run == 1 && p.Rate != 0 {
			t.Fatal("journal replay counted as throughput", p)
		}
	}
}

type cancelWindowSelectionBackend struct {
	*pipelineWindowBackend
	entered chan struct{}
}

func (b *cancelWindowSelectionBackend) WindowPresenceRanges(ctx context.Context, source string, names []string, windows syncRanges) (map[string][]syncDigest, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// Cancelling queued selections must not wait for every serial donor export.
// TestSyncWindowPipelineOverlapAndDrain separately proves issued pulls drain.
func TestSyncWindowCancellationStopsReadSelections(t *testing.T) {
	base, o := pipelineFixture(t, 8)
	b := &cancelWindowSelectionBackend{base, make(chan struct{}, 8)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runSync(ctx, o, b) }()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("selection did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read-only selection ignored cancellation")
	}
	if len(base.copies) != 0 {
		t.Fatal("cancelled selection started a copy")
	}
}
