package persister

import (
	"context"
	"errors"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

var errReadbackForTest = errors.New("independent readback failed")

type tracedReadbackStore struct {
	*store.Store
	reads int
	fail  bool
}

func (s *tracedReadbackStore) Snapshot(ctx context.Context, name string) (store.Snapshot, error) {
	s.reads++
	if s.fail {
		return store.Snapshot{}, errReadbackForTest
	}
	return s.Store.Snapshot(ctx, name)
}

func TestMirrorVerificationOnlySkipsCertifiedUnchangedReadback(t *testing.T) {
	ctx := context.Background()
	db, err := openTestStore(t, t.TempDir(), store.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := store.Retention{Step: 1, Count: 256}
	source := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "verified", Retentions: []store.Retention{r}, AggregationMethod: store.Last}}, Archives: []store.Archive{{Retention: r, Points: []store.Point{{Timestamp: 128, Value: 1}}}}}
	if _, err := db.Replace(ctx, source); err != nil {
		t.Fatal(err)
	}
	target := &tracedReadbackStore{Store: db, fail: true}
	p := &Whisper{metricStore: target}
	if _, count, err := p.reconcileMirrorSnapshot(ctx, target, source, true); err != nil || count != 1 || target.reads != 0 {
		t.Fatalf("equal image: points=%d reads=%d err=%v", count, target.reads, err)
	}
	source.Archives[0].Points[0].Value = 2
	if _, _, err := p.reconcileMirrorSnapshot(ctx, target, source, true); !errors.Is(err, errReadbackForTest) || target.reads != 1 {
		t.Fatalf("changed image bypassed readback: reads=%d err=%v", target.reads, err)
	}
	target.fail = false
	source.Archives[0].Points = append(source.Archives[0].Points, source.Archives[0].Points[0])
	if _, _, err := p.reconcileMirrorSnapshot(ctx, target, source, true); err == nil || target.reads != 2 {
		t.Fatalf("noncanonical image bypassed readback: reads=%d err=%v", target.reads, err)
	}
}
