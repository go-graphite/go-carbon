package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestDeleteMetricsIfUnchanged(t *testing.T) {
	s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	ctx := context.Background()
	gone := createTestMetric(t, s, "team.gone")
	stale := createTestMetric(t, s, "team.stale")
	kept := createTestMetric(t, s, "team.kept")
	stale.Revision++

	s.readOnly = true
	if _, err := s.DeleteMetricsIfUnchanged(ctx, []Metadata{gone}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only store: %v", err)
	}
	s.readOnly = false

	epoch := s.ReadCacheEpoch()
	outcomes, err := s.DeleteMetricsIfUnchanged(ctx, []Metadata{gone, stale})
	if err != nil || len(outcomes) != 2 || outcomes[0] != nil || !errors.Is(outcomes[1], ErrConflict) {
		t.Fatalf("outcomes=%v: %v", outcomes, err)
	}
	if s.ReadCacheEpoch() == epoch {
		t.Fatal("a removed metric kept cached reads")
	}
	for _, m := range []Metadata{gone, stale, kept} {
		_, err := s.Metadata(ctx, m.Name)
		if wantGone := m.Name == gone.Name; errors.Is(err, ErrNotFound) != wantGone {
			t.Errorf("%s: metadata error %v, deleted=%t", m.Name, err, wantGone)
		}
	}

	// Nothing removed: cached reads stay valid.
	epoch = s.ReadCacheEpoch()
	outcomes, err = s.DeleteMetricsIfUnchanged(ctx, []Metadata{gone})
	if err != nil || !errors.Is(outcomes[0], ErrNotFound) {
		t.Fatalf("outcomes=%v: %v", outcomes, err)
	}
	if s.ReadCacheEpoch() != epoch {
		t.Fatal("a batch that removed nothing invalidated cached reads")
	}
}
