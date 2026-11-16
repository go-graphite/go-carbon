package chunkstore

import (
	"context"
	"testing"
)

func TestFrozenDenseFallbackDoesNotRetainView(t *testing.T) {
	s, m := openFrozenWorkspaceStore(t, &frozenWorkspaceCodec{})
	var workspace FrozenReadWorkspace
	defer workspace.Reset()
	defer s.Close()
	for range 3 {
		err := s.FetchFrozenDenseWithWorkspace(context.Background(), m, 1, 2, &workspace, func(_, _, _ int, values []float64) error {
			if len(values) != 1 || values[0] != 7 {
				t.Fatal(values)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if s.readCount != 0 || workspace.io.ctx != nil || workspace.io.reader != nil {
			t.Fatal("retained view")
		}
	}
}
