package v2daemon

import (
	"fmt"
	"os"
	"testing"
)

// Rotation granularity must bound the replay tail independently of total input.
// Compare the same correction/IEEE-edge history with coarse and fine segments;
// a writable restart must reproduce every raw/rollup tuple and metadata field.
func TestJournalSegmentSizeDifferential(t *testing.T) {
	var image [32]byte
	var retained [2]int64
	for i, limit := range []int64{256, 16 << 10} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			root := t.TempDir()
			c := segmentSkipConfig()
			c.JournalSegmentBytes = limit
			s, err := Open(root, c)
			if err != nil {
				t.Fatal(err)
			}
			segmentSkipWrite(t, s, 24, func(_ int) {
				for _, p := range s.Producers {
					if err := s.Core.AppendBatches(p.index, nil, p.cursor, true); err != nil {
						t.Fatal(err)
					}
				}
			})
			got := parallelReplayDigest(t, s)
			if i == 0 {
				image = got
			} else if got != image {
				t.Fatal("segment size changed stored points or metadata")
			}
			paths, err := journalSegments(s.Producers[0].journalDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				st, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				retained[i] += st.Size()
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(root, c)
			if err != nil {
				t.Fatal(err)
			}
			if got := parallelReplayDigest(t, s); got != image {
				t.Fatal("segment size changed recovered points or metadata")
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			t.Logf("limit=%d retained=%d", limit, retained[i])
		})
	}
	if retained[0]*2 >= retained[1] {
		t.Fatalf("fine segments did not bound the durable tail: %v", retained)
	}
}
