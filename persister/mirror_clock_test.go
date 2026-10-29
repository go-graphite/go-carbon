package persister

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	whisper "github.com/go-graphite/go-whisper"
)

// Cross the source writer's admission clock after the mirror has constructed
// its input windows. The shared result must reflect the actual source write,
// including a point that moves into or out of the finest archive.
func TestMirrorArchiveCorrectionsAcrossSourceClockBoundary(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, rollup := range []bool{false, true} {
			for _, tc := range []struct {
				name          string
				shift, offset int64
			}{
				{"primary-cutoff-forward", 1, -120},
				{"oldest-cutoff-forward", 1, -600},
				{"primary-cutoff-backward", -1, -121},
				{"recent-forward", 1, -2},
				{"future-admitted-forward", 1, 1},
				{"future-rejected-backward", -1, 0},
			} {
				t.Run(fmt.Sprintf("compressed=%t/rollup=%t/%s", compressed, rollup, tc.name), func(t *testing.T) {
					now := time.Unix(time.Now().Unix()/60*60, 0)
					base := now.Unix()
					previous := whisper.Now
					armed, calls := false, 0
					whisper.Now = func() time.Time {
						if armed {
							calls++
							if calls == 2 {
								now = now.Add(time.Duration(tc.shift) * time.Second)
							}
						}
						return now
					}
					defer func() { whisper.Now = previous }()
					root := t.TempDir()
					cache := &fakeCache{}
					p := newOOOTestPersister(t, filepath.Join(root, "original"), cache)
					p.outOfOrder.ticker.Stop()
					p.outOfOrder.ticker = nil
					p.SetCompressed(compressed)
					p.schemas[0].Compressed = &compressed
					retention := "1s:2m"
					if rollup {
						retention += ",5s:10m"
					}
					p.schemas[0].Retentions = whisper.MustParseRetentionDefs(retention)
					db, err := openTestStore(t, filepath.Join(root, "exact"), store.Options{ExactBlocks: true, Now: func() time.Time { return now }})
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					wrapped := &boundaryMirrorStore{Store: db}
					p.SetMetricStore(wrapped)
					p.SetMirrorFiles(true)
					p.SetMirrorState(&MirrorState{ScratchDir: root})
					cache.add("clock", base-10, 7)
					p.store("clock")
					cache.add("clock", base+tc.offset, 111.25)
					armed = true
					p.store("clock")
					armed = false
					if calls < 2 || now.Unix() != base+tc.shift {
						t.Fatalf("source clock did not cross: calls=%d now=%d", calls, now.Unix())
					}
					if wrapped.replacements != 0 || cache.confirmedPoints() != 2 {
						t.Fatalf("routine second crossing performed full repair: repairs=%d confirmed=%d", wrapped.replacements, cache.confirmedPoints())
					}
					want, err := whisperio.ReadSnapshot(context.Background(), "clock", filepath.Join(root, "original", "clock.wsp"))
					if err != nil {
						t.Fatal(err)
					}
					got, err := db.Snapshot(context.Background(), "clock")
					if err != nil {
						t.Fatal(err)
					}
					if err = store.EqualSnapshots(want, got); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
