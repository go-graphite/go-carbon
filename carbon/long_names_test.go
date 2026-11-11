package carbon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
)

func TestLongNamesDumpRestore(t *testing.T) {
	for _, format := range []recovery.Format{recovery.FormatLegacy, recovery.FormatCompact, recovery.FormatBlocks} {
		t.Run(format.String(), func(t *testing.T) {
			root := t.TempDir()
			// Dump filenames carry a checkpoint identity, never the metric key.
			path := filepath.Join(root, recovery.CacheName(format, "42.1000.bin"))
			if len(filepath.Base(path)) > 255 {
				t.Fatal("unsafe dump filename")
			}
			writer, err := recovery.NewWriterFormat(path, 0, 4096, recovery.NewBuilder(nil), format)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]*points.Points{}
			for _, size := range []int{300, 4096} {
				name := "long." + strings.Repeat("x", size) + ".value"
				p := points.OnePoint(name, 42, 1000)
				want[name] = p
			}
			if err := writer.WriteSegments(1, 1, func(_ int, out *recovery.Segment) error {
				for _, p := range want {
					if err := out.WritePoints(p); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			app := &App{}
			app.RestoreFromDir(root, func(p *points.Points) {
				if expected := want[p.Metric]; expected == nil || !expected.Eq(p) {
					t.Fatal("dump changed metric key or points")
				}
				delete(want, p.Metric)
			})
			if len(want) != 0 {
				t.Fatal("long names missing after restore")
			}
			if entries, err := os.ReadDir(root); err != nil || len(entries) != 0 {
				t.Fatalf("restore left dump files: %v %v", entries, err)
			}
		})
	}
}
