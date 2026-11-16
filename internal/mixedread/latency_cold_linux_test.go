package mixedread

import (
	"context"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/partsread"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPhysicalColdSSTFixture(t *testing.T) {
	root := t.TempDir()
	m := partsread.Metric{ID: 42, Archives: []partsread.Archive{{Step: 1, Points: 1000}}}
	records := []partsread.Record{{ID: 42, Step: 1, Sample: partsread.Sample{Time: 123, Bits: 0x8000000000000000, Sequence: 1}}}
	path := filepath.Join(root, "parts")
	latencyPrepareSST(t, path, m, records)
	s := latencyOpenSST(t, path, m, records)
	defer s.close(t)
	latencyEvictOwned(t, root, path)
	before, _ := s.counts()
	n := 0
	err := s.index.Scan(context.Background(), 42, m.Archives[0], 100, 200, func(p partsread.Sample) error {
		if p != records[0].Sample {
			t.Fatal("SST sample changed")
		}
		n++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	after, _ := s.counts()
	if n != 1 || after <= before {
		t.Fatal("fixture did not read an SST block", n, before, after)
	}
}

func latencyEvictOwned(t *testing.T, owner string, roots ...string) {
	t.Helper()
	for _, root := range roots {
		rel, err := filepath.Rel(owner, root)
		if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
			t.Fatal("eviction outside owned fixture")
		}
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if d.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("fixture symlink refused")
			}
			if filepath.Ext(path) != ".sst" {
				return nil
			}
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			err = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
			closeErr := f.Close()
			if err != nil {
				return err
			}
			return closeErr
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
