package carbonserver

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// A cache written as concurrent gzip members must read back through the
// ordinary reader exactly as a single-stream cache does.
func TestFLCv2MembersReadAsOneStream(t *testing.T) {
	const total = 300000
	entry := func(i int, e *FLCEntry) {
		*e = FLCEntry{Path: fmt.Sprintf("/a/b%d/m%d.wsp", i%97, i), LogicalSize: int64(i), PhysicalSize: int64(2 * i), DataPoints: int64(3 * i), FirstSeenAt: int64(4 * i)}
	}
	for _, c := range []struct{ n, workers int }{{0, 1}, {1, 1}, {total, 1}, {total, 3}, {total, 8}} {
		n, workers := c.n, c.workers
		path := filepath.Join(t.TempDir(), "flc")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		w := bufio.NewWriter(f)
		if err = writeFLCv2Members(w, n, workers, entry); err != nil {
			t.Fatal(err)
		}
		if err = errors.Join(w.Flush(), f.Close()); err != nil {
			t.Fatal(err)
		}
		r, err := NewFileListCache(path, FLCVersionUnspecified, 'r')
		if err != nil {
			t.Fatal(err)
		}
		var want FLCEntry
		for i := 0; ; i++ {
			got, err := r.Read()
			if errors.Is(err, io.EOF) {
				if i != n {
					t.Fatal("entries", workers, i)
				}
				break
			}
			if err != nil {
				t.Fatal(workers, i, err)
			}
			entry(i, &want)
			if *got != want {
				t.Fatal("entry differs", workers, i, *got, want)
			}
		}
		_ = r.Close()
	}
}
