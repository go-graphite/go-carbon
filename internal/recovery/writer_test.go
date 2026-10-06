package recovery

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestWriterProducesLegacyAndIndexedRecovery(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	builder := NewBuilder(func(name string) bool { return name == "known" })
	dump, err := NewWriter(filepath.Join(dir, "cache.1.2.bin"), 0, 1<<20, builder)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := NewWriter(filepath.Join(dir, "input.1.2.bin"), 1, 4096, builder)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if err := wal.WritePoints(points.OnePoint(fmt.Sprintf("metric%d", worker), float64(i+1000), int64(i))); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	for worker := 0; worker < 4; worker++ {
		for i := 0; i < 100; i++ {
			if err := dump.WritePoints(points.OnePoint(fmt.Sprintf("metric%d", worker), float64(i), int64(i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	wg.Wait()
	cacheFile, err := dump.Close()
	if err != nil {
		t.Fatal(err)
	}
	walFile, err := wal.Close()
	if err != nil {
		t.Fatal(err)
	}
	indexFile, err := WriteIndex(dir, builder)
	if err != nil {
		t.Fatal(err)
	}
	if err = Publish(dir, root, cacheFile, walFile, indexFile, "test-read-index"); err != nil {
		t.Fatal(err)
	}
	bundle, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer bundle.Close()
	if bundle.Points() != 800 || bundle.Metrics() != 4 {
		t.Fatal("bundle totals")
	}
	for worker := 0; worker < 4; worker++ {
		name := fmt.Sprintf("metric%d", worker)
		slot, ok, err := bundle.Find(name)
		if err != nil || !ok {
			t.Fatal(err)
		}
		p, err := bundle.Read(slot)
		if err != nil {
			t.Fatal(err)
		}
		for i, point := range p.Data {
			want := float64(i)
			if i >= 100 {
				want = float64(i - 100 + 1000)
			}
			if point.Value != want || point.Timestamp != int64(i%100) {
				t.Fatal("writer replay ordering", i, point)
			}
		}
	}
	var legacy uint64
	for _, f := range []File{cacheFile, walFile} {
		path := filepath.Join(dir, f.Name)
		want, err := Describe(path)
		if err != nil || want != f {
			t.Fatal("streaming digest differs", want, err)
		}
		if err = points.ReadFromFile(path, func(p *points.Points) { legacy += uint64(len(p.Data)) }); err != nil {
			t.Fatal(err)
		}
	}
	if legacy != 800 {
		t.Fatal("legacy point count", legacy)
	}
	if err = wal.WritePoints(points.OnePoint("after-close", 1, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatal("write after close", err)
	}
}
