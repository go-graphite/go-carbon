package carbonserver

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/blevesearch/vellum"
)

// Preserve the pre-fix enumeration as the benchmark's output and cost oracle.
func snapshotListReference(ti *trieIndex, sep byte) []string {
	files := ti.allMetricsMutable(sep)
	_ = ti.snapshot.walkNamespace("/", func(name string, _ uint64) bool {
		if sep != '.' {
			name = strings.ReplaceAll(name, ".", string(sep))
		}
		files = append(files, name)
		return true
	})
	sort.Strings(files)
	return files
}

// GO_CARBON_SNAPSHOT_FST optionally selects a captured immutable index. Only
// the captured file is read; no live service or snapshot files are changed.
func BenchmarkSnapshotMetricList(b *testing.B) {
	var index *vellum.FST
	var err error
	if path := os.Getenv("GO_CARBON_SNAPSHOT_FST"); path != "" {
		index, err = vellum.Open(path)
	} else {
		var data bytes.Buffer
		builder, buildErr := vellum.New(&data, nil)
		if buildErr != nil {
			b.Fatal(buildErr)
		}
		for i := 0; i < 1000000; i++ {
			key := fmt.Sprintf("\x00metrics\x00namespace%04d\x00by_region\x00region%02d\x00by_service\x00service%04d\x00requests\x00duration\x00percentile%03d.wsp", i/10000, i/1000%10, i/10%100, i%10)
			if err := builder.Insert([]byte(key), uint64(i)); err != nil {
				b.Fatal(err)
			}
		}
		if err := builder.Close(); err != nil {
			b.Fatal(err)
		}
		index, err = vellum.Load(data.Bytes())
	}
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = index.Close() })
	ti := newTrie(".wsp", 0, nil)
	ti.snapshot = &indexSnapshot{index: index}
	// Spread new metrics across the captured namespaces instead of appending
	// one artificial namespace that sorts entirely before or after the base.
	stride, overlays := max(index.Len()/10000, 1), 0
	err = ti.snapshot.walkNamespace("/", func(name string, row uint64) bool {
		if row%uint64(stride) == 0 {
			path := "/" + strings.ReplaceAll(name, ".", "/") + "/__snapshot_benchmark_new__.wsp"
			if _, err := ti.insertMutable(path, 0, 0, 0, 0); err != nil {
				b.Fatal(err)
			}
			overlays++
		}
		return overlays < 10000
	})
	if err != nil {
		b.Fatal(err)
	}
	var digest [sha256.Size]byte
	haveReference := false
	for _, variant := range []struct {
		name string
		list func(*trieIndex, byte) []string
	}{{"reference", snapshotListReference}, {"candidate", (*trieIndex).allMetrics}} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				files := variant.list(ti, '.')
				b.StopTimer()
				if !sort.StringsAreSorted(files) || len(files) != index.Len()+overlays {
					b.Fatal("incorrect list")
				}
				h := sha256.New()
				for _, name := range files {
					_, _ = h.Write([]byte(name))
					_, _ = h.Write([]byte{0})
				}
				got := [sha256.Size]byte(h.Sum(nil))
				if variant.name == "reference" {
					digest = got
					haveReference = true
				} else if haveReference && got != digest {
					b.Fatal("list differs from reference")
				}
				b.StartTimer()
			}
		})
		runtime.GC()
	}
}

func TestSnapshotMetricListParity(t *testing.T) {
	hybrid, oracle, _, _ := snapshotTrieFixture(t)
	for _, sep := range []byte{'.', '/'} {
		if got, want := hybrid.allMetrics(sep), oracle.allMetrics(sep); !reflect.DeepEqual(got, want) {
			t.Fatalf("snapshot separator %q: got %v, want %v", sep, got, want)
		}
	}
	for _, path := range []string{"/a/overlay.wsp", "/a!/value.wsp", "/a-/value.wsp", "/a0/value/leaf.wsp", "/b.wsp", "/空间/new.wsp", "/z/value.wsp"} {
		for _, index := range []*trieIndex{hybrid, oracle} {
			if _, err := index.insert(path, 1, 2, 3, 12345); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, sep := range []byte{'.', '/'} {
		if got, want := hybrid.allMetrics(sep), oracle.allMetrics(sep); !reflect.DeepEqual(got, want) {
			t.Fatalf("overlay separator %q: got %v, want %v", sep, got, want)
		}
	}
}
