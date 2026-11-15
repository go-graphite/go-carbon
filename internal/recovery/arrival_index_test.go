package recovery

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func arrivalFixture(t testing.TB, count int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cache.test.bin")
	f, err := os.Create(ArrivalPath(path))
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	_, _ = w.WriteString("GCAGE001")
	var raw []byte
	for i := range count {
		name := fmt.Sprintf("service.%06d.%s.requests.count", i, strings.Repeat("region.host.", 12))
		for _, at := range []int64{int64(i + 1), int64(i - 7), math.MaxInt64, 0} {
			// Most metrics have one timestamp; some repeat across worker ranges.
			if at != int64(i+1) && i%13 != 0 {
				continue
			}
			raw = appendArrival(raw[:0], name, at)
			if _, err := w.Write(raw); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func compareArrivalIndex(t *testing.T, path string) {
	t.Helper()
	want, err := ReadArrivals(path) // unchanged master implementation
	got, fastErr := openArrivalIndex(path)
	if (err == nil) != (fastErr == nil) {
		t.Fatalf("map error %v != mapped error %v", err, fastErr)
	}
	if fastErr != nil || got == nil {
		return
	}
	defer got.data.Unmap()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for name, at := range want {
				if actual := got.first(name); actual != at {
					t.Errorf("%q: got %d, want %d", name, actual, at)
				}
			}
			if got.first("missing.metric") != 0 {
				t.Error("missing metric has an arrival")
			}
		})
	}
	wg.Wait()
}

func TestMappedArrivalsMatchMap(t *testing.T) {
	compareArrivalIndex(t, arrivalFixture(t, 25000))
}

func TestMappedArrivalDuplicatesAcrossRanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.test.bin")
	name := strings.Repeat("n", 1<<20)
	raw := []byte("GCAGE001")
	for _, at := range []int64{100, 200, 50, math.MinInt64, 0, math.MaxInt64, -1} {
		raw = appendArrival(raw, name, at)
	}
	if err := os.WriteFile(ArrivalPath(path), raw, 0600); err != nil {
		t.Fatal(err)
	}
	compareArrivalIndex(t, path)
}

func TestMappedArrivalErrorsMatchMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.test.bin")
	compareArrivalIndex(t, path) // no companion
	raw := appendArrival([]byte("GCAGE001"), "metric", math.MinInt64)
	raw = appendArrival(raw, "metric", math.MaxInt64)
	// A nonminimal name-length varint is accepted by the reference reader.
	nonminimal := append([]byte{0x86, 0}, raw[9:]...)
	nonminimal = append([]byte("GCAGE001"), nonminimal...)
	for _, data := range [][]byte{raw, nonminimal, []byte("GCAGE999"), appendArrival([]byte("GCAGE001"), "", 1)} {
		for end := 0; end <= len(data); end++ {
			if err := os.WriteFile(ArrivalPath(path), data[:end], 0600); err != nil {
				t.Fatal(err)
			}
			compareArrivalIndex(t, path)
		}
	}
	for i := 8; i < len(raw); i++ {
		bad := append([]byte(nil), raw...)
		bad[i] ^= 0x40
		if err := os.WriteFile(ArrivalPath(path), bad, 0600); err != nil {
			t.Fatal(err)
		}
		compareArrivalIndex(t, path)
	}
	var length [10]byte
	n := binary.PutUvarint(length[:], 1<<20+1)
	if err := os.WriteFile(ArrivalPath(path), append([]byte("GCAGE001"), length[:n]...), 0600); err != nil {
		t.Fatal(err)
	}
	compareArrivalIndex(t, path)
}

func TestBundleMappedArrivalsSurviveRetirement(t *testing.T) {
	dir, root := bundleFixture(t)
	m, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	raw := appendArrival([]byte("GCAGE001"), "known", 123)
	if err := os.WriteFile(ArrivalPath(filepath.Join(dir, m.Cache.Name)), raw, 0600); err != nil {
		t.Fatal(err)
	}
	b, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Retire(); err != nil {
		t.Fatal(err)
	}
	slot, ok, err := b.Find("known")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	p, err := b.Read(slot)
	if err != nil || p.FirstArrival != 123 || len(p.Data) != 2 {
		t.Fatal(p, err)
	}
}

func TestCapturedMappedArrivals(t *testing.T) {
	path := os.Getenv("GO_CARBON_ARRIVAL_FIXTURE")
	if path == "" {
		t.Skip("set GO_CARBON_ARRIVAL_FIXTURE to an immutable captured companion's base path")
	}
	compareArrivalIndex(t, path)
}

func BenchmarkArrivalLoad(b *testing.B) {
	path := arrivalFixture(b, 250000)
	if fixture := os.Getenv("GO_CARBON_ARRIVAL_FIXTURE"); fixture != "" {
		path = fixture
	}
	info, _ := os.Stat(ArrivalPath(path))
	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(info.Size())
		for b.Loop() {
			if _, err := ReadArrivals(path); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("mapped", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(info.Size())
		for b.Loop() {
			got, err := openArrivalIndex(path)
			if err != nil {
				b.Fatal(err)
			}
			_ = got.data.Unmap()
		}
	})
}
