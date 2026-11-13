package recovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func bundleFixture(t *testing.T) (string, string) {
	t.Helper()
	dir, root := t.TempDir(), t.TempDir()
	b := NewBuilder(func(name string) bool { return name == "known" })
	var sources [2][]byte
	for file, batches := range [][]*points.Points{
		{points.OnePoint("known", 1, 1), points.OnePoint("new", 2, 1)},
		{points.OnePoint("known", 3, 1), points.OnePoint("new", 4, 2)},
	} {
		for _, p := range batches {
			raw := p.AppendBinary(nil)
			sources[file] = append(sources[file], raw...)
			if err := b.Add(file, p, len(raw)); err != nil {
				t.Fatal(err)
			}
		}
	}
	var index bytes.Buffer
	if err := b.Write(&index); err != nil {
		t.Fatal(err)
	}
	var files []File
	for i, name := range []string{"cache.123.1234.bin", "input.123.1234.bin", ".pending-index-1234.bin"} {
		data := index.Bytes()
		if i < 2 {
			data = sources[i]
		}
		path := filepath.Join(dir, name)
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = file.Write(data); err != nil {
			t.Fatal(err)
		}
		if err = file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err = file.Close(); err != nil {
			t.Fatal(err)
		}
		described, err := Describe(path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, described)
	}
	if err := Publish(dir, root, files[0], files[1], files[2], "test-read-index"); err != nil {
		t.Fatal(err)
	}
	return dir, root
}

func TestBundleRoundtripAndRetiredReader(t *testing.T) {
	dir, root := bundleFixture(t)
	b, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.Metrics() != 2 || b.Points() != 4 {
		t.Fatal("bundle totals")
	}
	slot, ok, err := b.Find("known")
	if err != nil || !ok {
		t.Fatal(err)
	}
	want := []points.Point{{Value: 1, Timestamp: 1}, {Value: 3, Timestamp: 1}}
	got, err := b.Read(slot)
	if err != nil || !reflect.DeepEqual(got.Data, want) {
		t.Fatal(got, err)
	}
	var names []string
	if err = b.NewNames(func(name string) error { names = append(names, name); return nil }); err != nil || !reflect.DeepEqual(names, []string{"new"}) {
		t.Fatal(names, err)
	}
	journal := filepath.Join(dir, "input.456.4567.bin")
	if err = os.WriteFile(journal, points.OnePoint("known", 5, 1).AppendBinary(nil), 0600); err != nil {
		t.Fatal(err)
	}
	if err = b.Retire(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(journal); err != nil {
		t.Fatal("retiring old bundle removed newer journal", err)
	}
	got, err = b.Read(slot)
	if err != nil || !reflect.DeepEqual(got.Data, want) {
		t.Fatal("retired reader lost data", got, err)
	}
	if _, err = OpenBundle(dir, root); err == nil {
		t.Fatal("retired bundle reopened")
	}
}

func TestBundleRejectsCorruptStaleOrPartialState(t *testing.T) {
	for _, scenario := range []string{"extra input", "extra cache", "wrong root", "replaced root", "version", "unsafe path", "truncated cache", "changed wal", "truncated index", "corrupt manifest"} {
		t.Run(scenario, func(t *testing.T) {
			dir, root := bundleFixture(t)
			alter := func(name string, data []byte) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "extra input":
				alter("input.456.4567.bin", nil)
			case "extra cache":
				alter("cache.456.4567.bin", nil)
			case "wrong root":
				root = t.TempDir()
			case "replaced root":
				if err := os.Rename(root, root+"-old"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(root + "-old") })
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
			case "version", "unsafe path":
				raw, err := os.ReadFile(filepath.Join(dir, manifestName))
				if err != nil {
					t.Fatal(err)
				}
				var m Manifest
				if err = json.Unmarshal(raw, &m); err != nil {
					t.Fatal(err)
				}
				if scenario == "version" {
					m.Version++
				} else {
					m.Index.Name = "../.pending-index-1234.bin"
				}
				raw, err = json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				alter(manifestName, raw)
			case "truncated cache":
				alter("cache.123.1234.bin", nil)
			case "changed wal":
				raw, err := os.ReadFile(filepath.Join(dir, "input.123.1234.bin"))
				if err != nil {
					t.Fatal(err)
				}
				raw[len(raw)-1] ^= 1
				alter("input.123.1234.bin", raw)
			case "truncated index":
				alter(".pending-index-1234.bin", nil)
			case "corrupt manifest":
				alter(manifestName, make([]byte, 2<<20))
			}
			if b, err := OpenBundle(dir, root); err == nil {
				_ = b.Close()
				t.Fatal("accepted", scenario)
			}
		})
	}
}

func TestLargeManifestRoundtrip(t *testing.T) {
	dir, root := bundleFixture(t)
	raw, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	// Exercise the complete publisher/reader boundary beyond the former 16 KiB
	// and 1 MiB limits without constructing several GiB of source data just
	// for digests.
	for _, size := range []int{32 << 10, 2 << 20} {
		id := strings.Repeat("x", size)
		if err = Publish(dir, root, m.Cache, m.WAL, m.Index, id); err != nil {
			t.Fatal(err)
		}
		b, err := OpenBundle(dir, root)
		if err != nil {
			t.Fatal(size, err)
		}
		if b.ReadIndexID() != id || b.Points() != 4 {
			t.Fatal("large manifest lost identity or data", size)
		}
		if err = b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestManifestOfLargeCheckpoint publishes the digests of a 200 GB cache dump,
// over 1 MiB of JSON, and reads them back unchanged.
func TestManifestOfLargeCheckpoint(t *testing.T) {
	dir, root := t.TempDir(), t.TempDir()
	describe := func(name string, chunks int) File {
		f := File{Name: name, Size: int64(chunks) * checksumChunkSize, ChunkSize: checksumChunkSize, Chunks: make([][32]byte, chunks)}
		for i := range f.Chunks {
			for j := range f.Chunks[i] {
				f.Chunks[i][j] = byte(i*31 + j*7 + 0x80)
			}
		}
		return f
	}
	cache := describe(BlocksPrefix+"123.1234.bin", 12000)
	wal := describe(CompactInputPrefix+"123.1234.bin", 150)
	index := describe(".pending-index-1234.bin", 80)
	if err := Publish(dir, root, cache, wal, index, "id"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= 1<<20 {
		t.Fatalf("manifest is %d bytes; the test needs more than 1 MiB", info.Size())
	}
	m, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != 4 || m.ReadIndexID != "id" || !reflect.DeepEqual(m.Cache, cache) || !reflect.DeepEqual(m.WAL, wal) || !reflect.DeepEqual(m.Index, index) {
		t.Fatal("large manifest changed in a round trip")
	}
}

// TestBundleAcceptsItsTail opens a checkpoint next to the input that arrived
// after it, but not next to another generation's tail.
func TestBundleAcceptsItsTail(t *testing.T) {
	dir, root := bundleFixture(t)
	if got := TailName("input.123.1234.bin"); got != "input.123.1234.tail.bin" || !IsTail(got) || IsTail("input.123.1234.bin") {
		t.Fatal("tail name", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "input.123.1234.tail.bin"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	b, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err = os.WriteFile(filepath.Join(dir, "input.123.999.tail.bin"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenBundle(dir, root); err == nil || !strings.Contains(err.Error(), "unindexed") {
		t.Fatal("opened next to another generation's tail", err)
	}
}

func TestCaptureTailsShareCheckpointAuthority(t *testing.T) {
	dir, root := bundleFixture(t)
	tail := TailName("input.123.1234.bin")
	for i := range 32 {
		name := fmt.Sprintf("%s.capture-%02d.tail.bin", tail, i)
		if !IsTail(name) || !IsGenerationTail(name, "input.123.1234.bin") {
			t.Fatal(name)
		}
		if err := os.WriteFile(filepath.Join(dir, name), points.OnePoint("known", float64(i), 2).AppendBinary(nil), 0600); err != nil {
			t.Fatal(err)
		}
	}
	b, err := OpenBundle(dir, root)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Retire(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, tail+"*"))
	if err != nil || len(files) != 0 {
		t.Fatal(files, err)
	}
	if IsGenerationTail(tail+".capture-00.tail.bin", "input.999.1234.bin") {
		t.Fatal("another generation matched")
	}
}
