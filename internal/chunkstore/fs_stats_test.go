package chunkstore

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestFSKind(t *testing.T) {
	for name, want := range map[string]int{
		"/s/000123.sst": FSTable, "/s/ingest/base-t5.sst": FSIngest, "/s/.sst": FSIngest,
		"/s/000004.log": FSWAL, "/s/MANIFEST-000001": FSManifest, "/s/CHUNKSTORE": FSOther, "/s/OPTIONS-000003": FSOther,
	} {
		if got := fsKind(name); got != want {
			t.Errorf("fsKind(%q) = %s, want %s", name, FSKindNames[got], FSKindNames[want])
		}
	}
}

// Flushes and compactions show up as table creates, writes, syncs, reads and
// removes, WAL and manifest writes, and compaction kinds; deferred table syncs
// are counted when their batch runs.
func TestStoreCountsFilesystemOperations(t *testing.T) {
	for _, parallel := range []int{0, 8} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			s, _ := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ParallelTableSyncs: parallel, MemTableSize: 1 << 20})
			defer closeTestStore(t, s)
			for i := range 300 {
				createTestMetric(t, s, fmt.Sprintf("fs.counted.%03d", i))
				if i%100 == 99 {
					if err := s.Flush(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := s.Compact(); err != nil {
				t.Fatal(err)
			}
			st := s.Stats()
			for _, c := range []struct {
				op, kind int
			}{{FSCreate, FSTable}, {FSWrite, FSTable}, {FSSync, FSTable}, {FSRead, FSTable}, {FSRemove, FSTable}, {FSWrite, FSWAL}, {FSWrite, FSManifest}, {FSSync, FSOther}} {
				if st.FS.Ops[c.op][c.kind] == 0 {
					t.Errorf("no %s %s operations counted", FSOpNames[c.op], FSKindNames[c.kind])
				}
			}
			if st.FS.Bytes[FSWrite][FSTable] == 0 || st.FS.Bytes[FSRead][FSTable] == 0 {
				t.Errorf("table bytes not counted: %+v", st.FS.Bytes)
			}
			k := st.CompactionKinds
			if k.Default+k.Move+k.DeleteOnly+k.ElisionOnly+k.Read+k.Rewrite == 0 {
				t.Errorf("no compaction kinds counted: %+v", k)
			}
		})
	}
}

// BenchmarkCountingFileRead is the per-read cost the counters add.
func BenchmarkCountingFileRead(b *testing.B) {
	mem := vfs.NewMem()
	f, err := mem.Create("/000001.sst")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 4096)); err != nil {
		b.Fatal(err)
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 4096)
	for _, counted := range []bool{false, true} {
		b.Run(fmt.Sprintf("counted=%v", counted), func(b *testing.B) {
			var fs vfs.FS = mem
			if counted {
				fs = newCountingFS(mem)
			}
			r, err := fs.Open("/000001.sst")
			if err != nil {
				b.Fatal(err)
			}
			defer r.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.ReadAt(buf, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
