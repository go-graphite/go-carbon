package v2dict

import (
	"bytes"
	"encoding/binary"
	"runtime"
	"testing"
	"time"
)

// A frozen reader owns its heap arena through its snapshot, even after the
// file replaces it. Installing the file never waits for the reader.
func TestFrozenArenaReadersSurviveInstall(t *testing.T) {
	for _, read := range []string{"Lookup", "LookupID", "NewIter", "AppendBatch"} {
		t.Run(read, func(t *testing.T) {
			d, err := Open(t.TempDir(), Options{NoMerge: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = d.Append("g", 3, []byte("name"), 7); err != nil {
				t.Fatal(err)
			}
			written, write := make(chan struct{}), make(chan struct{})
			reading, resume := make(chan struct{}), make(chan struct{})
			installed := make(chan struct{})
			testHook = func(p string) error {
				switch p {
				case "level-written":
					close(written)
					<-write
				case "frozen-read":
					close(reading)
					<-resume
				case "flush-installed":
					close(installed)
				}
				return nil
			}
			if err = d.freeze(true); err != nil {
				t.Fatal(err)
			}
			<-written
			result := make(chan bool, 1)
			go func() {
				ok := false
				defer func() {
					if recover() != nil {
						ok = false
					}
					result <- ok
				}()
				switch read {
				case "Lookup":
					e, found := d.Lookup("g", 3, []byte("name"))
					ok = found && e.Policy == 7
				case "LookupID":
					n, e, found := d.LookupID("g", 3, 0, nil)
					ok = found && string(n) == "name" && e.Policy == 7
				case "NewIter":
					it := d.NewIter("g", 3)
					ok = it.SeekGE(nil) && string(it.Name()) == "name" && it.Entry().Policy == 7
				case "AppendBatch":
					n := []NewName{{Group: "g", VShard: 3, Name: "name", Policy: 7}}
					err := d.AppendBatch(n)
					ok = err == nil && !n[0].Created && n[0].Entry.Policy == 7
				}
			}()
			<-reading
			close(write)
			select {
			case <-installed:
			case <-time.After(time.Second):
				t.Error("file installation waited for a frozen reader")
			}
			runtime.GC()
			close(resume)
			if !<-result {
				t.Error("reader lost its frozen arena")
			}
			d.flushWG.Wait()
			testHook = nil
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Sparse shards reserve at most one small chunk; growing never moves an
// existing record. This catches restoring the 64 MiB minimum allocation.
func TestArenaChunkBoundaryAndSlack(t *testing.T) {
	var table memtable
	key := append(scopeKey(nil, "g", 2), bytes.Repeat([]byte("n"), 4000)...)
	var val [valueSize]byte
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := range 17 {
		binary.LittleEndian.PutUint32(key[len(key)-4:], uint32(i))
		binary.LittleEndian.PutUint32(val[:], uint32(i))
		table.put(key, val[:])
	}
	runtime.ReadMemStats(&after)
	if len(table.chunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(table.chunks))
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 256<<10 {
		t.Fatalf("arena allocated %d heap bytes, want <=256 KiB", n)
	}
	for i := range 17 {
		k, v := table.rec(uint32(i))
		if binary.LittleEndian.Uint32(k[len(k)-4:]) != uint32(i) || binary.LittleEndian.Uint32(v) != uint32(i) {
			t.Fatalf("record %d corrupted", i)
		}
	}
}
