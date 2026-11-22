package v2dict

import (
	"bytes"
	"testing"
	"time"
)

// A reader is paused while holding frozen arena bytes, as the level file
// finishes. Unmap must wait for the copy/decode, including NewIter's copy.
func TestFrozenArenaUnmapWaitsForReaders(t *testing.T) {
	for _, read := range []string{"Lookup", "LookupID", "NewIter", "AppendBatch"} {
		t.Run(read, func(t *testing.T) {
			d, err := Open(t.TempDir(), Options{NoMerge: true})
			if err != nil {
				t.Fatal(err)
			}
			name := []byte("frozen.name")
			if _, _, err = d.Append("g", 3, name, 7); err != nil {
				t.Fatal(err)
			}
			written, finishWrite := make(chan struct{}), make(chan struct{})
			reading, finishRead := make(chan struct{}), make(chan struct{})
			installing, unmapping, allowUnmap := make(chan struct{}), make(chan struct{}), make(chan struct{})
			testHook = func(p string) error {
				switch p {
				case "level-written":
					close(written)
					<-finishWrite
				case "frozen-read":
					close(reading)
					<-finishRead
				case "flush-before-install":
					close(installing)
				case "arena-unmap":
					close(unmapping)
					<-allowUnmap
				}
				return nil
			}
			if err = d.freeze(true); err != nil {
				t.Fatal(err)
			}
			<-written
			frozen := d.snap.Load().frozen[shardIx(3, len(d.shards))]
			result := make(chan bool, 1)
			go func() {
				ok := false
				switch read {
				case "Lookup":
					e, found := d.Lookup("g", 3, name)
					ok = found && e.ID == 0 && e.Policy == 7
				case "LookupID":
					n, e, found := d.LookupID("g", 3, 0, nil)
					ok = found && bytes.Equal(n, name) && e.Policy == 7
				case "NewIter":
					it := d.NewIter("g", 3)
					ok = it.SeekGE(nil) && bytes.Equal(it.Name(), name) && it.Entry().Policy == 7
				case "AppendBatch":
					n := []NewName{{Group: "g", VShard: 3, Name: string(name), Policy: 7}}
					err := d.AppendBatch(n)
					ok = err == nil && !n[0].Created && n[0].Entry.ID == 0
				}
				result <- ok
			}()
			<-reading
			close(finishWrite)
			<-installing
			early := false
			select {
			case <-unmapping:
				early = true
			case <-time.After(30 * time.Millisecond):
			}
			close(finishRead)
			var valid bool
			if early {
				// The mutation path pauses Munmap before it touches bytes.
				valid = <-result
				close(allowUnmap)
			} else {
				<-unmapping
				close(allowUnmap)
				valid = <-result
			}
			if !valid {
				t.Error("reader lost or corrupted frozen record")
			}
			d.flushWG.Wait()
			testHook = nil
			if early {
				t.Error("Munmap started before the frozen reader finished")
			}
			if len(frozen.chunks) != 0 {
				t.Error("flushed arena was not released")
			}
			if _, ok := d.Lookup("g", 3, name); !ok {
				t.Error("installed file lost name")
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCloseUnmapsActiveArena(t *testing.T) {
	d, err := Open(t.TempDir(), Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Append("g", 0, []byte("active"), 0); err != nil {
		t.Fatal(err)
	}
	if len(d.shards[0].buf.chunks) == 0 {
		t.Fatal("no mmap chunk")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if len(d.shards[0].buf.chunks) != 0 {
		t.Fatal("Close retained mmap chunks")
	}
}
