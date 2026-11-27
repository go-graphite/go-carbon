package v2dict

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A slow first flush must bound the next table while allowing frozen reads.
// After it resumes, every appended ID must survive flush and reopen.
func TestAppendWaitsForFrozenLevel(t *testing.T) {
	const limit = 32 << 10
	root := t.TempDir()
	d, err := Open(root, Options{BufferBytes: limit, Shards: 1, NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.Append("g", 0, []byte("first"), 0); err != nil {
		t.Fatal(err)
	}
	written, resume, waiting := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	var once sync.Once
	var finished chan struct{}
	var held atomic.Bool
	testHook = func(point string) error {
		if point == "level-written" && held.CompareAndSwap(false, true) {
			close(written)
			<-resume
		}
		if point == "append-wait" {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
		return nil
	}
	defer func() {
		once.Do(func() { close(resume) })
		if finished != nil {
			<-finished
		}
		d.Close()
		testHook = nil
	}()
	if err = d.freeze(true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not reach write")
	}
	names := make([]NewName, 8192)
	for i := range names {
		names[i] = NewName{Group: "g", Name: fmt.Sprintf("%06d.%s", i, strings.Repeat("x", 80))}
	}
	done := make(chan error, 1)
	finished = make(chan struct{})
	go func() { defer close(finished); done <- d.AppendBatch(names) }()
	select {
	case <-waiting:
	case err := <-done:
		t.Fatalf("append escaped a full table while its previous flush was blocked: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("append never reached its memory bound")
	}
	if size := d.bufSize.Load(); size > limit+batchNames*128 {
		t.Errorf("append level grew to %d bytes with a blocked flush", size)
	}
	if e, ok := d.Lookup("g", 0, []byte("first")); !ok || e.ID != 0 {
		t.Fatal("frozen lookup blocked or lost its entry")
	}
	select {
	case err := <-done:
		t.Fatalf("append did not wait for frozen level: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(resume) })
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("append failed to resume")
	}
	if err = d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(root, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, n := range names {
		e, ok := d.Lookup("g", 0, []byte(n.Name))
		if !ok || e != n.Entry || e.ID != uint32(i+1) {
			t.Fatalf("name %d lost across reopen: %+v %v", i, e, ok)
		}
	}
}
