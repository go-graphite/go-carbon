package v2dict

import (
	"testing"
	"time"
)

func TestAdmissionCloseDuringSingleRewrite(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = d.Append("g", 0, []byte("durable"), 0); err != nil {
		t.Fatal(err)
	}
	if err = d.Flush(); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	testHook = func(p string) error {
		if p == "level-written" {
			close(entered)
			<-release
		}
		return nil
	}
	defer func() { testHook = nil }()
	merged := make(chan error, 1)
	go func() { merged <- d.merge(d.snap.Load().levels) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("rewrite not entered")
	}
	closed := make(chan error, 1)
	go func() { closed <- d.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		d.mu.RLock()
		v := d.closed
		d.mu.RUnlock()
		if v {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("Close did not enter")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	<-merged
	if err = <-closed; err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, Options{NoMerge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, ok := d.Lookup("g", 0, []byte("durable")); !ok {
		t.Fatal("durable name lost when Close interrupted a same-path rewrite")
	}
}
