package helper

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestListenTCPRetriesUntilPredecessorReleasesPort(t *testing.T) {
	old, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	addr := old.Addr().(*net.TCPAddr)
	before := listenerBindRetries.Load()
	result := make(chan *net.TCPListener, 1)
	failure := make(chan error, 1)
	go func() {
		ln, err := listenTCP("tcp", addr, time.Second, time.Millisecond)
		if err != nil {
			failure <- err
			return
		}
		result <- ln
	}()
	deadline := time.Now().Add(time.Second)
	for listenerBindRetries.Load() == before {
		if time.Now().After(deadline) {
			t.Fatal("bind was not retried")
		}
		time.Sleep(time.Millisecond)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case ln := <-result:
		defer ln.Close()
		if ln.Addr().String() != addr.String() {
			t.Fatal(ln.Addr())
		}
	case err := <-failure:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("bind did not finish")
	}
}

func TestListenTCPRetryDeadlineAndNonBindError(t *testing.T) {
	old, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	start := time.Now()
	_, err = listenTCP("tcp", old.Addr().(*net.TCPAddr), 20*time.Millisecond, time.Millisecond)
	if !errors.Is(err, syscall.EADDRINUSE) || time.Since(start) > time.Second {
		t.Fatalf("deadline: %v after %s", err, time.Since(start))
	}
	before := listenerBindRetries.Load()
	_, err = listenTCP("invalid", nil, time.Hour, time.Hour)
	if err == nil || listenerBindRetries.Load() != before {
		t.Fatalf("non-bind error retried: %v", err)
	}
}
