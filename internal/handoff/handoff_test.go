//go:build unix

package handoff

import (
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestHandoffKeepsSocketAccepting(t *testing.T) {
	path := socketPath(t)
	old, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := old.Addr().(*net.TCPAddr)
	serve := func(ln net.Listener, body string) *http.Server {
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) })}
		go func() { _ = srv.Serve(ln) }()
		return srv
	}
	oldSrv := serve(old, "old")
	offer, err := NewOffer(path)
	if err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Bool
	result := make(chan bool, 1)
	go func() {
		taken, err := offer.Serve(5*time.Second, old, func() {
			oldSrv.SetKeepAlivesEnabled(false)
			_ = old.Close()
			stopped.Store(true)
		})
		if err != nil {
			t.Error(err)
		}
		result <- taken
	}()

	// Clients keep hitting the address throughout; no request may fail.
	var failures, served atomic.Int32
	done := make(chan struct{})
	go func() {
		client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 2 * time.Second}
		for {
			select {
			case <-done:
				return
			default:
			}
			resp, err := client.Get("http://" + addr.String())
			if err != nil {
				failures.Add(1)
				continue
			}
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			served.Add(1)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	claim := Register(path)
	if claim == nil {
		t.Fatal("register")
	}
	time.Sleep(50 * time.Millisecond) // registered successors may take a while to request
	ln, commit, err := claim.Take(&net.TCPAddr{Port: addr.Port, IP: addr.IP})
	if err != nil || ln == nil {
		t.Fatal("take", err)
	}
	newSrv := serve(ln, "new")
	defer newSrv.Close()
	if err = commit(); err != nil || !stopped.Load() {
		t.Fatal("commit", err)
	}
	if !<-result {
		t.Fatal("offer not taken")
	}
	_ = oldSrv.Close()
	time.Sleep(100 * time.Millisecond)
	close(done)
	if failures.Load() != 0 || served.Load() == 0 {
		t.Fatal("requests failed during handoff", failures.Load(), served.Load())
	}
	resp, err := http.Get("http://" + addr.String())
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "new" {
		t.Fatal("old instance still serving", string(body))
	}
}

func TestUnclaimedAndAbandonedOffers(t *testing.T) {
	path := socketPath(t)
	if Register(path) != nil {
		t.Fatal("registered without offer")
	}
	old, _ := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	defer old.Close()
	addr := old.Addr().(*net.TCPAddr)
	result := make(chan bool, 1)
	offer := func() {
		o, err := NewOffer(path)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			taken, _ := o.Serve(5*time.Second, old, func() { t.Error("stopped without commit") })
			result <- taken
		}()
	}

	// A successor without handoff support never registers.
	saved := RegisterTimeout
	RegisterTimeout = 100 * time.Millisecond
	offer()
	if <-result {
		t.Fatal("unregistered offer reported taken")
	}
	RegisterTimeout = saved

	// A successor that dies before requesting leaves the old instance in charge.
	offer()
	time.Sleep(20 * time.Millisecond)
	Register(path).Close()
	if <-result {
		t.Fatal("abandoned offer reported taken")
	}

	// A mismatched address is refused rather than served under the wrong name.
	offer()
	time.Sleep(20 * time.Millisecond)
	if ln, _, err := Register(path).Take(&net.TCPAddr{Port: addr.Port + 1}); ln != nil || err == nil {
		t.Fatal("mismatched listener accepted")
	}
	if <-result {
		t.Fatal("mismatched offer reported taken")
	}
}

// socketPath stays below the 104-byte unix socket limit on macOS.
func socketPath(t *testing.T) string {
	dir, err := os.MkdirTemp("/tmp", "ho")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "h.sock")
}

func TestSuccessorSupported(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if !SuccessorSupported(write("new", "echo "+Protocol)) {
		t.Fatal("capable successor rejected")
	}
	if SuccessorSupported(write("old", "echo 'flag provided but not defined' >&2; exit 2")) {
		t.Fatal("old successor accepted")
	}
	if SuccessorSupported(filepath.Join(dir, "missing")) {
		t.Fatal("missing successor accepted")
	}
}
