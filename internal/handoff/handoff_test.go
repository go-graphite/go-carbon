//go:build unix

package handoff

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
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
	serve := func(ln net.Listener, body string, state func(net.Conn, http.ConnState)) *http.Server {
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }), ConnState: state}
		go func() { _ = srv.Serve(ln) }()
		return srv
	}
	// Like carbonserver, the old server must answer connections it accepted
	// before it stopped accepting: http.Server drops requests read after
	// Shutdown began, so wait until its connections are idle first.
	var conns sync.Map
	oldSrv := serve(old, "old", func(c net.Conn, s http.ConnState) {
		if s == http.StateClosed || s == http.StateHijacked {
			conns.Delete(c)
		} else {
			conns.Store(c, s)
		}
	})
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
		}, nil, 0)
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
	newSrv := serve(ln, "new", nil)
	defer newSrv.Close()
	if err = commit(); err != nil || !stopped.Load() {
		t.Fatal("commit", err)
	}
	if !<-result {
		t.Fatal("offer not taken")
	}
	for busy := true; busy; {
		busy = false
		conns.Range(func(_, s any) bool { busy = s.(http.ConnState) != http.StateIdle; return !busy })
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = oldSrv.Shutdown(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
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
			taken, _ := o.Serve(5*time.Second, old, func() { t.Error("stopped without commit") }, nil, 0)
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

type inputFunc func() (*Input, error)

func (f inputFunc) Detach() (*Input, error) { return f() }

// TestInputHandoff hands over more connections than fit in one message, each
// with its own unparsed bytes, after the read listener.
func TestInputHandoff(t *testing.T) {
	path := socketPath(t)
	reads, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer reads.Close()
	input, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()

	const conns = 450
	var writers []*os.File
	detach := func() (*Input, error) {
		listener, err := input.File()
		if err != nil {
			return nil, err
		}
		in := &Input{Receivers: []InputReceiver{{Name: "tcp", Listener: listener}}}
		for i := range conns {
			r, w, err := os.Pipe()
			if err != nil {
				return nil, err
			}
			writers = append(writers, w)
			in.Receivers[0].Conns = append(in.Receivers[0].Conns, InputConn{File: r, Leftover: []byte(fmt.Sprintf("left %d", i))})
		}
		return in, nil
	}
	offer, err := NewOffer(path)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		taken, err := offer.Serve(5*time.Second, reads, func() {}, inputFunc(detach), 5*time.Second)
		if !taken {
			err = errors.Join(err, errors.New("reads not taken"))
		}
		result <- err
	}()

	claim := Register(path)
	if claim == nil {
		t.Fatal("register")
	}
	ln, commit, err := claim.Take(reads.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err = commit(); err != nil || !claim.InputOffered() {
		t.Fatal("commit", err, claim.InputOffered())
	}
	in, err := claim.TakeInput()
	if err != nil || in == nil {
		t.Fatal("take input", err, <-result)
	}
	defer in.Close()
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	if len(in.Receivers) != 1 || in.Receivers[0].Name != "tcp" || len(in.Receivers[0].Conns) != conns {
		t.Fatalf("received %+v", in.Receivers)
	}
	got, err := net.FileListener(in.Receivers[0].Listener)
	if err != nil || got.Addr().String() != input.Addr().String() {
		t.Fatal("listener", err)
	}
	got.Close()
	for i, c := range in.Receivers[0].Conns {
		if string(c.Leftover) != fmt.Sprintf("left %d", i) {
			t.Fatalf("conn %d leftover %q", i, c.Leftover)
		}
		// Each file is the matching socket, not just any descriptor.
		if _, err = fmt.Fprintf(writers[i], "%d", i); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 8)
		n, err := c.File.Read(buf)
		if err != nil || string(buf[:n]) != fmt.Sprint(i) {
			t.Fatalf("conn %d read %q %v", i, buf[:n], err)
		}
	}
	for _, w := range writers {
		w.Close()
	}
}

// TestInputAlreadyStopped answers a late request without files.
func TestInputAlreadyStopped(t *testing.T) {
	path := socketPath(t)
	reads, _ := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	defer reads.Close()
	offer, err := NewOffer(path)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := offer.Serve(5*time.Second, reads, func() {}, inputFunc(func() (*Input, error) { return nil, nil }), 5*time.Second)
		result <- err
	}()
	claim := Register(path)
	ln, commit, err := claim.Take(reads.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err = commit(); err != nil {
		t.Fatal(err)
	}
	if in, err := claim.TakeInput(); in != nil || err != nil {
		t.Fatal("took stopped input", in, err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
}

// A successor can die after it accepts reads but before it requests input.
// The offer must return promptly without detaching the still-owned input.
func TestInputRequestAbandoned(t *testing.T) {
	path := socketPath(t)
	reads, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer reads.Close()
	offer, err := NewOffer(path)
	if err != nil {
		t.Fatal(err)
	}
	var detached atomic.Bool
	done := make(chan error, 1)
	go func() {
		_, err := offer.Serve(time.Second, reads, func() {}, inputFunc(func() (*Input, error) {
			detached.Store(true)
			return nil, nil
		}), time.Second)
		done <- err
	}()
	claim := Register(path)
	if claim == nil {
		t.Fatal("register")
	}
	ln, commit, err := claim.Take(reads.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err = commit(); err != nil {
		t.Fatal(err)
	}
	_ = claim.Close()
	select {
	case err = <-done:
		if err == nil || detached.Load() {
			t.Fatalf("abandoned input request: error=%v detached=%v", err, detached.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("offer hung after successor disconnected")
	}
}
