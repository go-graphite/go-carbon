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
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == "-handoff-protocol" {
		switch os.Getenv("GO_CARBON_HANDOFF_TEST_PROTOCOL") {
		case "supported":
			fmt.Println(Protocol)
			os.Exit(0)
		case "unsupported":
			fmt.Fprintln(os.Stderr, "flag provided but not defined")
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}

func TestHandoffKeepsSocketAccepting(t *testing.T) {
	path := socketPath(t)
	old, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	addr := old.Addr().(*net.TCPAddr)
	serve := func(ln net.Listener, body string, state func(net.Conn, http.ConnState)) (*http.Server, <-chan struct{}) {
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }), ConnState: state}
		done := make(chan struct{})
		go func() { defer close(done); _ = srv.Serve(ln) }()
		t.Cleanup(func() { _ = srv.Close() })
		return srv, done
	}
	// Drain connections accepted before the old listener closed. ConnState
	// wakes the waiter after every transition, including the final close.
	idle := sync.NewCond(new(sync.Mutex))
	conns := make(map[net.Conn]http.ConnState)
	oldSrv, oldDone := serve(old, "old", func(c net.Conn, s http.ConnState) {
		idle.L.Lock()
		defer idle.L.Unlock()
		if s == http.StateClosed || s == http.StateHijacked {
			delete(conns, c)
		} else {
			conns[c] = s
		}
		idle.Broadcast()
	})
	offer, err := NewOffer(path)
	if err != nil {
		t.Fatal(err)
	}
	defer offer.Close()
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

	// Keep making requests across all three phases, and wait for completed
	// requests instead of assuming the client ran during a fixed sleep.
	type response struct {
		phase int32
		body  string
	}
	responses := make(chan response, 1)
	var phase atomic.Int32
	var failures, served atomic.Int32
	done, clientDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(clientDone)
		transport := &http.Transport{DisableKeepAlives: true}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
		for {
			select {
			case <-done:
				return
			default:
			}
			current := phase.Load()
			resp, err := client.Get("http://" + addr.String())
			if err != nil {
				failures.Add(1)
				continue
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				failures.Add(1)
				continue
			}
			served.Add(1)
			select {
			case responses <- response{current, string(body)}:
			default:
			}
		}
	}()
	var stopClient sync.Once
	stop := func() { stopClient.Do(func() { close(done); <-clientDone }) }
	defer stop()
	waitResponse := func(wantPhase int32, wantBody string) {
		t.Helper()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case got := <-responses:
				if got.phase == wantPhase && got.body == wantBody {
					return
				}
			case <-timer.C:
				t.Fatalf("no %s response during handoff phase %d", wantBody, wantPhase)
			}
		}
	}
	waitResponse(0, "old")
	claim := Register(path)
	if claim == nil {
		t.Fatal("register")
	}
	defer claim.Close()
	phase.Store(1)
	waitResponse(1, "old") // a registered successor can delay requesting
	ln, commit, err := claim.Take(&net.TCPAddr{Port: addr.Port, IP: addr.IP})
	if err != nil || ln == nil {
		t.Fatal("take", err)
	}
	_, _ = serve(ln, "new", nil)
	if err = commit(); err != nil || !stopped.Load() {
		t.Fatal("commit", err)
	}
	if !<-result {
		t.Fatal("offer not taken")
	}
	<-oldDone // no more connections can enter conns
	idle.L.Lock()
	for {
		busy := false
		for _, s := range conns {
			busy = busy || s != http.StateIdle
		}
		if !busy {
			break
		}
		idle.Wait()
	}
	idle.L.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = oldSrv.Shutdown(ctx); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	phase.Store(2)
	waitResponse(2, "new")
	stop()
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
	Register(path).Close()
	if <-result {
		t.Fatal("abandoned offer reported taken")
	}

	// A mismatched address is refused rather than served under the wrong name.
	offer()
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
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GO_CARBON_HANDOFF_TEST_PROTOCOL", "supported")
	if !SuccessorSupported(binary) {
		t.Fatal("capable successor rejected")
	}
	t.Setenv("GO_CARBON_HANDOFF_TEST_PROTOCOL", "unsupported")
	if SuccessorSupported(binary) {
		t.Fatal("old successor accepted")
	}
	if SuccessorSupported(filepath.Join(t.TempDir(), "missing")) {
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

// TestStoreInputHandoff hands input over without a read listener: the
// successor's listener request is answered with "input only", it binds reads
// itself and takes input afterwards.
func TestStoreInputHandoff(t *testing.T) {
	// The registered successor is this test binary, which knows no
	// handoff flags: stand in a capable binary for it.
	peerIs(t, protocolScript(t, StoreInputProtocol))
	path := socketPath(t)
	input, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	detached := false
	detach := func() (*Input, error) {
		detached = true
		listener, err := input.File()
		if err != nil {
			return nil, err
		}
		return &Input{Receivers: []InputReceiver{{Name: "tcp", Listener: listener,
			Conns: []InputConn{{File: r, Leftover: []byte("partial lin")}}}}}, nil
	}
	offer, err := NewOffer(path)
	if err != nil {
		t.Fatal(err)
	}
	type served struct {
		taken bool
		err   error
	}
	result := make(chan served, 1)
	go func() {
		taken, err := offer.ServeInput(inputFunc(detach), 5*time.Second)
		result <- served{taken, err}
	}()

	claim := Register(path)
	if claim == nil {
		t.Fatal("register")
	}
	ln, commit, err := claim.Take(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	if err != nil || ln != nil || commit != nil {
		t.Fatal("input-only offer returned a listener", ln, err)
	}
	if !claim.InputOffered() {
		t.Fatal("input not offered")
	}
	if detached {
		t.Fatal("input detached before the successor asked for it")
	}
	in, err := claim.TakeInput()
	if err != nil || in == nil {
		t.Fatal("take input", err)
	}
	defer in.Close()
	if got := <-result; !got.taken || got.err != nil {
		t.Fatalf("serve input: %+v", got)
	}
	if len(in.Receivers) != 1 || len(in.Receivers[0].Conns) != 1 || string(in.Receivers[0].Conns[0].Leftover) != "partial lin" {
		t.Fatalf("received %+v", in.Receivers)
	}
	got, err := net.FileListener(in.Receivers[0].Listener)
	if err != nil || got.Addr().String() != input.Addr().String() {
		t.Fatal("listener", err)
	}
	got.Close()
	if _, err = w.WriteString("e 1 2\n"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if n, err := in.Receivers[0].Conns[0].File.Read(buf); err != nil || string(buf[:n]) != "e 1 2\n" {
		t.Fatalf("connection read %q %v", buf[:n], err)
	}
}

// TestStoreInputAbandoned covers a successor that dies before taking input
// and one that never asks: the stopping instance keeps its input either way.
func TestStoreInputAbandoned(t *testing.T) {
	peerIs(t, protocolScript(t, StoreInputProtocol))
	for _, abandonAfterTake := range []bool{false, true} {
		path := socketPath(t)
		offer, err := NewOffer(path)
		if err != nil {
			t.Fatal(err)
		}
		detached := false
		result := make(chan error, 1)
		go func() {
			taken, err := offer.ServeInput(inputFunc(func() (*Input, error) { detached = true; return nil, nil }), time.Second)
			if taken {
				err = errors.Join(err, errors.New("taken"))
			}
			result <- err
		}()
		claim := Register(path)
		if claim == nil {
			t.Fatal("register")
		}
		if abandonAfterTake {
			if _, _, err := claim.Take(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}); err != nil {
				t.Fatal(err)
			}
		}
		_ = claim.Close()
		if err := <-result; err == nil {
			t.Fatal("abandoned handoff reported no error")
		}
		if detached {
			t.Fatal("input detached for an abandoned claim")
		}
	}
}

// protocolScript is a binary printing protocol for any flag, or rejecting
// every flag as a binary without it does when protocol is empty.
func protocolScript(t *testing.T, protocol string) string {
	t.Helper()
	body := "#!/bin/sh\nexit 2\n"
	if protocol != "" {
		body = "#!/bin/sh\necho " + protocol + "\n"
	}
	path := filepath.Join(t.TempDir(), "next")
	if err := os.WriteFile(path, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	// The first run of a new executable can stall for seconds on macOS
	// while it is assessed: run it once outside the timed checks.
	_ = exec.Command(path, "-store-input-handoff-protocol").Run()
	return path
}

// peerIs makes every registered successor run binary.
func peerIs(t *testing.T, binary string) {
	t.Helper()
	saved := PeerExecutable
	PeerExecutable = func(*net.UnixConn) (string, error) { return binary, nil }
	t.Cleanup(func() { PeerExecutable = saved })
}

// TestStoreInputChecksRegisteredBinary covers a successor that registers but
// runs a binary without store input handoff, as after a rollback through a
// service override: the offer gives input up as soon as it registers, without
// waiting for a request that never comes. A binary that cannot be run says
// nothing about support, so the offer waits as before.
func TestStoreInputChecksRegisteredBinary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		binary  string
		refused bool
	}{
		{"binary without support", protocolScript(t, ""), true},
		{"older protocol", protocolScript(t, InputProtocol), true},
		{"capable binary", protocolScript(t, StoreInputProtocol), false},
		{"binary not executable", filepath.Join(t.TempDir(), "missing"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peerIs(t, tc.binary)
			path := socketPath(t)
			offer, err := NewOffer(path)
			if err != nil {
				t.Fatal(err)
			}
			detached := false
			type served struct {
				stats ServeInputStats
				taken bool
				err   error
			}
			result := make(chan served, 1)
			go func() {
				stats, taken, err := offer.ServeInputStats(inputFunc(func() (*Input, error) { detached = true; return nil, nil }), 2*time.Second)
				result <- served{stats, taken, err}
			}()
			claim := Register(path)
			if claim == nil {
				t.Fatal("register")
			}
			defer claim.Close()
			started := time.Now()
			got := <-result
			elapsed := time.Since(started)
			if got.taken || detached {
				t.Fatalf("input taken: %+v detached %v", got, detached)
			}
			if got.stats.Peer != tc.binary || got.stats.Registered.IsZero() {
				t.Fatalf("stats %+v", got.stats)
			}
			if refused := errors.Is(got.err, ErrSuccessorCannotTakeInput); refused != tc.refused {
				t.Fatalf("refused %v, want %v: %v", refused, tc.refused, got.err)
			}
			if tc.refused && elapsed > time.Second {
				t.Fatalf("refusal waited %v for a request", elapsed)
			}
			if !tc.refused && elapsed < 2*time.Second {
				t.Fatalf("gave input up after %v without a request: %v", elapsed, got.err)
			}
		})
	}
}
