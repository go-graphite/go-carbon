package storehttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// The old process keeps established keepalives while a new process accepts on
// the identical listener address. Every API must escape without caller retries.
func TestRestartRedialsEveryRequestPath(t *testing.T) {
	for _, path := range []string{"/v1/native/capabilities", "/v1/native/export", "/v1/native/pull", "/v1/transfer/import", "/v1/transfer/digest", "/metrics", "/metrics/find/", "/render/", "/v1/native/receipt/id"} {
		t.Run(path, func(t *testing.T) {
			var stopping atomic.Bool
			var oldCalls, newCalls atomic.Int64
			old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if stopping.Load() {
					oldCalls.Add(1)
					http.Error(w, "buckyd is stopping; retry after restart", 503)
					return
				}
				fmt.Fprint(w, "ok")
			}))
			defer old.Close()
			tr := NewTransport(&http.Transport{})
			client := &http.Client{Transport: tr, Timeout: time.Second}
			defer client.CloseIdleConnections()
			call := func() {
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, old.URL+path, strings.NewReader("same durable identity"))
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				b, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || resp.StatusCode != 200 || string(b) != "ok" {
					t.Fatalf("%s %q %v", resp.Status, b, err)
				}
			}
			call()
			stopping.Store(true)
			addr := old.Listener.Addr().String()
			old.Listener.Close()
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			next := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				newCalls.Add(1)
				b, _ := io.ReadAll(r.Body)
				if string(b) != "same durable identity" {
					t.Errorf("replay body changed: %q", b)
				}
				fmt.Fprint(w, "ok")
			}))
			next.Listener = ln
			next.Start()
			defer next.Close()
			call()
			if oldCalls.Load() != 1 || newCalls.Load() != 1 {
				t.Fatal("expected one rejected request and one redial", oldCalls.Load(), newCalls.Load())
			}
		})
	}
}

func TestRestartPreservesOtherOriginsAndErrors(t *testing.T) {
	var stopping atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stopping.Load() {
			http.Error(w, "not ready", 503)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()
	tr := NewTransport(&http.Transport{})
	client := &http.Client{Transport: tr}
	defer client.CloseIdleConnections()
	other := tr.pool("http://unrelated")
	for _, want := range []int{200, 503} {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want || want == 503 && string(b) != "not ready\n" {
			t.Fatal(resp.Status, string(b))
		}
		stopping.Store(true)
	}
	if tr.pool("http://unrelated") != other {
		t.Fatal("unrelated pool replaced")
	}
}

func TestReusedConnectionEOFRedialsReplayablePost(t *testing.T) {
	var cut atomic.Bool
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		if cut.CompareAndSwap(true, false) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()
	client := &http.Client{Transport: NewTransport(&http.Transport{}), Timeout: time.Second}
	defer client.CloseIdleConnections()
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/native/export", strings.NewReader("durable identity"))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		cut.Store(true)
	}
	if calls.Load() != 3 {
		t.Fatal("reset connection was not retried exactly once", calls.Load())
	}
}

func TestMutationLostAckReturnsToJournalOwner(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/warm" {
			fmt.Fprint(w, "ok")
			return
		}
		io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer srv.Close()
	client := &http.Client{Transport: NewTransport(&http.Transport{}), Timeout: time.Second}
	defer client.CloseIdleConnections()
	resp, err := client.Get(srv.URL + "/warm")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/transfer/import", strings.NewReader("mutation"))
	if resp, err := client.Do(req); err == nil {
		resp.Body.Close()
		t.Fatal("lost acknowledgement was hidden")
	}
	if calls.Load() != 2 {
		t.Fatal("mutation replayed without checking its receipt", calls.Load())
	}
}

func TestInterruptedResponseRetiresOrigin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		fmt.Fprint(w, "partial")
	}))
	defer srv.Close()
	tr := NewTransport(&http.Transport{})
	var restarts atomic.Int64
	tr.OnRestart = func(string) { restarts.Add(1) }
	client := &http.Client{Transport: tr}
	defer client.CloseIdleConnections()
	old := tr.pool(srv.URL)
	resp, err := client.Get(srv.URL + "/v1/native/export")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !errors.Is(err, io.ErrUnexpectedEOF) || restarts.Load() != 1 || tr.pool(srv.URL) == old {
		t.Fatalf("interrupted response retained pool: %v restarts=%d", err, restarts.Load())
	}
}

// Local dial exhaustion must leave healthy in-flight keepalives reusable.
func TestLocalExhaustionPreservesPool(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			close(entered)
			<-release
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()
	var dials, restarts atomic.Int64
	tr := NewTransport(&http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if dials.Add(1) > 1 {
			return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.EADDRNOTAVAIL}
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}})
	tr.OnRestart = func(string) { restarts.Add(1) }
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	call := func(path string) error {
		resp, err := client.Get(srv.URL + path)
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return err
	}
	old := tr.pool(srv.URL)
	held := make(chan error, 1)
	go func() { held <- call("/hold") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("request not started")
	}
	err := call("/fail")
	close(release)
	if heldErr := <-held; heldErr != nil {
		t.Fatal(heldErr)
	}
	if !errors.Is(err, syscall.EADDRNOTAVAIL) {
		t.Fatalf("unexpected error: %v", err)
	}
	if tr.pool(srv.URL) != old || restarts.Load() != 0 {
		t.Fatal("local failure retired healthy pool")
	}
	if err := call("/reuse"); err != nil {
		t.Fatalf("healthy connection lost: %v", err)
	}
	if dials.Load() != 2 {
		t.Fatalf("did not reuse connection: %d dials", dials.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := tr.RoundTrip(req)
	if resp != nil {
		resp.Body.Close()
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if tr.pool(srv.URL) != old || restarts.Load() != 0 {
		t.Fatal("cancellation retired healthy pool")
	}
}

func TestTransportPoolLimits(t *testing.T) {
	tr := NewTransport(&http.Transport{})
	if tr.MaxConnsPerHost != 1024 || tr.IdleConnTimeout != 90*time.Second {
		t.Fatal("unbounded default pool")
	}
	tr = NewTransport(&http.Transport{MaxConnsPerHost: 17, IdleConnTimeout: time.Second})
	if tr.MaxConnsPerHost != 17 || tr.IdleConnTimeout != time.Second {
		t.Fatal("explicit limits changed")
	}
}
