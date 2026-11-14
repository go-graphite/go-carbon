package downsize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestSyncDigestAllowsDelayedHeaders(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-time.After(180 * time.Millisecond):
			fmt.Fprint(w, `[{"Name":"a","Found":true,"Archives":[{"Step":60,"Count":2,"Hash":"0123456789abcdef"}]}]`)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	// Scale the old two-minute deadline to reproduce discarded work quickly.
	old := &http.Client{Timeout: time.Second, Transport: &http.Transport{ResponseHeaderTimeout: 30 * time.Millisecond}}
	defer old.CloseIdleConnections()
	if resp, err := old.Get(server.URL); err == nil {
		resp.Body.Close()
		t.Fatal("legacy header timeout did not discard delayed work")
	}
	calls.Store(0)
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second})
	defer b.m.client.CloseIdleConnections()
	if b.m.client.Timeout != 10*time.Minute {
		t.Fatal("overall request deadline changed")
	}
	if tr := b.m.client.Transport.(*http.Transport); tr.ResponseHeaderTimeout != 0 {
		t.Fatal("sync still discards batches at a separate header deadline")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := b.Digest(ctx, server.URL, []string{"a"}, 60, 180)
	want := map[string][]syncDigest{"a": {{Step: 60, Count: 2, Hash: "0123456789abcdef"}}}
	if err != nil || !reflect.DeepEqual(got, want) || calls.Load() != 1 || b.waits.Load() != 0 {
		t.Fatalf("digest=%+v err=%v requests=%d retries=%d", got, err, calls.Load(), b.waits.Load())
	}
}

func TestSyncDigestDelayedHeadersRemainCancellable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer server.Close()
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Second})
	defer b.m.client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := b.Digest(ctx, server.URL, []string{"a"}, 60, 180); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation: %v", err)
	}
	if b.waits.Load() != 0 {
		t.Fatal("cancelled request was retried")
	}
}
