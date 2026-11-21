package downsize

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestForwardedSourceUnavailable(t *testing.T) {
	for _, message := range []string{
		"native source export HTTP503: buckyd is stopping; retry after restart",
		"native source export HTTP503: store recovery has not finished",
		"native source export HTTP502: bad gateway",
		"native source export HTTP504: gateway timeout",
	} {
		err := fmt.Errorf("window copy: %w", &apiError{500, "target /v1/native/pull: HTTP 500 " + message})
		if !restartCopyError(err) || !transientCopyError(err) {
			t.Fatal("source unavailable treated as terminal", err)
		}
	}
	for _, message := range []string{
		"native source export HTTP403: forbidden",
		"native source export HTTP500: corrupt store",
		"native source export HTTP5030: bad checksum",
		"checksum mismatch in metric HTTP503",
	} {
		err := &apiError{500, message}
		if restartCopyError(err) || transientCopyError(err) {
			t.Fatal("genuine failure treated as restart", err)
		}
	}
}

func TestReadyReadbackWaitsForRestart(t *testing.T) {
	metrics := &progressMetrics{}
	outputMu.Lock()
	storeWaitObserver = metrics.observeStoreWait
	outputMu.Unlock()
	t.Cleanup(func() { outputMu.Lock(); storeWaitObserver = nil; outputMu.Unlock() })
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 3 {
			http.Error(w, "store starting", 503)
			return
		}
		fmt.Fprint(w, `[{"target":"metric","datapoints":[[1,100],[2,160],[3,220]]}]`)
	}))
	defer server.Close()
	b := newSyncLiveBackend(syncLiveOptions{hostWait: time.Nanosecond})
	defer b.client.CloseIdleConnections()
	b.retryTick = time.Microsecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := b.Readback(ctx, server.URL, "metric", 100, 280)
	if err != nil || calls.Load() < 4 || len(got.Values) != 3 || got.Values[2] != 3 || got.Step != 60 {
		t.Fatal(got, calls.Load(), err)
	}
	if b.waits.Load() == 0 || b.waiting.Load() != 0 {
		t.Fatal("missing retry accounting")
	}
	if metrics.storeRetries == 0 || metrics.storeWaiting != 0 || metrics.storeWaitSeconds <= 0 {
		t.Fatal("READY restart wait did not reach run metrics", metrics.storeRetries, metrics.storeWaiting, metrics.storeWaitSeconds)
	}
}

func TestReadyReadbackConsistencyFailureIsTerminal(t *testing.T) {
	for _, body := range []string{`[{"target":"wrong","datapoints":[]}]`, `[{"target":"metric","datapoints":[[1,100],[2,99]]}]`} {
		var calls atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, body) }))
		b := newSyncLiveBackend(syncLiveOptions{})
		_, err := b.Readback(context.Background(), server.URL, "metric", 100, 280)
		b.client.CloseIdleConnections()
		server.Close()
		if err == nil || calls.Load() != 1 || b.waits.Load() != 0 {
			t.Fatal("invalid data retried", calls.Load(), err)
		}
	}
}

func TestSyncOverallTimeoutDuringRestart(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "store starting", 503) }))
	defer server.Close()
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	if err := os.WriteFile(secret, []byte("test-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	err := SyncMain(context.Background(), []string{"--source", "http://source.example:4242", "--streams", "1", "--to", server.URL, "--state", filepath.Join(dir, "state"), "--jwt-secret", secret, "--timeout", "250ms"})
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() == 0 {
		t.Fatal(calls.Load(), err)
	}
	c, err := parseSyncCommand([]string{"--resume", filepath.Join(dir, "state"), "--timeout", "0"})
	if err != nil || c.timeout != 0 {
		t.Fatal("cannot remove deadline on resume", err)
	}
	_, err = parseSyncCommand([]string{"--timeout", "-1s"})
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatal("negative deadline accepted", err)
	}
}
