package downsize

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSyncWindowSelectionQueueDoesNotReachDonor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	source := nativeControllerStore(t)
	addPoints(t, source, "metric", 1)
	handler := nativeControllerService(t, source)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	var requests atomic.Int32
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/export" {
			requests.Add(1)
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer donor.Close()
	defer once.Do(func() { close(release) })
	b := newSyncLiveBackend(syncLiveOptions{})
	finish := b.beginWindowCopy()
	defer finish()
	done := make(chan error, 1)
	go func() {
		_, err := b.WindowPresence(ctx, donor.URL, []string{"metric"}, syncRange{From: testMetricNow - 20, Until: testMetricNow})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queued, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	if _, err := b.WindowPresence(queued, donor.URL, []string{"metric"}, syncRange{From: testMetricNow - 20, Until: testMetricNow}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatal("queued selection reached donor", requests.Load())
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := b.WindowPresence(ctx, donor.URL, []string{"metric"}, syncRange{From: testMetricNow - 20, Until: testMetricNow}); err != nil {
		t.Fatal("slot not released", err)
	}
}
