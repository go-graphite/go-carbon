package downsize

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNativeLifecycleBusyRetry(t *testing.T) {
	for _, recover := range []bool{true, false} {
		t.Run(map[bool]string{true: "recovers", false: "cancels"}[recover], func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/transfer/status" {
					w.Write([]byte("{}"))
					return
				}
				calls++
				if !recover || calls == 1 {
					http.Error(w, "application lifecycle is busy; retry native copy", 500)
					return
				}
				w.Write([]byte("{}"))
			}))
			defer server.Close()
			m := &mover{client: server.Client()}
			m.stallTimeout = time.Minute
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_, err := m.requestOnce(ctx, server.URL, "/v1/native/pull", struct{}{}, nil)
			if recover {
				if err != nil || calls != 2 {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		})
	}
	if transientCopyError(&apiError{Status: 500, Message: "HTTP 500 unrelated failure"}) {
		t.Fatal("unrelated server failure retried")
	}
}
