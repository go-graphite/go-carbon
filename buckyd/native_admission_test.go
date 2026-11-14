package buckyd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativePullConcurrentAdmissionAndQueuedCancellation(t *testing.T) {
	for _, imports := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(imports), func(t *testing.T) { testNativePullAdmission(t, imports) })
	}
}

func testNativePullAdmission(t *testing.T, imports int) {
	t.Helper()
	destination, target := newNativeTestService(t, imports)
	admitted := imports
	if admitted == 0 {
		admitted = 1
	}
	started := make(chan struct{}, 3)
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Store-Format", fmt.Sprint(target.TransferFormat()))
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer donor.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	root := token(t, destination.secret, []string{"*"}, []string{"*"})
	start := func(i int, ctx context.Context) <-chan struct{} {
		done := make(chan struct{})
		body, err := json.Marshal(nativePullRequest{Range: chunkstore.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: fmt.Sprintf("%064x", i+3), Source: donor.URL, End: 1, Names: []string{fmt.Sprint("admitted.", i)}, Combine: true}, Session: "pin"})
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, "/v1/native/pull", bytes.NewReader(body)).WithContext(ctx)
		r.Header.Set(authHeader, root)
		go func() { defer close(done); destination.Handler().ServeHTTP(httptest.NewRecorder(), r) }()
		return done
	}
	var active []<-chan struct{}
	for i := range admitted {
		active = append(active, start(i, ctx))
	}
	for range admitted {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("HTTP admission serialized imports")
		}
	}
	queued, stop := context.WithTimeout(ctx, 30*time.Millisecond)
	defer stop()
	<-start(admitted, queued)
	select {
	case <-started:
		t.Fatal("queued request opened donor")
	default:
	}
	target.CancelNativeTransfers()
	if cap(destination.nativePullAdmission) != admitted {
		t.Fatal("HTTP admission capacity changed")
	}
	for _, done := range active {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("native cancellation did not drain both imports")
		}
	}
	response := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/native/capabilities", nil)
	r.Header.Set(authHeader, root)
	destination.Handler().ServeHTTP(response, r)
	var caps struct{ NativeImportConcurrency int }
	if err := json.Unmarshal(response.Body.Bytes(), &caps); err != nil || caps.NativeImportConcurrency != admitted {
		t.Fatal("admission capability", caps, err)
	}
}
