package buckyd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCancelTransfersBeforeStop(t *testing.T) {
	s, _ := newNativeTestService(t, 1)
	started := make(chan struct{})
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Store-Format", "1")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer donor.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body, _ := json.Marshal(nativePullRequest{Range: chunkstore.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: donor.URL, End: 1, Names: []string{"test.cancel"}, Combine: true}, Session: "pin"})
	r := httptest.NewRequest(http.MethodPost, "/v1/native/pull", bytes.NewReader(body)).WithContext(ctx)
	r.Header.Set(authHeader, token(t, s.secret, []string{"*"}, []string{"*"}))
	done := make(chan struct{})
	response := httptest.NewRecorder()
	go func() { defer close(done); s.Handler().ServeHTTP(response, r) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("pull never started")
	}
	before := time.Now()
	s.CancelTransfers()
	if time.Since(before) > time.Second {
		t.Fatal("cancellation waited on copy")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("copy did not cancel")
	}
	if response.Code != 503 {
		t.Fatal(fmt.Sprint(response.Code), response.Body.String())
	}
	reject := httptest.NewRecorder()
	s.Handler().ServeHTTP(reject, httptest.NewRequest("GET", "/v1/native/capabilities", nil))
	if reject.Code != 503 {
		t.Fatal("new work admitted")
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
}

type notifyUpload struct {
	io.ReadCloser
	once    sync.Once
	started chan struct{}
}

func (b *notifyUpload) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	return b.ReadCloser.Read(p)
}

func TestCancelTransfersUnblocksPartialUpload(t *testing.T) {
	s, _ := newNativeTestService(t)
	started := make(chan struct{})
	handler := s.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &notifyUpload{ReadCloser: r.Body, started: started}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	root := token(t, s.secret, []string{"*"}, []string{"*"})
	_, err = fmt.Fprintf(conn, "POST /v1/native/source HTTP/1.1\r\nHost: test\r\nContent-Length: 1000000\r\nX-Buckyd-Authorization: %s\r\n\r\n{", root)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler never read upload")
	}
	s.CancelTransfers()
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal("blocked upload prevented retry response", err)
	}
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal(response.Status)
	}
}
