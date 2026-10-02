package carbonserver

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A stalled log sink must not hold up expected misses when their logging is disabled.
type blockedAccessWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *blockedAccessWriter) Write(p []byte) (int, error) {
	w.entered <- struct{}{}
	<-w.release
	return len(p), nil
}

func (w *blockedAccessWriter) Sync() error { return nil }

func TestFindNotFoundLogging(t *testing.T) {
	for _, protocol := range []string{"http", "grpc"} {
		for _, cached := range []bool{false, true} {
			for _, suppress := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/cache=%t/suppress=%t", protocol, cached, suppress), func(t *testing.T) {
					listener := NewCarbonserverListener(nil)
					listener.whisperData = t.TempDir()
					listener.trigramIndex = false
					listener.findCacheEnabled = cached
					listener.SetDoNotLog404s(suppress)
					listener.logger = zap.NewNop()
					if cached {
						listener.accessLogger = zap.NewNop()
						if protocol == "http" {
							listener.findHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/metrics/find/?query=missing.metric&format=protobuf", nil))
						} else {
							_, _ = listener.Find(context.Background(), &protov2.GlobRequest{Query: "missing.metric"})
						}
					}
					writer := &blockedAccessWriter{make(chan struct{}, 1), make(chan struct{})}
					listener.accessLogger = zap.New(zapcore.NewCore(
						zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), writer, zap.InfoLevel))
					done := make(chan error, 1)
					go func() {
						if protocol == "http" {
							rec := httptest.NewRecorder()
							listener.findHandler(rec, httptest.NewRequest(http.MethodGet, "/metrics/find/?query=missing.metric&format=protobuf", nil))
							if rec.Code != http.StatusNotFound {
								done <- fmt.Errorf("HTTP status = %d; want 404", rec.Code)
								return
							}
						} else {
							_, err := listener.Find(context.Background(), &protov2.GlobRequest{Query: "missing.metric"})
							if status.Code(err) != codes.NotFound {
								done <- fmt.Errorf("gRPC error = %w; want NotFound", err)
								return
							}
						}
						done <- nil
					}()
					select {
					case <-writer.entered:
						close(writer.release)
						if suppress {
							t.Error("expected miss was blocked by access logging with do-not-log-404s enabled")
						}
						if err := <-done; err != nil {
							t.Error(err)
						}
					case err := <-done:
						close(writer.release)
						if err != nil {
							t.Error(err)
						}
						if !suppress {
							t.Error("expected miss was not logged with do-not-log-404s disabled")
						}
					case <-time.After(5 * time.Second):
						close(writer.release)
						t.Fatal("find handler did not finish or reach access logging")
					}
				})
			}
		}
	}
}

func TestFindBadRequestStillLogged(t *testing.T) {
	listener := NewCarbonserverListener(nil)
	listener.SetDoNotLog404s(true)
	var logs bytes.Buffer
	listener.accessLogger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.ErrorLevel))
	rec := httptest.NewRecorder()
	listener.findHandler(rec, httptest.NewRequest(http.MethodGet, "/metrics/find/?query=missing.metric&format=invalid", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("HTTP status = %d; want 400", rec.Code)
	}
	if !bytes.Contains(logs.Bytes(), []byte(`"msg":"find failed"`)) {
		t.Fatal("bad request must still be logged with do-not-log-404s enabled")
	}
}
