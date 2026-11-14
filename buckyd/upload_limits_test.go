package buckyd

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"
)

func TestMetricUploadLimitsReport413(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		for _, compressed := range []bool{false, true} {
			s, db := testService(t)
			s.config.MaxBodyBytes = 128
			body := bytes.Repeat([]byte{'x'}, 1024)
			if compressed {
				var encoded bytes.Buffer
				w := snappy.NewBufferedWriter(&encoded)
				if _, err := w.Write(body); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				body = encoded.Bytes()
			}
			req := httptest.NewRequest(method, "/metrics/oversized", bytes.NewReader(body))
			if compressed {
				req.Header.Set("Content-Encoding", "snappy")
			}
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, req)
			if response.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("%s compressed=%v: %d %s", method, compressed, response.Code, response.Body)
			}
			if _, err := db.Metadata(context.Background(), "oversized"); err == nil {
				t.Fatal("oversized upload published metric")
			}
		}
	}
}

func TestLegacyRoutesRejectUnsupportedMethods(t *testing.T) {
	s, _ := testService(t)
	for _, path := range []string{"/metrics", "/metrics/example", "/hashring"} {
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPatch, path, http.NoBody))
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s: %d", path, response.Code)
		}
	}
}

func TestCatalogFormBodyLimit(t *testing.T) {
	s, _ := testService(t)
	s.config.MaxBodyBytes = 32
	request := httptest.NewRequest(http.MethodPost, "/metrics", bytes.NewBufferString("regex="+string(bytes.Repeat([]byte{'a'}, 64))))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("form limit: %d %s", response.Code, response.Body)
	}
}

func TestMaintenanceBodyLimitsReport413(t *testing.T) {
	s, _ := testService(t)
	for _, path := range []string{"/v1/activity-backfill", "/v1/maintenance/archive-anchors", "/v1/transfer/verify"} {
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(bytes.Repeat([]byte{' '}, (1<<20)+1)))
		if path == "/v1/transfer/verify" {
			s.config.MaxBodyBytes = 128
		}
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s: %d %s", path, response.Code, response.Body)
		}
	}
}

func TestNativeBodyLimitsReport413(t *testing.T) {
	s, _ := testService(t)
	for _, path := range []string{"/v1/native/source", "/v1/native/export"} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(bytes.Repeat([]byte{' '}, (8<<20)+1)))
		s.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s: %d %s", path, response.Code, response.Body)
		}
	}
}

func TestBulkTrailingBodyLimitReports413(t *testing.T) {
	s, _ := testService(t)
	s.config.MaxBodyBytes = 128
	body := append([]byte("[]"), bytes.Repeat([]byte{' '}, 129)...)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/transfer/verify", bytes.NewReader(body)))
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("trailing body limit: %d %s", response.Code, response.Body)
	}
}
