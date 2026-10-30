package storecopy

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

type discardResponse struct{ header http.Header }

func (w discardResponse) Header() http.Header         { return w.header }
func (w discardResponse) Write(p []byte) (int, error) { return len(p), nil }
func (w discardResponse) WriteHeader(int)             {}

func BenchmarkCopyFile(b *testing.B) {
	path := filepath.Join(b.TempDir(), "file.sst")
	const size = 2 << 20
	if err := os.WriteFile(path, make([]byte, size), 0600); err != nil {
		b.Fatal(err)
	}
	m := &Manager{
		slots:    make(chan struct{}, 1),
		manifest: &Manifest{ID: "fixture"},
		paths:    map[string]fileSource{"store/file.sst": {path: path, size: size}},
	}
	r := httptest.NewRequest(http.MethodGet, Endpoint+"/file?id=fixture&name=store/file.sst&offset=0&length=2097152", nil)
	w := discardResponse{header: make(http.Header)}
	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		m.file(w, r)
		if w.header.Get("X-Content-SHA256") == "" {
			b.Fatal("missing verified trailer")
		}
	}
}
