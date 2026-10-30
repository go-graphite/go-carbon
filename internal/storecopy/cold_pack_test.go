package storecopy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestColdPackCopyNegotiation(t *testing.T) {
	m := &Manager{manifest: &Manifest{Version: 2, StoreFormat: 9}}
	request := httptest.NewRequest(http.MethodGet, Endpoint+"/status", nil)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, request)
	if w.Code != http.StatusConflict {
		t.Fatal("old copy client was not fenced", w.Code)
	}
	manifest := &Manifest{Version: 2, StoreFormat: 9, ID: "01234567890123456789012345678901", Expires: time.Now().Add(time.Hour), Files: []File{{Name: "store/CHUNKSTORE"}, {Name: "store/CURRENT"}}}
	if err := validateManifest(manifest); err != nil {
		t.Fatal(err)
	}
	manifest.StoreFormat = 10
	if err := validateManifest(manifest); err == nil {
		t.Fatal("unknown future format accepted")
	}
}
