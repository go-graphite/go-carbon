package carbon

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestV2MergeAdminHTTP(t *testing.T) {
	app := v2TestApp(t, t.TempDir())
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	client := http.Client{Timeout: 10 * time.Second}
	url := "http://" + app.Carbonserver.HTTPListener().Addr().String() + "/admin/v2/merge"
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if method == http.MethodGet {
			if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != http.MethodPost {
				t.Errorf("GET: status=%d allow=%q", resp.StatusCode, resp.Header.Get("Allow"))
			}
		} else {
			var body struct {
				Merges int `json:"merges"`
			}
			err = json.NewDecoder(resp.Body).Decode(&body)
			if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" || err != nil || body.Merges != 0 {
				t.Errorf("POST: status=%d type=%q body=%+v err=%v", resp.StatusCode, resp.Header.Get("Content-Type"), body, err)
			}
		}
		resp.Body.Close()
	}
}
