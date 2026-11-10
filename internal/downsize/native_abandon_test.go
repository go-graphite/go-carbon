package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNativeAbandonCommandDryRunAndApply(t *testing.T) {
	id := strings.Repeat("a", 64)
	calls := 0
	t.Setenv("ABANDON_TEST_TOKEN", "secret")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/v1/native/abandon" || r.Header.Get("X-Buckyd-Authorization") != "secret" {
			t.Error("wrong target/auth", r.Method, r.URL.Path)
		}
		var request struct{ Identity string }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Identity != id {
			t.Error(request, err)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"Identity": id, "State": "abandoned"})
	}))
	defer server.Close()
	args := []string{"--target", server.URL, "--identity", id, "--token-env", "ABANDON_TEST_TOKEN"}
	var out bytes.Buffer
	if err := nativeAbandonMain(context.Background(), args, &out); err != nil || calls != 0 {
		t.Fatal("dry-run contacted server", calls, err)
	}
	out.Reset()
	if err := nativeAbandonMain(context.Background(), append(args, "--apply"), &out); err != nil || calls != 1 || !strings.Contains(out.String(), "abandoned") {
		t.Fatal(out.String(), calls, err)
	}
}
