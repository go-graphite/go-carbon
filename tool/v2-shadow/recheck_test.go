package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/points"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/protobuf/proto"
)

func TestStoppedRootFrozenRecheck(t *testing.T) {
	root := filepath.Join(t.TempDir(), "shadow")
	s, err := v2daemon.Open(root, v2daemon.Config{Layer: 1, Producers: 1, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Producers[0].Append([]string{"a"}, nil, []points.Point{{Timestamp: 60, Value: 2}}, []uint64{1}, 120); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("target") != "a" || r.URL.Query().Get("from") != "60" || r.URL.Query().Get("until") != "120" {
			t.Error("changed frozen query", r.URL.RawQuery)
		}
		b, _ := proto.Marshal(&pb.MultiFetchResponse{Metrics: []*pb.FetchResponse{{Name: "a", StartTime: 60, StepTime: 60, Values: []float64{2}, IsAbsent: []bool{false}}}})
		_, _ = w.Write(b)
	}))
	defer server.Close()
	b, err := json.Marshal(map[string]any{"stage": "comparison", "names": []candidate{{1, "a"}}, "from": 60, "until": 120, "attempt": 2, "context": map[string]string{"when_cet": time.Now().Format(time.RFC3339)}})
	if err != nil {
		t.Fatal(err)
	}
	casePath := filepath.Join(t.TempDir(), "case.json")
	if err = os.WriteFile(casePath, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = recheckFrozen(root, casePath, server.URL, 1); err == nil {
		t.Fatal("accepted a root still owned by the writer")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = recheckFrozen(root, casePath, server.URL, 1); err != nil {
		t.Fatal(err)
	}
	absent := filepath.Join(t.TempDir(), "absent")
	if err = recheckFrozen(absent, casePath, server.URL, 1); err == nil {
		t.Fatal("accepted a non-shadow root")
	}
	if _, err = os.Stat(absent); !os.IsNotExist(err) {
		t.Fatalf("created missing root: %v", err)
	}
}
