package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	pb "github.com/go-graphite/protocol/carbonapi_v2_pb"
	"google.golang.org/protobuf/proto"
)

func TestLivePolicyAndBacklog(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("target") != "a;b=c" {
			t.Errorf("target lost tags: %s", r.URL)
		}
		b, _ := proto.Marshal(&pb.InfoResponse{Name: "a;b=c", AggregationMethod: "Sum", XFilesFactor: 0.5, Retentions: []*pb.Retention{{SecondsPerPoint: 1, NumberOfPoints: 60}, {SecondsPerPoint: 60, NumberOfPoints: 60}}})
		_, _ = w.Write(b)
	}))
	defer srv.Close()
	b, e := fetchPolicy(srv.Client(), srv.URL, "a;b=c")
	if e != nil {
		t.Fatal(e)
	}
	var p v2core.RollupPolicy
	if e = json.Unmarshal(b, &p); e != nil {
		t.Fatal(e)
	}
	if p.Name != "a;b=c" || p.Config.Method != sealrollup.Sum || len(p.Config.Archives) != 2 || p.Config.XFilesFactor != 0.5 {
		t.Fatalf("bad policy: %+v", p)
	}
	dir := t.TempDir()
	a := filepath.Join(dir, "0001.log")
	_ = os.WriteFile(a, make([]byte, 20), 0600)
	_ = os.WriteFile(filepath.Join(dir, "0002.log"), make([]byte, 30), 0600)
	backlog := sourceBacklog(dir, a, 5)
	if backlog["preallocated_file_bytes_upper_bound"] != int64(45) || backlog["successor_segments"] != 1 {
		t.Fatal(backlog)
	}
}
