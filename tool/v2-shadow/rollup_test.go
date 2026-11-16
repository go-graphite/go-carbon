package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/points"
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

func TestL3PinnedArchiveComparison(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	s, err := v2daemon.Open(root, v2daemon.Config{Layer: 3, Producers: 1, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if closed {
			return
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: "a;b=c", Group: "shadow", Config: sealrollup.Config{Method: sealrollup.Sum, XFilesFactor: 0, Archives: []sealrollup.Archive{{Step: 1, Count: 60}, {Step: 60, Count: 10}}}})
	if err != nil {
		t.Fatal(err)
	}
	p := s.Producers[0]
	if err = p.AppendApplications([]v2daemon.Application{{Name: "a;b=c", Policy: policy, Order: 1, Clock: 122, Points: []points.Point{{Timestamp: 120, Value: 2}, {Timestamp: 121, Value: 3}}, Sequence: []uint64{1, 2}}}); err != nil {
		t.Fatal(err)
	}
	if err = p.Seal(); err != nil {
		t.Fatal(err)
	}
	if err = p.Drain(); err != nil {
		t.Fatal(err)
	}
	if err = s.Core.Append(0, nil, 2, true); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("target") != "a;b=c" {
			t.Error("lost tagged target")
		}
		b, _ := proto.Marshal(&pb.MultiFetchResponse{Metrics: []*pb.FetchResponse{{Name: "a;b=c", StartTime: 120, StopTime: 240, StepTime: 60, Values: []float64{5, 0}, IsAbsent: []bool{false, true}}}})
		_, _ = w.Write(b)
	}))
	defer srv.Close()
	job := compareJob{Layer: 3, Names: []candidate{{ID: 1, Name: "a;b=c"}}, From: 100, Until: 240}
	for _, db := range s.Core.DBs() {
		job.Snapshots = append(job.Snapshots, db.NewSnapshot())
	}
	sums := &totals{}
	compareL3(job, srv.URL, sums)
	if sums.Names != 1 || sums.Points != 1 || sums.Mismatches != 0 || sums.Failures != 0 {
		t.Fatal(sums)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	b, err := json.Marshal(map[string]any{"stage": "l3_comparison", "names": job.Names, "from": 100, "until": 240, "context": map[string]string{"when_cet": time.Now().Format(time.RFC3339)}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "case.json")
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = recheckFrozen(root, path, srv.URL, 1); err != nil {
		t.Fatal(err)
	}
}
