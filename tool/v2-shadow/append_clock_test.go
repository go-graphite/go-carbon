package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

func TestAppendClockBoundsAndRollover(t *testing.T) {
	dir := t.TempDir()
	makeWriter := func(seq uint64, path string) (*os.File, *inputlog.SegmentWriter) {
		t.Helper()
		f, e := os.Create(filepath.Join(dir, path))
		if e != nil {
			t.Fatal(e)
		}
		w, e := inputlog.NewSegmentWriter(f, seq)
		if e != nil {
			t.Fatal(e)
		}
		return f, w
	}
	f, w := makeWriter(1, "0000000000000001.log")
	defer f.Close()
	defer w.Close()
	if _, e := w.Append([]*points.Points{points.OnePoint("warm", 0, 0)}, nil); e != nil {
		t.Fatal(e)
	}
	st, e := f.Stat()
	if e != nil {
		t.Fatal(e)
	}
	offset := st.Size()
	c, e := startAppendClock(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer c.close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		ready := !c.lastEOF.IsZero()
		c.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("observer did not reach EOF")
		}
		time.Sleep(time.Millisecond)
	}
	before := time.Now()
	if _, e = w.Append([]*points.Points{points.OnePoint("a", 1, 1)}, nil); e != nil {
		t.Fatal(e)
	}
	after := time.Now() // Append also waits for sync, which may follow first visibility.
	stamp, e := c.take(1, offset)
	if e != nil {
		t.Fatal(e)
	}
	if stamp.Lower.After(before) || stamp.Upper.Before(before) || stamp.Upper.After(time.Now()) {
		t.Fatalf("invalid append bounds: %+v before=%v after=%v", stamp, before, after)
	}
	// A ready successor can already contain frames when the old EOF is read.
	// Its lower bound must remain the earlier observation, never that later EOF.
	before = time.Now()
	f2, w2 := makeWriter(2, "0000000000000002.log")
	defer f2.Close()
	defer w2.Close()
	if _, e = w2.Append([]*points.Points{points.OnePoint("b", 2, 2)}, nil); e != nil {
		t.Fatal(e)
	}
	stamp, e = c.take(2, 64)
	if e != nil {
		t.Fatal(e)
	}
	if stamp.Lower.After(before) {
		t.Fatalf("rollover manufactured a late append lower bound: %v > %v", stamp.Lower, before)
	}
}

func TestVisibilityPointWeightedBounds(t *testing.T) {
	h := &visibilityHistogram{}
	now := time.Now()
	h.record([]visibilityFrame{{appendStamp{now.Add(-2 * time.Second), now.Add(-time.Second)}, 99}, {appendStamp{now.Add(-4 * time.Second), now.Add(-3 * time.Second)}, 1}}, now)
	r := h.report()
	if r["points"] != uint64(100) || r["p99_lower_seconds"] != float64(1) || r["p99_upper_seconds"] != float64(2) {
		t.Fatalf("wrong weighted bounds: %v", r)
	}
}
func TestFullPoliciesRequireExactMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if len(r.Form["target"]) != 2 {
			t.Error("not batched")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"metrics":[{"name":"a","consolidationFunc":"sum","xFilesFactor":0.5,"retentions":[{"secondsPerPoint":"1","numberOfPoints":"100"},{"secondsPerPoint":"10","numberOfPoints":"100"}]},{"name":"b","consolidationFunc":"max","retentions":[{"secondsPerPoint":"60","numberOfPoints":"100"}]}]}`))
	}))
	defer server.Close()
	got, e := fetchPolicies(server.Client(), server.URL, []string{"a", "b"})
	if e != nil || len(got) != 2 {
		t.Fatalf("%s %v", got, e)
	}
	_, e = fetchPolicies(server.Client(), server.URL, []string{"a", "missing"})
	if e == nil {
		t.Fatal("silently accepted missing policy")
	}
}

func TestFullPolicy404IdentifiesMissingName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "Metric not found", http.StatusNotFound) }))
	defer server.Close()
	_, err := fetchPolicies(server.Client(), server.URL, []string{"missing.metric"})
	if err == nil || !strings.Contains(err.Error(), "missing.metric") {
		t.Fatalf("missing policy was not identified: %v", err)
	}
}
