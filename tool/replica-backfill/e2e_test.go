package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	whisper "github.com/go-graphite/go-whisper"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/points"
)

// memCache stands in for the local go-carbon cache: the plaintext receiver
// adds points and carbonserver merges them into reads.
type memCache struct {
	mu  sync.Mutex
	pts map[string][]points.Point
}

func (c *memCache) add(name string, ts int64, v float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pts[name] = append(c.pts[name], points.Point{Value: v, Timestamp: ts})
}

func (c *memCache) get(name string) []points.Point {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]points.Point(nil), c.pts[name]...)
}

func writeWhisper(t *testing.T, root, name string, pts map[int64]float64) {
	t.Helper()
	path := filepath.Join(root, strings.ReplaceAll(name, ".", "/")+".wsp")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	ret, err := whisper.ParseRetentionDefs("60s:1d")
	if err != nil {
		t.Fatal(err)
	}
	w, err := whisper.Create(path, ret, whisper.Average, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var batch []*whisper.TimeSeriesPoint
	for ts, v := range pts {
		batch = append(batch, &whisper.TimeSeriesPoint{Time: int(ts), Value: v})
	}
	if err := w.UpdateMany(batch); err != nil {
		t.Fatal(err)
	}
}

func startCarbonserver(t *testing.T, root string, cacheGet func(string) []points.Point, want int) string {
	t.Helper()
	l := carbonserver.NewCarbonserverListener(cacheGet)
	l.SetWhisperData(root)
	l.SetTrieIndex(true)
	l.SetScanWorkers(1)
	l.SetScanFrequency(50 * time.Millisecond)
	l.SetMaxGlobs(100)
	l.SetMaxMetricsGlobbed(30000)
	l.SetMaxMetricsRendered(1000)
	l.SetQueryCacheEnabled(false)
	l.SetFindCacheEnabled(false)
	if err := l.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Stop() })
	url := "http://" + l.HTTPListener().Addr().String()
	c := &client{base: url, http: http.DefaultClient}
	deadline := time.Now().Add(10 * time.Second)
	for {
		names, err := c.list(context.Background())
		if err == nil && len(names) == want {
			return url
		}
		if time.Now().After(deadline) {
			t.Fatalf("carbonserver index not ready: %v %v", names, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestEndToEndRealCarbonserver runs the tool against real carbonserver
// listeners over whisper files: a replica with full data and a local host
// with a gap, whose plaintext input feeds the cache carbonserver reads.
func TestEndToEndRealCarbonserver(t *testing.T) {
	t.Parallel()
	now := time.Now().Unix() / 60 * 60
	from, until := now-30*60, now-2*60
	gapFrom, gapUntil := now-20*60, now-16*60
	val := func(ts int64) float64 { return float64(ts%7919) / 4 }
	full := map[int64]float64{}
	gappy := map[int64]float64{}
	for ts := now - 60*60; ts <= until; ts += 60 {
		full[ts] = val(ts)
		if ts < gapFrom || ts > gapUntil {
			gappy[ts] = val(ts)
		}
	}
	gappy[now-25*60] = -3 // local value differs from the replica and must win

	replicaDir, localDir := t.TempDir(), t.TempDir()
	writeWhisper(t, replicaDir, "e2e.gappy", full)
	writeWhisper(t, replicaDir, "e2e.complete", full)
	writeWhisper(t, localDir, "e2e.gappy", gappy)
	writeWhisper(t, localDir, "e2e.complete", full)
	writeWhisper(t, localDir, "e2e.localonly", gappy)

	cache := &memCache{pts: map[string][]points.Point{}}
	replicaURL := startCarbonserver(t, replicaDir, func(string) []points.Point { return nil }, 2)
	localURL := startCarbonserver(t, localDir, cache.get, 3)
	in := newIngest(t, cache.add)

	opts := func() *options {
		return &options{
			local: localURL, ingest: in.ln.Addr().String(), replicas: urlList{replicaURL},
			from: strconv.FormatInt(from, 10), until: strconv.FormatInt(until, 10),
			lookback: 30 * time.Minute, batch: 2, workers: 2, replicaRPS: 100,
			timeout: 10 * time.Second, retries: 2, backoff: 10 * time.Millisecond, maxBack: 100 * time.Millisecond,
			flushPt: 1000, writeRetries: 3, checkpoint: filepath.Join(t.TempDir(), "ckpt.json"),
		}
	}
	r, err := run(context.Background(), opts(), time.Unix(now, 0))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	var want []string
	for ts := gapFrom; ts <= gapUntil; ts += 60 {
		want = append(want, line("e2e.gappy", val(ts), ts))
	}
	// Write order across batches and flushes is not part of the contract:
	// the store merges points by timestamp.
	got := in.wait(t, len(want))
	slices.Sort(got)
	slices.Sort(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		b, _ := json.Marshal(r)
		t.Fatalf("sent:\n%s\nwant:\n%s\nreport %s", strings.Join(got, "\n"), strings.Join(want, "\n"), b)
	}
	if m := r.Metrics; m["with_gaps"] != 2 || m["complete"] != 1 || m["unfilled_metrics"] != 1 || m["fillable_points"] != 5 {
		t.Fatalf("report %+v", m)
	}

	// The filled points are now served locally (from the cache): a dry run
	// over the same window finds nothing more to fill for e2e.gappy.
	o := opts()
	o.dryRun = true
	r, err = run(context.Background(), o, time.Unix(now, 0))
	if err != nil {
		t.Fatal(err)
	}
	if m := r.Metrics; m["with_gaps"] != 1 || m["complete"] != 2 || m["fillable_points"] != 0 {
		t.Fatalf("second pass report %+v", m)
	}
}
