package carbonserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2index"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
)

// Exercise the real core, discovery ticker and HTTP handlers together. Small
// owner seals contain only two routed shards, separated by almost the entire
// shard space; opening a part must cost its actual inventory, not that range.
func TestV2SparseSealsLiveDiscovery(t *testing.T) {
	const owners, rounds = 16, 64
	r, err := v2read.New(owners, 256<<20, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	e, err := v2core.Open(t.TempDir(), v2core.Config{Rollups: true, AppendParts: true, JournalIdentity: "live-discovery", Shards: owners, Owners: owners, Producers: 1, MaxRows: 256, PartitionSeconds: 86400, RawCodec: "run1"}, v2core.Hooks{PublishPending: r.PublishPending})
	if err != nil {
		r.Close()
		t.Fatal(err)
	}
	defer func() {
		r.Close()
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Load an earlier publication, then let sealed parts accumulate before the
	// next refresh. This models a discovery pass catching up during live intake.
	if _, err := r.Names(context.Background()); err != nil {
		t.Fatal(err)
	}
	catalog := &v2index.Catalog{}
	if err := catalog.Publish(1, nil); err != nil {
		t.Fatal(err)
	}
	l := NewCarbonserverListener(nil)
	l.SetTrieIndex(true)
	l.SetMaxGlobs(100)
	l.SetMaxMetricsRendered(100)
	l.SetV2Reader(r)
	l.initializeCaches()
	if err := l.SetV2Catalog(catalog); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(l.newHTTPMux())
	defer server.Close()
	now := time.Now().Unix()
	stamp := now - 10
	var batches []v2core.Batch
	// Exactly one name at either end of each owner's routed shard range.
	var selected [owners][2]bool
	for candidate := 0; len(batches) < owners*2; candidate++ {
		name := fmt.Sprintf("sparse.live.metric.%d", candidate)
		v := int(v2part.Route(v2part.ID(name)))
		if v >= owners && v < v2part.VirtualShards-owners {
			continue
		}
		owner, end := v%owners, v/(v2part.VirtualShards-owners)
		if selected[owner][end] {
			continue
		}
		selected[owner][end] = true
		policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 3600}, {Step: 60, Count: 3600}}}})
		if err != nil {
			t.Fatal(err)
		}
		id := uint64(len(batches) + 1)
		batches = append(batches, v2core.Batch{ID: id, Policy: policy, Clock: now, Points: []v2core.Row{{ID: id, Time: stamp, Bits: math.Float64bits(42)}}})
	}
	appendRound := func(order uint64) error {
		for i := range batches {
			batches[i].Order = order
			batches[i].Points[0].Seq = order
		}
		return e.AppendBatches(0, batches, order, true)
	}
	// Accumulate a backlog before the next discovery pass.
	for round := uint64(1); round <= rounds; round++ {
		if err := appendRound(round); err != nil {
			t.Fatal(err)
		}
		if round%32 == 0 {
			t.Logf("seeded %d sparse parts per owner", round)
		}
	}
	name := ""
	for candidate := 0; ; candidate++ {
		name = fmt.Sprintf("sparse.new.metric.%d", candidate)
		if int(v2part.Route(v2part.ID(name))) < owners {
			break
		}
	}
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: sealrollup.Config{Method: sealrollup.Last, Archives: []sealrollup.Archive{{Step: 1, Count: 3600}, {Step: 60, Count: 3600}}}})
	if err != nil {
		t.Fatal(err)
	}
	id := uint64(len(batches) + 1)
	batches = append(batches, v2core.Batch{ID: id, Policy: policy, Clock: now, Points: []v2core.Row{{ID: id, Time: stamp, Bits: math.Float64bits(42)}}})
	started := time.Now()
	if err := appendRound(rounds + 1); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Go(func() {
		for revision := uint64(2); ; revision++ {
			select {
			case <-stop:
				return
			default:
			}
			names, err := r.Names(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			if err := catalog.Publish(revision, names); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
	writer.Go(func() {
		for round := uint64(rounds + 2); ; round++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := appendRound(round); err != nil {
				t.Error(err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	defer func() { close(stop); writer.Wait() }()
	base := server.URL
	client := &http.Client{Timeout: time.Second}
	get := func(path string) (int, string, error) {
		resp, err := client.Get(base + path)
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body), err
	}
	var maxInfo time.Duration
	for {
		infoStarted := time.Now()
		status, body, err := get("/info/?format=json&target=" + name)
		maxInfo = max(maxInfo, time.Since(infoStarted))
		if err != nil || status != http.StatusOK || !strings.Contains(strings.ToLower(body), "last") {
			t.Fatalf("info must answer within 1s under ingest: status=%d err=%v body=%s", status, err, body)
		}
		_, found, err := get("/metrics/find/?format=json&query=" + name)
		if err != nil {
			t.Fatal(err)
		}
		status, body, err = get(fmt.Sprintf("/render/?format=json&target=%s&from=%d&until=%d", name, stamp-1, stamp))
		var rendered struct {
			Metrics []struct {
				Values []float64 `json:"values"`
				Absent []bool    `json:"isAbsent"`
			} `json:"metrics"`
		}
		exact := false
		if json.Unmarshal([]byte(body), &rendered) == nil && len(rendered.Metrics) == 1 {
			m := rendered.Metrics[0]
			exact = len(m.Values) == 1 && len(m.Absent) == 1 && !m.Absent[0] && math.Float64bits(m.Values[0]) == math.Float64bits(42)
		}
		if err == nil && status == http.StatusOK && strings.Contains(found, name) && exact {
			if time.Since(started) > 5*time.Second {
				t.Fatal("find/render exceeded 5s")
			}
			t.Logf("find/render visible in %s; maximum info latency %s with continuous sparse owner seals", time.Since(started), maxInfo)
			break
		}
		if time.Since(started) >= 5*time.Second {
			t.Fatalf("new name not findable/renderable within 5s: status=%d err=%v body=%s find=%s", status, err, body, found)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
