package carbon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/points"
)

// Exercise the actual application's existing carbonserver HTTP listener,
// including prepared links surviving transport cleanup and an app restart.
func TestV2MoveAdminHTTP(t *testing.T) {
	root := t.TempDir()
	app := v2TestApp(t, root)
	app.Config.Prometheus.Enabled = true
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if app != nil {
			app.Stop()
		}
	}()
	const name = "move.http"
	schema, _ := app.Config.Whisper.Schemas.Match(name)
	agg := app.Config.Whisper.Aggregation.Match(name)
	cfg := sealrollup.Config{Method: sealrollup.Method(agg.AggregationMethod()), XFilesFactor: float32(agg.XFilesFactor())}
	for _, a := range schema.Retentions {
		cfg.Archives = append(cfg.Archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
	}
	policy, err := v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: name, Group: "local", Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	donorRoot := t.TempDir()
	donor, err := v2daemon.Open(donorRoot, v2daemon.Config{Layer: 3, Producers: 1, Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	now := time.Now().Unix()
	stamp := now - now%cfg.Archives[0].Step - cfg.Archives[0].Step
	if err = donor.Producers[0].AppendApplications([]v2daemon.Application{{Name: name, Policy: policy, Order: 1, Clock: now, Points: []points.Point{{Timestamp: stamp, Value: 3.25}}, Sequence: []uint64{1}}}); err != nil {
		t.Fatal(err)
	}
	if err = donor.Producers[0].Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot := donor.Core.DBs()[0].NewSnapshot()
	files, err := (v2core.SealedSnapshotReader{Snapshot: snapshot, Directory: filepath.Join(donorRoot, "placement-group-00000001", "core", "owner-000")}).ReadPartFiles(t.Context())
	_ = snapshot.Close()
	if err != nil {
		t.Fatal(err)
	}
	transport := t.TempDir()
	request := v2daemon.MoveRequest{Now: now}
	for _, file := range files {
		if !strings.HasSuffix(file.Path, ".descriptor") {
			continue
		}
		b, err := os.ReadFile(file.Path)
		if err != nil {
			t.Fatal(err)
		}
		d, err := v2part.ParseDescriptor(b)
		if err != nil {
			t.Fatal(err)
		}
		pinned := filepath.Join(transport, fmt.Sprintf("%x.sst", d.SHA256))
		if err := os.Link(strings.TrimSuffix(file.Path, ".descriptor"), pinned); err != nil {
			t.Fatal(err)
		}
		request.Parts = append(request.Parts, v2core.MovePart{Path: pinned, Descriptor: d})
	}
	call := func(action, id string, body any) v2daemon.MoveStatus {
		t.Helper()
		var data []byte
		if body != nil {
			data, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
		}
		method := http.MethodPost
		if action == "status" {
			method = http.MethodGet
		}
		url := "http://" + app.Carbonserver.HTTPListener().Addr().String() + "/admin/v2/move/" + action + "?id=" + id
		req, err := http.NewRequest(method, url, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		client := http.Client{Timeout: 20 * time.Second}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(response.Body)
			t.Fatalf("%s: %d %s", action, response.StatusCode, b)
		}
		var result v2daemon.MoveStatus
		if err = json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	prepared := call("prepare", "", request)
	if prepared.ID == "" || prepared.Done {
		t.Fatalf("prepare: %+v", prepared)
	}
	if status := call("status", prepared.ID, nil); status.Done {
		t.Fatal("prepare prematurely done")
	}
	// Prepare owns links; the original transport files may now disappear.
	for _, part := range request.Parts {
		if err = os.Remove(part.Path); err != nil {
			t.Fatal(err)
		}
	}
	app.Stop()
	app = nil
	app = v2TestApp(t, root)
	app.Config.Prometheus.Enabled = true
	if err = app.Start(); err != nil {
		t.Fatal(err)
	}
	if status := call("adopt", prepared.ID, nil); !status.Done {
		t.Fatal("adopt incomplete")
	}
	visibleDeadline := time.Now().Add(2200 * time.Millisecond)
	for !slices.Contains(slices.Collect(app.v2.catalog.Snapshot().Names()), name) {
		if time.Now().After(visibleDeadline) {
			t.Fatal("live adopted name did not reach discovery within 2.2s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status := call("adopt", prepared.ID, nil); !status.Done {
		t.Fatal("adopt retry incomplete")
	}
	if status := call("status", prepared.ID, nil); !status.Done || status.Phase != "done" || status.PartsDone != status.PartsTotal || status.BytesDone != status.BytesTotal || status.MBps <= 0 || status.ETASeconds != 0 {
		t.Fatalf("bad completion status: %+v", status)
	}
	families, err := app.PromRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "v2_move_") {
			found[family.GetName()] = true
		}
	}
	for _, name := range []string{"v2_move_progress", "v2_move_owner_state", "v2_move_regressions_total"} {
		if !found[name] {
			t.Errorf("missing move metric %s", name)
		}
	}
	graphite := map[string]float64{}
	v2MoveStats{app.v2.moves}.Stat(func(name string, value float64) { graphite[name] = value })
	if graphite[prepared.ID+".phase"] != 5 || graphite[prepared.ID+".bytes_done"] <= 0 {
		t.Fatalf("missing graphite progress: %v", graphite)
	}
	if app.Collector == nil || app.Collector.endpoint == MetricEndpointLocal {
		t.Fatal("v2 graphite collector not connected to intake")
	}
	app.Collector.collect()
	graphiteName := app.Collector.graphPrefix + ".v2.move." + prepared.ID + ".phase"
	deadlineGraphite := time.Now().Add(8 * time.Second)
	for {
		found := false
		for name := range app.v2.catalog.Snapshot().Names() {
			if name == graphiteName {
				found = true
				break
			}
		}
		if found {
			break
		}
		if time.Now().After(deadlineGraphite) {
			t.Fatal("move Graphite stats did not reach v2 intake")
		}
		time.Sleep(20 * time.Millisecond)
	}

	app.Stop()
	app = nil
	app = v2TestApp(t, root)
	app.Config.Prometheus.Enabled = true
	if err = app.Start(); err != nil {
		t.Fatal(err)
	}
	if status := call("status", prepared.ID, nil); !status.Done {
		t.Fatal("done marker lost after restart")
	}
	deadline := time.Now().Add(8 * time.Second)
	for app.v2.catalog.Snapshot().Len() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("adopted name was not published to HTTP discovery")
		}
		time.Sleep(20 * time.Millisecond)
	}
	body := v2HTTP(t, app, fmt.Sprintf("/render/?format=json&target=%s&from=%d&until=%d", name, stamp-cfg.Archives[0].Step, stamp+cfg.Archives[0].Step))
	if !strings.Contains(string(body), "3.25") {
		t.Fatalf("adopted HTTP render: %s", body)
	}
}
