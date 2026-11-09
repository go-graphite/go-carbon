package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeMergeController100PinnedAndDurableBoundary(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := make([]string, 100)
	want := make(map[string]store.Snapshot)
	for i := range names {
		names[i] = fmt.Sprintf("native.merge.%03d", i)
		addPoints(t, source, names[i], float64(i))
		var err error
		want[names[i]], err = source.Snapshot(ctx, names[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	addPoints(t, target, names[99], 999)
	existing, err := target.Snapshot(ctx, names[99])
	if err != nil {
		t.Fatal(err)
	}
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) {
		pending := map[string]struct{}{}
		for _, name := range names[:50] {
			pending[name] = struct{}{}
		}
		return pending, nil
	})
	handler := nativeControllerService(t, source)
	var revise sync.Once
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/export" {
			revise.Do(func() {
				for _, name := range names[50:] {
					if err := source.UpdateMany(ctx, name, []store.Point{{Timestamp: time.Now().Unix() - 1, Value: 888}}); err != nil {
						t.Error(err)
					}
				}
			})
		}
		handler.ServeHTTP(w, r)
	}))
	defer donor.Close()
	destination := httptest.NewServer(nativeControllerService(t, target))
	defer destination.Close()
	plan := nativeMergeGeneratedPlan(t, donor.URL, destination.URL, names)
	o := testOptions(plan)
	o.engine = "bulk"
	o.delete = false
	o.bulkOnly = true
	o.mergeSession = "precutover-history"
	o.state = filepath.Join(plan, "merge-state")
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	for _, name := range names[:99] {
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.EqualSnapshots(want[name], got); err != nil {
			t.Fatal(name, err)
		}
	}
	got, err := target.Snapshot(ctx, names[99])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EqualSnapshots(existing, got); err != nil {
		t.Fatal("present name changed", err)
	}
	f, err := os.Open(o.state)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	boundaries := 0
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var row journalRow
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		if b := row.NativeSource; b != nil {
			boundaries++
			if b.Source != donor.URL || b.MergeSession != o.mergeSession || len(b.ID) != 32 || b.SnapshotStartedAt.IsZero() || b.SnapshotPinnedAt.Before(b.SnapshotStartedAt) || b.BackfillFromUnix != 0 || !b.BackfillFullRetention {
				t.Fatal("bad durable boundary", b)
			}
		}
	}
	if scan.Err() != nil || boundaries != 1 {
		t.Fatal("boundary not recorded", boundaries, scan.Err())
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("resume", err)
	}
	o.mergeSession = ""
	if err := run(ctx, o); err == nil {
		t.Fatal("merge journal resumed in default mode")
	}
}

func nativeMergeGeneratedPlan(t *testing.T, source, target string, names []string) string {
	t.Helper()
	root := t.TempDir()
	namesPath := filepath.Join(root, "names.txt")
	if err := os.WriteFile(namesPath, []byte(strings.Join(names, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	node := func(id, address, endpoint string) map[string]any {
		return map[string]any{"hash": "jump_fnv1a", "replicas": 1, "nodes": []map[string]string{{"id": id, "node": address, "endpoint": endpoint}}}
	}
	spec := map[string]any{"version": 1, "cluster": "source", "target_cluster": "combined", "site": "test",
		"old": node("old", "127.0.0.1:2003=000", source), "new": node("new", "127.0.0.1:2004=000", target),
		"inputs": []map[string]string{{"source": "old", "path": namesPath, "format": "lines"}}}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "spec.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	plan := filepath.Join(root, "plan")
	if err := Main([]string{"plan", "--spec", path, "--out", plan, "--apply"}); err != nil {
		t.Fatal(err)
	}
	p, err := readPlanStructure(plan, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Whole) != 0 || len(p.Streams) != 1 || p.Streams[0].Metrics != int64(len(names)) {
		t.Fatal("independent singleton became whole-store replacement", p.Whole, p.Streams)
	}
	return plan
}

func TestNativeMergeRefusesUnsafeModes(t *testing.T) {
	for _, o := range []options{
		{mergeSession: "merge", engine: "metric", bulkOnly: true},
		{mergeSession: "merge", engine: "bulk"},
		{mergeSession: "merge", engine: "bulk", bulkOnly: true, bulkCombine: true},
		{mergeSession: "merge", engine: "bulk", bulkOnly: true, delete: true},
	} {
		if err := run(context.Background(), o); err == nil {
			t.Fatal("unsafe mode accepted", o)
		}
	}
}

// Waiters must not reuse an ID retained only for cleanup after boundary
// validation or fsync fails. Otherwise another range can import without a ledger.
func TestNativeMergeFailedBoundaryIsNeverReusable(t *testing.T) {
	ctx := context.Background()
	for _, stage := range []string{"invalid-boundary", "journal-write"} {
		t.Run(stage, func(t *testing.T) {
			var pins int
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					return
				}
				pins++
				bound := nativeSourceCheckpoint{ID: "0123456789abcdef0123456789abcdef", MergeSession: "history", SnapshotStartedAt: time.Now().UTC(), SnapshotPinnedAt: time.Now().UTC(), BackfillFullRetention: true}
				if stage == "invalid-boundary" {
					bound.BackfillFullRetention = false
				}
				_ = json.NewEncoder(w).Encode(bound)
			}))
			defer peer.Close()
			j, err := openJournal(filepath.Join(t.TempDir(), "state"), "plan")
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			if stage == "journal-write" {
				_ = j.file.Close()
			}
			c := &nativeController{mover: &mover{options: options{mergeSession: "history"}, journal: j, client: http.DefaultClient}, sessions: map[string]*nativeRemoteSession{}}
			defer c.release()
			// The first worker fails; every waiter must fail, with no usable ID.
			for range 100 {
				if id, err := c.session(ctx, peer.URL); err == nil || id != "" {
					t.Fatalf("unsafe pin reused: %q %v", id, err)
				}
			}
			if pins != 1 {
				t.Fatal("unexpected pins", pins)
			}
		})
	}
}
