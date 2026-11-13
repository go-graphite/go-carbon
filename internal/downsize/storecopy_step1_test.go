package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestNativeReceiverRejectsRangesAboveControllerCap(t *testing.T) {
	if nativeRangeNames != store.NativeMaxRangeNames {
		t.Fatal("controller range cap differs from the receiver limit", nativeRangeNames)
	}
	names := make([]string, store.NativeMaxRangeNames+1)
	for i := range names {
		names[i] = fmt.Sprintf("cap.%05d", i)
	}
	if err := store.ValidateNativeNames(names); !errors.Is(err, store.ErrTransferLimit) {
		t.Fatal("4097-name range accepted", err)
	}
	if err := store.ValidateNativeNames(names[:store.NativeMaxRangeNames]); err != nil {
		t.Fatal("4096-name range rejected", err)
	}
}

// One long plan stream (an old 100k-name segment) is sent as fixed sub-ranges
// no larger than the cap; resume skips completed sub-ranges and replays none.
func TestNativeControllerSubRangesLongStreams(t *testing.T) {
	defer func(limit int64) { nativeRangeNames = limit }(nativeRangeNames)
	nativeRangeNames = 4
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := make([]string, 10)
	for i := range names {
		names[i] = fmt.Sprintf("native.subrange.%02d", i)
		addPoints(t, source, names[i], float64(i))
	}
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	targetHandler := nativeControllerService(t, target)
	var mu sync.Mutex
	var ranges [][2]int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			var request struct{ Range store.NativeRange }
			if err := json.Unmarshal(data, &request); err != nil {
				t.Error(err)
				return
			}
			if int64(len(request.Range.Names)) > nativeRangeNames || request.Range.End-request.Range.Start != int64(len(request.Range.Names)) {
				t.Errorf("range %d:%d has %d names", request.Range.Start, request.Range.End, len(request.Range.Names))
			}
			mu.Lock()
			ranges = append(ranges, [2]int64{request.Range.Start, request.Range.End})
			mu.Unlock()
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		targetHandler.ServeHTTP(w, r)
	}))
	defer destination.Close()
	plan := fixturePlan(t, donor.URL, destination.URL, names)
	o := testOptions(plan)
	o.delete = false
	o.deleteMode = "auto"
	o.engine = "bulk"
	o.bulkOnly = true
	o.maxBatches = 1
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	o.maxBatches = 0
	if err := run(ctx, o); err != nil {
		t.Fatal("resume", err)
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("completed resume", err)
	}
	want := [][2]int64{{0, 4}, {4, 8}, {8, 10}}
	if fmt.Sprint(ranges) != fmt.Sprint(want) {
		t.Fatal("sub-ranges", ranges, "want", want)
	}
	for _, name := range names {
		if _, err := target.Metadata(ctx, name); err != nil {
			t.Fatal("not copied", name, err)
		}
	}
}

func TestPlanNamesMatchesConfigPlanForSameList(t *testing.T) {
	root := t.TempDir()
	names := []string{"a.one", "a.two", "b.x", "c.y", "c.z", "d.last", "e.more"}
	list := filepath.Join(root, "names.txt")
	if err := os.WriteFile(list, []byte(strings.Join(names, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, target := "http://donor.example:4242", "http://survivor.example:4242/"
	byNames := filepath.Join(root, "names-plan")
	if err := Main([]string{"plan", "--names", list, "--source", source, "--target", target, "--out", byNames, "--apply", "--segment-metrics", "3"}); err != nil {
		t.Fatal(err)
	}
	node := func(id, endpoint string) map[string]any {
		return map[string]any{"hash": "jump_fnv1a", "replicas": 1, "nodes": []map[string]string{{"id": id, "node": id + "=" + id, "endpoint": endpoint}}}
	}
	spec, err := json.Marshal(map[string]any{"version": 1, "cluster": "names", "site": "names",
		"old": node("source", source), "new": node("target", strings.TrimRight(target, "/")),
		"inputs": []map[string]any{{"source": "source", "path": list, "format": "lines", "estimated_bytes": 65536}}})
	if err != nil {
		t.Fatal(err)
	}
	specPath := filepath.Join(root, "spec.json")
	if err := os.WriteFile(specPath, spec, 0600); err != nil {
		t.Fatal(err)
	}
	byConfig := filepath.Join(root, "config-plan")
	if err := Main([]string{"config", "--mode", "plan", "--spec", specPath, "--out", byConfig, "--apply", "--segment-metrics", "3"}); err != nil {
		t.Fatal(err)
	}
	p, err := readPlan(byNames)
	if err != nil {
		t.Fatal("names plan unreadable by run", err)
	}
	var planned int64
	for _, stream := range p.Streams {
		if stream.Metrics > 3 || stream.Source != "source" || stream.Destination != "target" {
			t.Fatal("stream", stream.Path, stream.Metrics, stream.Source, stream.Destination)
		}
		planned += stream.Metrics
	}
	if planned != int64(len(names)) || len(p.Streams) != 3 {
		t.Fatal("names plan", planned, len(p.Streams))
	}
	files, err := filepath.Glob(filepath.Join(byConfig, "streams", "*.jsonl"))
	if err != nil || len(files) != 3 {
		t.Fatal(files, err)
	}
	for _, rel := range append([]string{"manifest.json"}, func() (out []string) {
		for _, f := range files {
			out = append(out, filepath.Join("streams", filepath.Base(f)))
		}
		return
	}()...) {
		a, err := os.ReadFile(filepath.Join(byNames, rel))
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(byConfig, rel))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("%s differs:\n%s\n%s", rel, a, b)
		}
	}
	unsorted := filepath.Join(root, "unsorted.txt")
	if err := os.WriteFile(unsorted, []byte("b\na\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"plan", "--names", unsorted, "--source", source, "--target", target},
		{"plan", "--names", list, "--source", source},
		{"plan", "--names", list, "--source", source, "--target", source},
		{"plan", "--names", list, "--source", source, "--target", target, "--spec", specPath},
		{"plan", "--names", list, "--source", source, "--target", target, "--segment-metrics", "0"},
		{"plan", "--names", list, "--source", source, "--target", target, "--estimated-bytes", "1228730543"},
		{"plan", "--names", list, "--source", source, "--target", target, "--estimated-bytes", "0"},
	} {
		if err := Main(args); err == nil {
			t.Fatal("accepted", args)
		}
	}
}

func idleFixture(t *testing.T, names []string, hang func(request []string) bool) (*store.Store, string, *sync.Map) {
	t.Helper()
	source, target := localStore(t), localStore(t)
	for i, name := range names {
		addPoints(t, source, name, float64(i+2))
	}
	addPoints(t, target, names[0], 999)
	src := httptest.NewServer(localService(t, source))
	t.Cleanup(src.Close)
	handler := localService(t, target)
	var pulls sync.Map
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/pull" {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			var request struct{ Names []string }
			if err := json.Unmarshal(data, &request); err != nil {
				t.Error(err)
				return
			}
			for _, name := range request.Names {
				counter, _ := pulls.LoadOrStore(name, new(atomic.Int64))
				counter.(*atomic.Int64).Add(1)
			}
			if hang(request.Names) {
				// Both ends idle: no bytes until the controller gives up.
				<-r.Context().Done()
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(dst.Close)
	return target, fixturePlan(t, src.URL, dst.URL, names), &pulls
}

func pullCount(pulls *sync.Map, name string) int64 {
	counter, ok := pulls.Load(name)
	if !ok {
		return 0
	}
	return counter.(*atomic.Int64).Load()
}

func idleOptions(root string) options {
	o := testOptions(root)
	o.delete = false
	o.deleteMode = "auto"
	o.transferIdle = 200 * time.Millisecond
	return o
}

func TestTransferIdleTimeoutRetriesSingleNameAndSplitsBatches(t *testing.T) {
	ctx := context.Background()
	names := []string{"idle.00", "idle.01", "idle.02", "idle.03", "idle.04", "idle.05"}
	var hungSingle, hungBatch atomic.Bool
	target, root, pulls := idleFixture(t, names, func(request []string) bool {
		if len(request) > 1 {
			return !hungBatch.Swap(true)
		}
		return request[0] == "idle.03" && !hungSingle.Swap(true)
	})
	o := idleOptions(root)
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if !hungBatch.Load() {
		t.Fatal("fixture did not hang a batch")
	}
	for _, name := range names {
		got, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(name, err)
		}
		if name == names[0] && got.Archives[0].Points[1].Value != 999 {
			t.Fatal("target point lost on retry")
		}
	}
	// The hung batch named every metric once; the split halves name them again.
	if pullCount(pulls, "idle.00") < 2 {
		t.Fatal("hung batch was not retried", pullCount(pulls, "idle.00"))
	}
	before := map[string]int64{}
	for _, name := range names {
		before[name] = pullCount(pulls, name)
	}
	if err := run(ctx, o); err != nil {
		t.Fatal("completed resume", err)
	}
	for _, name := range names {
		if pullCount(pulls, name) != before[name] {
			t.Fatal("completed name pulled again", name)
		}
	}
	o = idleOptions(root)
	o.batch = 1
	o.state = filepath.Join(root, "single-state")
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if !hungSingle.Load() || pullCount(pulls, "idle.03") != before["idle.03"]+2 {
		t.Fatal("hung single name was not retried once", pullCount(pulls, "idle.03"))
	}
}

func TestTransferIdleTimeoutAbortsLoudlyAndKeepsJournal(t *testing.T) {
	ctx := context.Background()
	names := []string{"stuck.00", "stuck.01", "stuck.02", "stuck.03"}
	var stuck atomic.Bool
	stuck.Store(true)
	target, root, pulls := idleFixture(t, names, func(request []string) bool {
		return request[0] == "stuck.02" && stuck.Load()
	})
	o := idleOptions(root)
	o.batch = 1
	o.streams = 1
	err := run(ctx, o)
	var idle *transferIdleError
	if !errors.As(err, &idle) {
		t.Fatal("idle transfer did not abort with an idle error", err)
	}
	if pullCount(pulls, "stuck.02") != transferIdleAttempts {
		t.Fatal("attempts", pullCount(pulls, "stuck.02"))
	}
	done := map[string]int64{}
	for _, name := range names {
		done[name] = pullCount(pulls, name)
	}
	stuck.Store(false)
	if err := run(ctx, o); err != nil {
		t.Fatal("resume", err)
	}
	for _, name := range names {
		if done[name] > 0 && name != "stuck.02" && pullCount(pulls, name) != done[name] {
			t.Fatal("journal lost progress: verified name pulled again", name)
		}
		if _, err := target.Metadata(ctx, name); err != nil {
			t.Fatal("not copied after resume", name)
		}
	}
}

// A native-deferred child records a bulk intent before its pull. An idle pull
// leaves that intent uncertain, so it is never retried or imported twice.
func TestTransferIdleTimeoutKeepsNativeChildFailClosed(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := []string{"child.00", "child.01", "child.02"}
	for i, name := range names {
		addPoints(t, source, name, float64(i))
	}
	addPoints(t, target, names[1], 100)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	targetHandler := nativeControllerService(t, target)
	var pulls atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/pull" {
			pulls.Add(1)
			// Consume the body so the server notices the client hanging up.
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			return
		}
		targetHandler.ServeHTTP(w, r)
	}))
	defer destination.Close()
	plan := fixturePlan(t, donor.URL, destination.URL, names)
	o := testOptions(plan)
	o.delete = false
	o.deleteMode = "auto"
	o.engine = "bulk"
	o.bulkOnly = true
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	o.engine = "metric"
	o.bulkOnly = false
	o.transferIdle = 200 * time.Millisecond
	var idle *transferIdleError
	if err := run(ctx, o); !errors.As(err, &idle) {
		t.Fatal("native child idle pull", err)
	}
	if pulls.Load() != 1 {
		t.Fatal("native child pull retried", pulls.Load())
	}
	if err := run(ctx, o); err == nil || !strings.Contains(err.Error(), "uncertain bulk import") {
		t.Fatal("uncertain intent not fail-closed", err)
	}
	if pulls.Load() != 1 {
		t.Fatal("uncertain intent imported again", pulls.Load())
	}
}

func TestPlanRejectsInventoryTotalAsPerNameEstimate(t *testing.T) {
	root := t.TempDir()
	list := filepath.Join(root, "names.txt")
	if err := os.WriteFile(list, []byte("a\nb\n"), 0600); err != nil {
		t.Fatal(err)
	}
	node := func(id, endpoint string) map[string]any {
		return map[string]any{"hash": "jump_fnv1a", "replicas": 1, "nodes": []map[string]string{{"id": id, "node": id + "=" + id, "endpoint": endpoint}}}
	}
	for estimate, ok := range map[int64]bool{1228730543: false, 64<<20 + 1: false, 64 << 20: true, 65536: true} {
		spec, err := json.Marshal(map[string]any{"version": 1, "cluster": "c", "site": "s",
			"old": node("source", "http://donor:4242"), "new": node("target", "http://survivor:4242"),
			"inputs": []map[string]any{{"source": "source", "path": list, "format": "lines", "estimated_bytes": estimate}}})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, fmt.Sprintf("spec-%d.json", estimate))
		if err := os.WriteFile(path, spec, 0600); err != nil {
			t.Fatal(err)
		}
		err = Main([]string{"plan", "--spec", path})
		if ok != (err == nil) || !ok && !strings.Contains(err.Error(), "per-name estimate") {
			t.Fatal(estimate, err)
		}
	}
}

// Existing plans that carry an inventory total per name still batch many
// names per transfer instead of one; the import result is unchanged.
func TestImporterBatchesPlansWithImpossiblePerNameEstimates(t *testing.T) {
	ctx := context.Background()
	source, target := localStore(t), localStore(t)
	names := make([]string, 12)
	for i := range names {
		names[i] = fmt.Sprintf("estimate.%02d", i)
		addPoints(t, source, names[i], float64(i))
	}
	src := httptest.NewServer(localService(t, source))
	defer src.Close()
	handler := localService(t, target)
	var pulls atomic.Int64
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/transfer/pull" {
			pulls.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	defer dst.Close()
	root := fixturePlanEstimated(t, src.URL, dst.URL, names, 1228730543)
	o := testOptions(root)
	o.delete = false
	o.deleteMode = "auto"
	o.batch = 8
	if err := run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if pulls.Load() != 2 {
		t.Fatal("pulls for 12 names at batch 8", pulls.Load())
	}
	for _, name := range names {
		if _, err := target.Metadata(ctx, name); err != nil {
			t.Fatal("not copied", name)
		}
	}
}
