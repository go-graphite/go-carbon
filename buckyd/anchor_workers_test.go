package buckyd

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

const anchorNow = 1793984000

type anchorMapIO map[string][]byte

func (m anchorMapIO) Get(k []byte) ([]byte, error) {
	v, ok := m[string(k)]
	if !ok {
		return nil, chunkstore.ErrNotFound
	}
	return bytes.Clone(v), nil
}
func (m anchorMapIO) Set(k, v []byte) error { m[string(k)] = bytes.Clone(v); return nil }
func (m anchorMapIO) Delete(k []byte) error { delete(m, string(k)); return nil }
func (m anchorMapIO) DeletePrefix(p []byte) error {
	for k := range m {
		if strings.HasPrefix(k, string(p)) {
			delete(m, k)
		}
	}
	return nil
}
func (m anchorMapIO) Visit(p []byte, fn func([]byte, []byte) error) error {
	keys := make([]string, 0, len(m))
	for k := range m {
		if strings.HasPrefix(k, string(p)) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	for _, k := range keys {
		if err := fn([]byte(k), m[k]); err != nil {
			return err
		}
	}
	return nil
}

func anchorStore(t testing.TB) *chunkstore.Store {
	t.Helper()
	db, err := chunkstore.Open(t.TempDir(), chunkstore.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 8 << 20, MemTableSize: 4 << 20, MetadataCacheSize: 4 << 20, Now: func() time.Time { return time.Unix(anchorNow, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// anchorMetric is a continuation metric whose first archive (10s:10m) may be
// rewritten in the pre-fix importer's absolute epoch slots ("absolute") or
// additionally carry an off-grid alias that blocks repair ("aliased").
type anchorMetric struct {
	name   string
	layout string
	shift  int64
	heavy  int // dense points in an extra 60s archive, modelling the slow tail
}

func anchorTransfer(t testing.TB, m anchorMetric) chunkstore.TransferMetric {
	t.Helper()
	small := chunkstore.Retention{Step: 10, Count: 60}
	cfg := chunkstore.MetricConfig{Name: m.name, Retentions: []chunkstore.Retention{small}, AggregationMethod: chunkstore.Sum}
	points := []chunkstore.Point{
		{Timestamp: anchorNow - 220 - 10*m.shift, Value: math.Copysign(0, -1)},
		{Timestamp: anchorNow - 190 - 10*m.shift, Value: math.Float64frombits(0x7ff8000000000042)},
		{Timestamp: anchorNow - 10 - 10*m.shift, Value: float64(m.shift) + 0.125},
	}
	snap := chunkstore.Snapshot{Metadata: chunkstore.Metadata{MetricConfig: cfg}, Archives: []chunkstore.Archive{{Retention: small, Points: points}}}
	if m.heavy > 0 {
		big := chunkstore.Retention{Step: 60, Count: m.heavy}
		dense := make([]chunkstore.Point, m.heavy)
		for i := range dense {
			dense[i] = chunkstore.Point{Timestamp: anchorNow - anchorNow%60 - 60*int64(m.heavy-i), Value: float64(i) / 7}
		}
		snap.Metadata.Retentions = append(snap.Metadata.Retentions, big)
		snap.Archives = append(snap.Archives, chunkstore.Archive{Retention: big, Points: dense})
		cfg = snap.Metadata.MetricConfig
	}
	kv := anchorMapIO{}
	if err := (&whisperio.ContinuationCodec{}).RestoreArchives(context.Background(), kv, snap, func() time.Time { return time.Unix(anchorNow, 0) }); err != nil {
		t.Fatal(err)
	}
	if m.layout != "relative" {
		// Archive 0 fits one 4 KiB classic page that restore already allocated;
		// only its point group moves to absolute epoch slots.
		var prefix []byte
		for k := range kv {
			key := []byte(k)
			if len(key) == 20 && key[9] == 'p' && binary.BigEndian.Uint16(key[10:]) == 0 && binary.BigEndian.Uint32(key[12:]) == 0 {
				prefix = key[:16]
			}
		}
		if prefix == nil {
			t.Fatal("fixture has no archive-0 point group")
		}
		_ = kv.DeletePrefix(prefix)
		var block chunkstore.PointBlock
		for _, p := range points {
			slot := int(p.Timestamp / 10 % 60)
			block.Present[slot/64] |= 1 << uint(slot%64)
			block.Points[slot] = p
		}
		if m.layout == "aliased" {
			block.Present[0] |= 1 << 5
			block.Points[5] = chunkstore.Point{Timestamp: points[0].Timestamp + 1, Value: 8}
		}
		_ = kv.Set(binary.BigEndian.AppendUint32(bytes.Clone(prefix), 0), chunkstore.EncodePointBlock(&block))
	}
	r := chunkstore.TransferMetric{Config: cfg, Version: chunkstore.TransferVersion{Name: m.name}, Continuation: true}
	_ = kv.Visit(nil, func(k, v []byte) error {
		r.Keys = append(r.Keys, chunkstore.RelayoutKV{Key: k, Value: bytes.Clone(v)})
		return nil
	})
	return r
}

func importAnchorFixture(t testing.TB, db *chunkstore.Store, metrics []anchorMetric) {
	t.Helper()
	records := make([]chunkstore.TransferMetric, 0, len(metrics))
	for _, m := range metrics {
		records = append(records, anchorTransfer(t, m))
	}
	if _, err := db.ImportTransfer(context.Background(), records); err != nil {
		t.Fatal(err)
	}
}

// serialAnchorRows is the pre-pool request loop, kept verbatim as the oracle.
func serialAnchorRows(ctx context.Context, db *chunkstore.Store, names []string, apply bool) []anchorRow {
	rows := make([]anchorRow, 0, len(names))
	for _, name := range names {
		result, err := db.RepairArchiveAnchors(ctx, name, apply)
		result.Name = name
		item := anchorRow{AnchorRepairResult: result}
		if err != nil {
			item.Error = err.Error()
		}
		rows = append(rows, item)
		if ctx.Err() != nil {
			break
		}
	}
	return rows
}

// comparableRows drops wall-clock timings and normalizes through the wire.
func comparableRows(t *testing.T, rows any) []anchorRow {
	t.Helper()
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	var out []anchorRow
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	for i := range out {
		out[i].PreparationNanos, out[i].LockNanos = 0, 0
	}
	return out
}

func storedAnchorBytes(t *testing.T, db *chunkstore.Store, names []string) []chunkstore.TransferMetric {
	t.Helper()
	records, err := db.ExportTransfer(context.Background(), names)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func TestArchiveAnchorWorkersMatchSerialOnFixture(t *testing.T) {
	var metrics []anchorMetric
	for i := range 24 {
		layout := []string{"relative", "absolute", "absolute", "aliased"}[i%4]
		metrics = append(metrics, anchorMetric{name: fmt.Sprintf("census.m%02d", i), layout: layout, shift: int64(i), heavy: map[bool]int{true: 4096}[i%7 == 0]})
	}
	var existing []string
	for _, m := range metrics {
		existing = append(existing, m.name)
	}
	// 32 names: duplicates (a repeated apply must observe its predecessor),
	// unknown names (per-row errors) and every layout, in mixed order.
	names := append(slices.Clone(existing[:20]), "census.missing", "census.m01", "census.m02", "census.m21", "census.m22", "census.m23", "census.m01", "census.absent", "census.m20", "census.m05", "census.m03", "census.m00")
	if len(names) != maxAnchorNames {
		t.Fatalf("fixture has %d names", len(names))
	}
	oracle, candidate := anchorStore(t), anchorStore(t)
	importAnchorFixture(t, oracle, metrics)
	importAnchorFixture(t, candidate, metrics)
	secret := []byte("anchor-secret")
	service, err := New(Config{TmpDir: t.TempDir(), MaxBodyBytes: 1 << 20, MaintenanceWorkers: 8}, candidate)
	if err != nil {
		t.Fatal(err)
	}
	service.secret = secret
	invoke := func(apply bool) []anchorRow {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"names": names, "apply": apply})
		req := httptest.NewRequest(http.MethodPost, "/v1/maintenance/archive-anchors", bytes.NewReader(body))
		req.Header.Set(authHeader, token(t, secret, []string{"*"}, []string{map[bool]string{false: "read", true: "update"}[apply]}))
		w := httptest.NewRecorder()
		service.Handler().ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("maintenance %d %s", w.Code, w.Body)
		}
		var rows []anchorRow
		if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil {
			t.Fatal(err)
		}
		return comparableRows(t, rows)
	}
	before := storedAnchorBytes(t, candidate, existing)
	if !reflect.DeepEqual(before, storedAnchorBytes(t, oracle, existing)) {
		t.Fatal("fixture stores differ before repair")
	}
	ctx := context.Background()
	for _, step := range []struct {
		name  string
		apply bool
	}{{"dry-run", false}, {"apply", true}, {"idempotent dry-run", false}, {"repeated apply", true}} {
		want := comparableRows(t, serialAnchorRows(ctx, oracle, names, step.apply))
		got := invoke(step.apply)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s rows differ from serial:\n got %+v\nwant %+v", step.name, got, want)
		}
		if !reflect.DeepEqual(storedAnchorBytes(t, candidate, existing), storedAnchorBytes(t, oracle, existing)) {
			t.Fatalf("%s stored bytes differ from serial", step.name)
		}
		changed, applied, blocked, failed := 0, 0, 0, 0
		for _, r := range got {
			for _, a := range r.Archives {
				changed += map[bool]int{true: 1}[a.Changed]
				blocked += map[bool]int{true: 1}[a.Blocked != ""]
			}
			applied += map[bool]int{true: 1}[r.Applied]
			failed += map[bool]int{true: 1}[r.Error != ""]
		}
		t.Logf("%s: changed=%d applied=%d blocked=%d errors=%d", step.name, changed, applied, blocked, failed)
		switch step.name {
		case "dry-run":
			if changed == 0 || blocked == 0 || applied != 0 || failed != 2 {
				t.Fatal("fixture does not exercise misplaced, blocked and missing names")
			}
			if !reflect.DeepEqual(storedAnchorBytes(t, candidate, existing), before) {
				t.Fatal("dry-run changed stored bytes")
			}
		case "apply":
			if applied == 0 {
				t.Fatal("apply repaired nothing")
			}
			if reflect.DeepEqual(storedAnchorBytes(t, candidate, existing), before) {
				t.Fatal("apply left stored bytes unchanged")
			}
		default:
			if applied != 0 {
				t.Fatal("repair was not idempotent")
			}
		}
	}
}

func TestAnchorRowsKeepRequestOrderAndBoundWorkers(t *testing.T) {
	names := make([]string, maxAnchorNames)
	for i := range names {
		names[i] = fmt.Sprintf("n%02d", i)
	}
	const workers = 8
	var inFlight, peak atomic.Int32
	full := make(chan struct{})
	var fullOnce sync.Once
	rows := anchorRows(context.Background(), names, workers, func(_ context.Context, name string) (chunkstore.AnchorRepairResult, error) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		if n == workers {
			fullOnce.Do(func() { close(full) })
		}
		select {
		case <-full:
		case <-time.After(5 * time.Second):
		}
		var i int
		_, _ = fmt.Sscanf(name, "n%d", &i)
		// Later names finish first, so completion order is reversed.
		time.Sleep(time.Duration(len(names)-i) * 100 * time.Microsecond)
		return chunkstore.AnchorRepairResult{Retries: i}, nil
	})
	if len(rows) != len(names) {
		t.Fatalf("got %d rows", len(rows))
	}
	for i, r := range rows {
		if r.Name != names[i] || r.Retries != i || r.Error != "" {
			t.Fatalf("row %d out of order: %+v", i, r)
		}
	}
	if got := peak.Load(); got != workers {
		t.Fatalf("peak concurrency %d, want %d", got, workers)
	}
}

func TestAnchorRowsRepeatedNamesStaySequential(t *testing.T) {
	names := []string{"a", "b", "a", "c", "a", "b", "d"}
	var mu sync.Mutex
	calls := map[string]int{}
	busy := map[string]bool{}
	rows := anchorRows(context.Background(), names, 8, func(_ context.Context, name string) (chunkstore.AnchorRepairResult, error) {
		mu.Lock()
		if busy[name] {
			mu.Unlock()
			return chunkstore.AnchorRepairResult{}, errors.New("repeated name ran concurrently")
		}
		busy[name] = true
		seen := calls[name]
		calls[name]++
		mu.Unlock()
		time.Sleep(time.Millisecond)
		mu.Lock()
		busy[name] = false
		mu.Unlock()
		// Applied mimics a serial pass: only the first occurrence publishes.
		return chunkstore.AnchorRepairResult{Retries: seen, Applied: seen == 0}, nil
	})
	want := []int{0, 0, 1, 0, 2, 1, 0}
	for i, r := range rows {
		if r.Error != "" || r.Name != names[i] || r.Retries != want[i] || r.Applied != (want[i] == 0) {
			t.Fatalf("row %d %+v, want occurrence %d", i, r, want[i])
		}
	}
	if len(rows) != len(names) {
		t.Fatalf("got %d rows", len(rows))
	}
}

func TestAnchorRowsKeepPerRowErrors(t *testing.T) {
	names := []string{"ok.1", "bad.1", "ok.2", "bad.2", "ok.3"}
	for _, workers := range []int{1, 8} {
		rows := anchorRows(context.Background(), names, workers, func(_ context.Context, name string) (chunkstore.AnchorRepairResult, error) {
			result := chunkstore.AnchorRepairResult{Archives: []chunkstore.AnchorArchive{{Points: len(name)}}}
			if strings.HasPrefix(name, "bad") {
				return result, errors.New("failed " + name)
			}
			return result, nil
		})
		if len(rows) != len(names) {
			t.Fatalf("workers=%d: %d rows", workers, len(rows))
		}
		for i, r := range rows {
			wantErr := ""
			if strings.HasPrefix(names[i], "bad") {
				wantErr = "failed " + names[i]
			}
			if r.Name != names[i] || r.Error != wantErr || len(r.Archives) != 1 || r.Archives[0].Points != len(names[i]) {
				t.Fatalf("workers=%d row %d: %+v", workers, i, r)
			}
		}
	}
}

func TestAnchorRowsStopStartingNamesOnCancel(t *testing.T) {
	names := make([]string, maxAnchorNames)
	for i := range names {
		names[i] = fmt.Sprintf("n%02d", i)
	}
	const workers, cancelAt = 4, 5
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var started, lateStarts atomic.Int32
	rows := anchorRows(ctx, names, workers, func(ctx context.Context, name string) (chunkstore.AnchorRepairResult, error) {
		started.Add(1)
		var i int
		_, _ = fmt.Sscanf(name, "n%d", &i)
		switch {
		case i == cancelAt:
			cancel()
			return chunkstore.AnchorRepairResult{}, ctx.Err()
		case i > cancelAt:
			// In-flight names observe cancellation like RepairArchiveAnchors.
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				lateStarts.Add(1)
			}
			return chunkstore.AnchorRepairResult{}, ctx.Err()
		}
		return chunkstore.AnchorRepairResult{}, nil
	})
	if lateStarts.Load() != 0 {
		t.Fatal("cancellation did not reach in-flight names")
	}
	// Names are claimed in order; at most workers-1 more were in flight when
	// cancelAt cancelled, and nothing starts afterwards.
	if n := int(started.Load()); n > cancelAt+workers || n < cancelAt+1 {
		t.Fatalf("started %d names", n)
	}
	if len(rows) != int(started.Load()) {
		t.Fatalf("reply %d rows, started %d", len(rows), started.Load())
	}
	for i, r := range rows {
		if r.Name != names[i] || (i >= cancelAt) != (r.Error != "") {
			t.Fatalf("row %d %+v", i, r)
		}
	}
}

func TestAnchorRowsCancelledRequestThroughHandler(t *testing.T) {
	db := anchorStore(t)
	importAnchorFixture(t, db, []anchorMetric{{name: "census.a", layout: "absolute"}, {name: "census.b", layout: "relative", shift: 1}})
	s, err := New(Config{TmpDir: t.TempDir(), MaxBodyBytes: 1 << 20}, db)
	if err != nil {
		t.Fatal(err)
	}
	s.secret = []byte("anchor-secret")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/maintenance/archive-anchors", bytes.NewBufferString(`{"names":["census.a","census.b","census.a"],"apply":true}`))
	req.Header.Set(authHeader, token(t, s.secret, []string{"*"}, []string{"update"}))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	var rows []anchorRow
	if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil || w.Code != 200 || len(rows) != 0 {
		t.Fatalf("cancelled request %d %s %v", w.Code, w.Body, err)
	}
	dry, err := db.RepairArchiveAnchors(context.Background(), "census.a", false)
	if err != nil || len(dry.Archives) == 0 || !dry.Archives[0].Changed {
		t.Fatalf("cancelled request repaired: %+v %v", dry, err)
	}
	if len(s.maintenance) != 0 {
		t.Fatal("maintenance slot leaked")
	}
}

func TestAnchorRowsReraisePanicOnCaller(t *testing.T) {
	names := []string{"a", "b", "boom", "c", "d"}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		anchorRows(context.Background(), names, 4, func(_ context.Context, name string) (chunkstore.AnchorRepairResult, error) {
			if name == "boom" {
				panic("planner bug")
			}
			return chunkstore.AnchorRepairResult{}, nil
		})
	}()
	if text, _ := recovered.(string); !strings.Contains(text, "planner bug") || !strings.Contains(text, "anchorRows") {
		t.Fatalf("recovered %v", recovered)
	}
}

func TestMaintenanceConcurrencyConfig(t *testing.T) {
	db, err := chunkstore.Open(t.TempDir(), chunkstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, test := range []struct {
		requests, workers         int
		wantRequests, wantWorkers int
		err                       string
	}{
		{0, 0, defaultMaintenanceRequests, defaultMaintenanceWorkers, ""},
		{1, 1, 1, 1, ""},
		{6, 16, 6, 16, ""},
		{maxMaintenanceRequests, maxAnchorNames, maxMaintenanceRequests, maxAnchorNames, ""},
		{-1, 0, 0, 0, "max-maintenance-requests"},
		{maxMaintenanceRequests + 1, 0, 0, 0, "max-maintenance-requests"},
		{0, -1, 0, 0, "maintenance-workers"},
		{0, maxAnchorNames + 1, 0, 0, "maintenance-workers"},
	} {
		s, err := New(Config{TmpDir: t.TempDir(), MaxMaintenanceRequests: test.requests, MaintenanceWorkers: test.workers}, db)
		if test.err != "" {
			if err == nil || !strings.Contains(err.Error(), test.err) {
				t.Fatalf("%+v accepted or wrong error: %v", test, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if cap(s.maintenance) != test.wantRequests || s.config.MaintenanceWorkers != test.wantWorkers {
			t.Fatalf("%+v: requests=%d workers=%d", test, cap(s.maintenance), s.config.MaintenanceWorkers)
		}
	}
}
