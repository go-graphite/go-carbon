package downsize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type retrySelectionBackend struct {
	*fakeSync
	pending bool
}

func (b *retrySelectionBackend) WindowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	d, err := b.Digest(ctx, source, names, w.From, w.Until)
	if b.pending && source == syncD1 {
		for _, n := range names {
			d[n] = []syncDigest{{Hash: syncWindowUnresolved}}
		}
	}
	return d, err
}
func TestSyncWindowRetryResumeAndOtherReplica(t *testing.T) {
	b := &retrySelectionBackend{newFakeSync(), true}
	w := syncRange{Name: "gap", From: syncTestNow - 600, Until: syncTestNow - 120}
	for _, source := range []string{syncD1, syncD2} {
		b.store(source).archives["metric"] = []syncArchive{{Step: 60, Points: 10000}}
		b.store(source).points["metric"] = map[int64]map[int64]float64{60: {w.From: math.Copysign(0, -1), w.From - 60: 77}}
	}
	o := syncOptions{to: syncT, from: []string{syncD1, syncD2}, state: t.TempDir(), batch: 2, streams: 1, maxRounds: 1, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}
	if err := runSync(context.Background(), o, b); !errors.Is(err, ErrSyncResidual) {
		t.Fatal("peer coverage cleared unknown original source", err)
	}
	if _, ok := b.store(syncT).points["metric"][60][w.From-60]; ok {
		t.Fatal("imported full history")
	}
	original, err := os.ReadFile(filepath.Join(o.state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	b.pending = false
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(o.state, "residual.json"))
	if err != nil || strings.TrimSpace(string(data)) != "[]" && strings.TrimSpace(string(data)) != "null" {
		t.Fatal("resolved selection still residual", string(data), err)
	}
	after, err := os.ReadFile(filepath.Join(o.state, "sync.jsonl"))
	if err != nil || !strings.HasPrefix(string(after), string(original)) {
		t.Fatal("rewrote old intents", err)
	}
	copies := len(b.copies)
	if err := runSync(context.Background(), o, b); err != nil {
		t.Fatal(err)
	}
	if len(b.copies) != copies {
		t.Fatal("resolution replay copied again")
	}
}

// Compare journaled pending -> partial -> resolved retries (including process
// resume) with a fresh complete native window copy on 2,048 names. Every archive,
// timestamp and float64 bit must match; target overlap and outside history win.
func TestSyncWindowRetryDifferential2048(t *testing.T) {
	ctx := context.Background()
	source := nativeControllerStore(t)
	targets := []*store.Store{nativeControllerStore(t), nativeControllerStore(t)}
	names := make([]string, 2048)
	base := testMetricNow/60*60 - 120
	for i := range names {
		n := fmt.Sprintf("retry.%04d", i)
		names[i] = n
		value := float64(i) + 0.125
		if i%17 == 0 {
			value = math.Copysign(0, -1)
		}
		if i%19 == 0 {
			value = math.SmallestNonzeroFloat64
		}
		ret := []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 128}}
		snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: n, Retentions: ret, AggregationMethod: store.Sum}}, Archives: []store.Archive{
			{Retention: ret[0], Points: []store.Point{{Timestamp: base - 10, Value: 100}, {Timestamp: base, Value: value}, {Timestamp: base + 10, Value: 2}}},
			{Retention: ret[1], Points: []store.Point{{Timestamp: base - 60, Value: 300}, {Timestamp: base, Value: value}}},
		}}
		if _, err := source.CreateFromSnapshot(ctx, snap); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			snap.Archives[0].Points[1].Value = 42
			snap.Archives[1].Points[1].Value = 43
			for _, dst := range targets {
				if _, err := dst.CreateFromSnapshot(ctx, snap); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	var stage atomic.Int64
	source.SetNativePendingSnapshot(func() (map[string]struct{}, error) {
		pending := map[string]struct{}{}
		for i, n := range names {
			if stage.Load() == 0 || stage.Load() == 1 && i%2 == 0 {
				pending[n] = struct{}{}
			}
		}
		return pending, nil
	})
	handler := nativeControllerService(t, source)
	var oversized atomic.Int64
	donor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/export" {
			body, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			var request struct{ Names []string }
			if err := json.Unmarshal(body, &request); err != nil {
				t.Error(err)
			}
			if stage.Load() < 3 && len(request.Names) == 128 {
				oversized.Add(1)
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer donor.Close()
	servers := []*httptest.Server{httptest.NewServer(nativeControllerService(t, targets[0])), httptest.NewServer(nativeControllerService(t, targets[1]))}
	for _, server := range servers {
		defer server.Close()
	}
	path := t.TempDir()
	jpath := filepath.Join(path, "sync.jsonl")
	j, err := openSyncJournal(jpath, "retry-proof")
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < len(names); start += 128 {
		var skipped []syncSkip
		for _, n := range names[start : start+128] {
			skipped = append(skipped, syncSkip{n, syncWindowSelectionReason})
		}
		if err := j.append(syncRow{Kind: "window-selected", Round: start/128 + 1, Source: donor.URL, Skipped: skipped, SelectionComplete: true}); err != nil {
			t.Fatal(err)
		}
	}
	original, err := os.ReadFile(jpath)
	if err != nil {
		t.Fatal(err)
	}
	o := syncOptions{to: servers[0].URL, from: []string{donor.URL}, state: path, windows: syncRanges{{From: base, Until: base + 60}}, includeShort: true, now: func() time.Time { return time.Unix(testMetricNow, 0) }}
	makeSync := func() *syncer {
		return &syncer{o: o, b: newSyncLiveBackend(syncLiveOptions{}), j: j, forbid: map[string]bool{}}
	}
	s := makeSync()
	if err := s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.resolvedWindowSelections()) != 0 {
		t.Fatal("pending names resolved")
	}
	if oversized.Load() == 0 {
		t.Fatal("oversized selection was not exercised")
	}
	stage.Store(1)
	if err := s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, m := range s.resolvedWindowSelections() {
		count += len(m)
	}
	if count != 1024 {
		t.Fatal("partial resolutions", count)
	}
	j.close()
	j, err = openSyncJournal(jpath, "retry-proof")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	s = makeSync()
	stage.Store(2)
	if err := s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	count = 0
	for _, m := range s.resolvedWindowSelections() {
		count += len(m)
	}
	if count != len(names) {
		t.Fatal("unresolved names", count)
	}
	before := len(j.rows)
	if err := s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if len(j.rows) != before {
		t.Fatal("replayed resolved intents")
	}
	final, err := os.ReadFile(jpath)
	if err != nil || !strings.HasPrefix(string(final), string(original)) {
		t.Fatal("rewrote initial journal", err)
	}
	stage.Store(3) // The independent oracle does not use the selection transport.
	oracle := newSyncLiveBackend(syncLiveOptions{})
	for start := 0; start < len(names); start += 128 {
		result, err := oracle.CopyWindow(ctx, donor.URL, servers[1].URL, names[start:start+128], filepath.Join(t.TempDir(), "copy"), o.windows)
		if err != nil || result.Copied != 128 {
			t.Fatal(result, err)
		}
	}
	for _, n := range names {
		a, ea := targets[0].Snapshot(ctx, n)
		b, eb := targets[1].Snapshot(ctx, n)
		if ea != nil || eb != nil || !reflect.DeepEqual(a.Metadata.MetricConfig, b.Metadata.MetricConfig) || len(a.Archives) != len(b.Archives) {
			t.Fatal(n, ea, eb)
		}
		for ai, archive := range a.Archives {
			other := b.Archives[ai]
			if archive.Retention != other.Retention || len(archive.Points) != len(other.Points) {
				t.Fatal("archive differs", n)
			}
			for pi, p := range archive.Points {
				q := other.Points[pi]
				if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
					t.Fatal("point bits differ", n, p, q)
				}
			}
		}
	}
}

func TestSyncWindowRetryRetainsFailedBoundary(t *testing.T) {
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	base := testMetricNow/60*60 - 120
	ret := []store.Retention{{Step: 10, Count: 128}, {Step: 60, Count: 128}}
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: store.MetricConfig{Name: "boundary", Retentions: ret, AggregationMethod: store.Sum}}, Archives: []store.Archive{
		{Retention: ret[0], Points: []store.Point{{Timestamp: base, Value: 1}}},
		{Retention: ret[1], Points: []store.Point{{Timestamp: base, Value: 2}}},
	}}
	if _, err := source.CreateFromSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	dst := httptest.NewServer(nativeControllerService(t, target))
	defer dst.Close()
	path := t.TempDir()
	j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "boundary")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if err := j.append(syncRow{Kind: "window-selected", Round: 1, Source: donor.URL, Skipped: []syncSkip{{"boundary", syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
		t.Fatal(err)
	}
	s := &syncer{o: syncOptions{to: dst.URL, from: []string{donor.URL}, state: path, includeShort: true, windows: syncRanges{{From: base, Until: base + 20}}, now: func() time.Time { return time.Unix(testMetricNow, 0) }}, b: newSyncLiveBackend(syncLiveOptions{}), j: j, forbid: map[string]bool{}}
	s.addResidual(syncHole{Name: "boundary", Source: donor.URL}, syncWindowSelectionReason)
	for range 2 {
		if err := s.retryWindowSelections(ctx); err != nil {
			t.Fatal(err)
		}
		if len(s.resolvedWindowSelections()) != 0 || len(s.sum.residual) != 1 {
			t.Fatal("incomplete coarse boundary cleared", s.sum.residual)
		}
	}
}

type failedWindowRetryBackend struct {
	*fakeSync
	fail bool
}

func (b *failedWindowRetryBackend) WindowPresence(ctx context.Context, source string, names []string, w syncRange) (map[string][]syncDigest, error) {
	if b.fail && names[0] == "bad" {
		return nil, fmt.Errorf("source window not readable")
	}
	return b.Digest(ctx, source, names, w.From, w.Until)
}
func TestSyncWindowRetrySelectionErrorKeepsResidualAndContinues(t *testing.T) {
	ctx := context.Background()
	b := &failedWindowRetryBackend{newFakeSync(), true}
	w := syncRange{From: syncTestNow - 600, Until: syncTestNow - 120}
	path := t.TempDir()
	j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "errors")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	s := &syncer{o: syncOptions{to: syncT, from: []string{syncD1}, state: path, windows: syncRanges{w}, includeShort: true, now: func() time.Time { return time.Unix(syncTestNow, 0) }}, b: b, j: j, forbid: map[string]bool{}}
	for i, n := range []string{"bad", "good"} {
		b.store(syncD1).archives[n] = []syncArchive{{Step: 60, Points: 1000}}
		b.store(syncD1).points[n] = map[int64]map[int64]float64{60: {w.From: 42}}
		if err := j.append(syncRow{Kind: "window-selected", Round: i + 1, Source: syncD1, Skipped: []syncSkip{{n, syncWindowSelectionReason}}, SelectionComplete: true}); err != nil {
			t.Fatal(err)
		}
		s.addResidual(syncHole{Name: n, Source: syncD1}, syncWindowSelectionReason)
	}
	s.forbid[syncD1] = true
	if err := s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.sum.residual) != 2 {
		t.Fatal("forbidden source lost residuals", s.sum.residual)
	}
	delete(s.forbid, syncD1)
	if err := s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.sum.residual) != 1 || s.sum.residual[0].Name != "bad" {
		t.Fatal("bad source error was cleared or healthy peer stalled", s.sum.residual)
	}
	b.fail = false
	if err := s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.sum.residual) != 0 {
		t.Fatal("recovered source stayed residual", s.sum.residual)
	}
}
