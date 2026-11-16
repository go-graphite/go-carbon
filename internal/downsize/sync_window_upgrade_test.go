package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A donor upgrade cannot change an old client's ordinary pin. Seed its durable
// receipt, then resume with committed-window pins while writes remain pending.
func TestSyncWindowUpgradeResumesOrdinaryReceipt(t *testing.T) {
	t.Parallel()
	testSyncWindowUpgradeResumesOrdinaryReceipt(t, []string{
		"batch.cron.partition.shard.1.retry.0.failed.sum",
		"batch.cron.partition.shard.2.retry.0.total.min",
		"batch.cron.partition.shard.4.retry.0.success.percentiles.99",
		"batch.cron.partition.shard.5.retry.0.wallclock.count",
		"batch.cron.progress.shard.1.retry.0.eta_ms.percentiles.50",
	})
}

func testSyncWindowUpgradeResumesOrdinaryReceipt(t *testing.T, deferred []string) {
	t.Helper()
	ctx := context.Background()
	source, target := nativeControllerStore(t), nativeControllerStore(t)
	names := append(append([]string(nil), deferred...), "completed.control")
	for _, name := range names {
		addPoints(t, source, name, math.Copysign(0, -1))
	}
	// The active cache never clears during this test. Only the client pin mode
	// changes; an ordinary fresh pin would still defer these names.
	source.SetNativePendingCheck(func(name string) bool { return name != names[len(names)-1] })
	addPoints(t, target, names[0], 42)
	donor := httptest.NewServer(nativeControllerService(t, source))
	defer donor.Close()
	handler := nativeControllerService(t, target)
	var pulls atomic.Int32
	var parentID string
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/native/pull" {
			n := pulls.Add(1)
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var request struct{ Range store.NativeRange }
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Error(err)
			}
			if n > 1 && (request.Range.DeferredFrom != parentID || !bytes.Equal(request.Range.Selection, []byte{31})) {
				t.Errorf("resume includes completed ordinal or loses receipt chain: %+v", request.Range)
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer destination.Close()
	windows := syncRanges{{From: testMetricNow - 10, Until: testMetricNow}}
	path := filepath.Join(t.TempDir(), "copy")
	raw, _ := json.Marshal([]any{donor.URL, destination.URL, names, windows[0].From, windows[0].Until})
	hash := sha256.Sum256(raw)
	id := hex.EncodeToString(hash[:])
	j, err := openJournal(fmt.Sprintf("%s.window-0-%s", path, id[:16]), id)
	if err != nil {
		t.Fatal(err)
	}
	m := &mover{options: options{bulkCombine: true, transferIdle: defaultTransferIdle, nativeWindow: &store.NativeWindow{From: windows[0].From, Until: windows[0].Until}}, client: http.DefaultClient, journal: j, planData: &manifest{PlanID: id}}
	old := &nativeController{mover: m, sessions: map[string]*nativeRemoteSession{}}
	task := task{Stream: id, Source: donor.URL, Target: destination.URL, End: int64(len(names))}
	err = old.executeRange(ctx, task, names)
	old.release()
	saved := j.tasks[taskKey(task)]
	j.close()
	if err != nil || saved.Native == nil {
		t.Fatal("seed old client receipt", saved, err)
	}
	receipt := saved.Native.Receipt
	parentID = receipt.Identity
	if receipt.CopiedCount != 1 || receipt.DeferredCount != len(deferred) || len(receipt.Skipped) != 0 {
		t.Fatal("ordinary pin must omit pending names without a reason", receipt)
	}
	b := newSyncLiveBackend(syncLiveOptions{})
	for range 2 {
		result, err := b.CopyWindow(ctx, donor.URL, destination.URL, names, path, windows)
		if err != nil || result.Copied != len(names) || len(result.Failed) != 0 || pulls.Load() != 2 {
			t.Fatal("upgrade failed or replayed completed work", result, err, pulls.Load())
		}
	}
	for i, name := range names {
		snapshot, err := target.Snapshot(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		want := math.Copysign(0, -1)
		if i == 0 {
			want = 42 // existing target values win, including signed zero conflicts
		}
		found := false
		for _, archive := range snapshot.Archives {
			for _, point := range archive.Points {
				if point.Timestamp == testMetricNow-10 {
					found = true
					if math.Float64bits(point.Value) != math.Float64bits(want) {
						t.Fatalf("%s: bits %x, want %x", name, math.Float64bits(point.Value), math.Float64bits(want))
					}
				}
			}
		}
		if !found {
			t.Fatal("missing window point", name)
		}
	}
}
