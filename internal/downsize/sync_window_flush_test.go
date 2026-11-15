package downsize

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/buckyd"
)

func TestWindowRetryFlushesOnlySavedPendingNames(t *testing.T) {
	ctx := context.Background()
	source := nativeControllerStore(t)
	addPoints(t, source, "pending", 1)
	addPoints(t, source, "clean", 2)
	var pending atomic.Bool
	pending.Store(true)
	source.SetNativePendingCheck(func(name string) bool { return name == "pending" && pending.Load() })
	service, err := buckyd.New(buckyd.Config{TmpDir: t.TempDir(), MaxTransfers: 2, MaxBodyBytes: 1 << 20}, source)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Stop()
	var flushed []string
	service.SetTransferFlush(func(_ context.Context, names []string) error {
		flushed = append(flushed, names...)
		pending.Store(false)
		return nil
	})
	donor := httptest.NewServer(service.Handler())
	defer donor.Close()
	path := t.TempDir()
	j, err := openSyncJournal(filepath.Join(path, "sync.jsonl"), "pending-flush")
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	row := syncRow{Kind: "window-selected", Round: 1, Source: donor.URL, Skipped: []syncSkip{{"pending", syncWindowSelectionReason}}, SelectionComplete: true}
	if err = j.append(row); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(j.find(func(r syncRow) bool { return r.Kind == "window-selected" })[0])
	// The point lies outside this old window. Only a complete fresh native
	// selection may retire it; the ordinary export is never imported.
	window := syncRange{From: testMetricNow - 600, Until: testMetricNow - 500}
	s := &syncer{o: syncOptions{to: syncT, from: []string{donor.URL}, state: path, windows: syncRanges{window}, includeShort: true, now: func() time.Time { return time.Unix(testMetricNow, 0) }}, b: newSyncLiveBackend(syncLiveOptions{}), j: j, forbid: map[string]bool{}}
	s.addResidual(syncHole{Name: "pending", Source: donor.URL}, syncWindowSelectionReason)
	if err = s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.sum.residual) != 0 || !reflect.DeepEqual(flushed, []string{"pending"}) {
		t.Fatalf("pending selection not flushed/resolved: residual=%v flushed=%v", s.sum.residual, flushed)
	}
	after, _ := json.Marshal(j.find(func(r syncRow) bool { return r.Kind == "window-selected" })[0])
	if string(before) != string(after) {
		t.Fatal("saved selection rewritten")
	}
	if err = s.retryWindowSelections(ctx); err != nil {
		t.Fatal(err)
	}
	if len(flushed) != 1 {
		t.Fatal("resolved selection flushed again", flushed)
	}
}
