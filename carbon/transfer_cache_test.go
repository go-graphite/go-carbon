package carbon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
)

func TestTransferFlushesPendingCacheBeforeExport(t *testing.T) {
	path, cfg := sharedAppConfig(t)
	cfg.Cache.WriteoutMinPoints, cfg.Cache.WriteoutMaxDelay = 1000, Duration{time.Hour}
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	ctx := context.Background()
	name := "transfer.pending"
	_, err := app.MetricStore.Create(ctx, store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 3600}}, AggregationMethod: store.Average})
	if err != nil {
		t.Fatal(err)
	}
	app.Cache.Add(points.OnePoint(name, 42.5, time.Now().Unix()-2))
	if !app.Cache.Has(name) {
		t.Fatal("fixture was not queued")
	}
	body, _ := json.Marshal([]string{name})
	r := httptest.NewRequest(http.MethodPost, "/v1/transfer/export?wire=2", bytes.NewReader(body))
	w := httptest.NewRecorder()
	app.Buckyd.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("export %d: %s", w.Code, w.Body.String())
	}
	if app.Cache.Has(name) {
		t.Fatal("export left original queued points")
	}
	records, err := store.ReadTransfer(bytes.NewReader(w.Body.Bytes()), int64(w.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	target, err := store.Open(t.TempDir(), store.Options{ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if _, err := target.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	got, err := target.Snapshot(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Archives[0].Points) != 1 || got.Archives[0].Points[0].Value != 42.5 {
		t.Fatal("queued point absent", got)
	}
	operation := strings.Repeat("a", 64)
	if err := app.MetricStore.StageTransferDelete(ctx, operation, []store.TransferVersion{records[0].Version}); err != nil {
		t.Fatal(err)
	}
	app.Cache.Add(points.OnePoint(name, 84, time.Now().Unix()-1))
	if _, err := app.MetricStore.RetireTransferredStore(ctx, operation); err == nil {
		t.Fatal("retirement dropped pending input")
	}
	pending, err := app.MetricStore.PendingTransferDelete(ctx, operation, 256)
	if err != nil || len(pending) != 1 || pending[0] != name {
		t.Fatal("pending delta not surfaced", pending, err)
	}
}
