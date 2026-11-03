package carbon

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// SIGHUP calls ReloadConfig. Check its serving effects over real HTTP, not just
// the parsed Config: equivalent request order hits the canonical cache, prefix
// reuse starts/stops, and neither the listener nor the store is replaced.
func TestReadPathFlagsReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	app, _ := newQuotaReloadApp(t, func(cfg *Config) {
		cfg.Whisper.Enabled = false
		cfg.Whisper.StorageBackend = "pebble-chunk"
		cfg.Whisper.StoreDir = filepath.Join(t.TempDir(), "store")
		cfg.Whisper.StoreCacheSize, cfg.Whisper.StoreMemTableSize = 1<<20, 1<<20
		cfg.Whisper.StoreEncoding = "exact"
		cfg.Carbonserver.QueryCacheEnabled = true
		cfg.Carbonserver.MetricsAsCounters = true
		cfg.Carbonserver.QueryRangeCacheSizeMB = 1
	})
	defer app.Stop()
	listener, db := app.Carbonserver, app.MetricStore
	now := int(time.Now().Unix())
	for _, name := range []string{"readpath.a", "readpath.b"} {
		if _, err := db.Create(context.Background(), store.MetricConfig{Name: name, Retentions: []store.Retention{{Step: 1, Count: 86400}}, AggregationMethod: store.Average}); err != nil {
			t.Fatal(err)
		}
		if err := db.UpdateMany(context.Background(), name, []store.Point{{Timestamp: int64(now - 7200), Value: 3.25}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := listener.RefreshMetricStoreIndex(); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	render := func(from int, reverse bool) {
		t.Helper()
		targets := "target=readpath.a&target=readpath.b"
		if reverse {
			targets = "target=readpath.b&target=readpath.a"
		}
		url := fmt.Sprintf("http://%s/render/?format=json&%s&from=%d&until=%d", app.Config.Carbonserver.Listen, targets, from, now-3600)
		response, err := client.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 {
			t.Fatalf("render status=%d body=%s err=%v", response.StatusCode, body, err)
		}
	}
	counter := func(name string) float64 {
		var value float64
		listener.Stat(func(metric string, v float64) {
			if metric == name {
				value = v
			}
		})
		return value
	}
	cfg, err := ReadConfig(app.ConfigFilename)
	if err != nil {
		t.Fatal(err)
	}
	for stage, enabled := range []bool{true, false, true} {
		cfg.Carbonserver.QueryCacheCanonicalKeys = enabled
		cfg.Carbonserver.QueryRangeCacheEnabled = enabled
		writeBatchingConfig(t, app.ConfigFilename, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if listener != app.Carbonserver || db != app.MetricStore {
			t.Fatal("reload replaced serving components")
		}
		from := now - 7200 + stage*30
		before := counter("range_cache_hit")
		render(from, false)
		if counter("range_cache_hit") != before {
			t.Fatal("new/disabled namespace unexpectedly hit")
		}
		render(from+1, false) // new exact key, overlapping historical prefix
		if got := counter("range_cache_hit") - before; got != map[bool]float64{false: 0, true: 2}[enabled] {
			t.Fatalf("prefix flag=%v hits=%v", enabled, got)
		}
		// An unrelated reload must retain the populated prefix namespace.
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		before = counter("range_cache_hit")
		render(from+2, false)
		if got := counter("range_cache_hit") - before; got != map[bool]float64{false: 0, true: 2}[enabled] {
			t.Fatalf("unrelated reload lost prefix: enabled=%v hits=%v", enabled, got)
		}
		before = counter("query_cache_hit")
		render(from+2, true)
		if got := counter("query_cache_hit") - before; got != map[bool]float64{false: 0, true: 1}[enabled] {
			t.Fatalf("canonical flag=%v hits=%v", enabled, got)
		}
	}
	// Invalid cache settings must leave the currently serving policy intact.
	cfg.Carbonserver.QueryRangeCacheSizeMB = 0
	writeBatchingConfig(t, app.ConfigFilename, cfg)
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("invalid enabled cache policy accepted")
	}
	if app.Config.Carbonserver.QueryRangeCacheSizeMB != 1 {
		t.Fatal("failed reload published invalid config")
	}
}
