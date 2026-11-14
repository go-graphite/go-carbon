package carbon

import (
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestAutoCacheLimit(t *testing.T) {
	const tb = uint64(1600) << 30
	for _, tc := range []struct {
		name          string
		fixed         float64
		bytes, points int64
		wantBPP       float64
	}{
		{"startup uses the fallback", 0, 0, 0, autoCacheBytesPerPoint},
		{"small cache uses the fallback", 0, 1 << 20, 1000, autoCacheBytesPerPoint},
		{"measured", 0, 7 << 30, 1 << 30, 7},
		{"fixed wins", 2, 7 << 30, 1 << 30, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := autoCacheLimit(tb, 0.15, tc.fixed, tc.bytes, tc.points)
			if got.bytesPerPoint != tc.wantBPP || got.limitBytes != uint64(float64(tb)*0.15) || got.points != uint64(float64(got.limitBytes)/tc.wantBPP) || !got.auto {
				t.Fatalf("%+v", got)
			}
		})
	}
	// A large host gets tens of billions of points with the startup estimate.
	if p := autoCacheLimit(tb, defaultCacheMemoryShare, 0, 0, 0).points; p < 80e9 || p > 90e9 {
		t.Fatal("unexpected default limit", p)
	}
}

func TestCacheMaxSizeAutoOnlyWhenUnset(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) *Config {
		path := filepath.Join(dir, "c.conf")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := ReadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	auto := write("[cache]\nmemory-share = 0.2\n")
	if !auto.Cache.autoMaxSize || auto.Cache.MemoryShare != 0.2 {
		t.Fatal("unset max-size is not automatic", auto.Cache)
	}
	app := &App{}
	core := cache.New()
	defer core.Stop()
	data := make([]points.Point, autoCacheMinMeasuredPoints)
	for i := range data {
		data[i] = points.Point{Value: float64(i), Timestamp: int64(i)}
	}
	core.Add(&points.Points{Metric: "m", Data: data})
	app.resolveCacheLimit(auto, core)
	limit := app.cacheLimit.Load()
	if total, err := availableMemory(); err == nil && total > 0 {
		if !limit.auto || auto.Cache.MaxSize != limit.points || limit.limitBytes != uint64(float64(total)*0.2) {
			t.Fatal("automatic limit not applied", limit, auto.Cache.MaxSize)
		}
		if want := float64(core.EstimatedBytes()) / float64(core.Size()); limit.bytesPerPoint != want {
			t.Fatal("bytes per point not measured", limit.bytesPerPoint, want)
		}
	}
	for _, body := range []string{"[cache]\nmax-size = 5\n", "[cache]\nmax-size = 0\n"} {
		explicit := write(body)
		want := explicit.Cache.MaxSize
		app.resolveCacheLimit(explicit, core)
		if explicit.Cache.autoMaxSize || explicit.Cache.MaxSize != want || app.cacheLimit.Load().auto {
			t.Fatal("explicit max-size overridden", body, explicit.Cache.MaxSize)
		}
	}
	for _, body := range []string{"[cache]\nmemory-share = 0\n", "[cache]\nmemory-share = 0.95\n", "[cache]\nbytes-per-point = -1\n", "[cache]\nmemory-share = nan\n", "[cache]\nmemory-share = inf\n", "[cache]\nbytes-per-point = nan\n", "[cache]\nbytes-per-point = inf\n"} {
		path := filepath.Join(dir, "bad.conf")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadConfig(path); err == nil {
			t.Fatal("invalid setting accepted", body)
		}
	}
}

func TestAutoCacheLimitBounds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		total uint64
		bpp   float64
		want  uint64
	}{
		{"512 GiB", 512 << 30, 0, 27_487_790_694},
		{"1 TiB", 1 << 40, 0, 54_975_581_388},
		{"minimum", 1, 100, 1},
		{"signed maximum", math.MaxUint64, math.SmallestNonzeroFloat64, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := autoCacheLimit(tc.total, defaultCacheMemoryShare, tc.bpp, 0, 0).points; got != tc.want {
				t.Fatalf("points = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCacheLimitReloadAndMetrics(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	total, err := availableMemory()
	if err != nil || total == 0 {
		t.Skipf("memory detection unavailable: %v", err)
	}
	app, _ := newQuotaReloadApp(t, func(cfg *Config) {
		cfg.Whisper.Enabled = false
	})
	defer app.Stop()
	registry := prometheus.NewPedanticRegistry()
	// Gather the same atomically published state through a pedantic registry.
	collector := &cacheLimitCollector{app: app}
	registry.MustRegister(collector)
	for _, tc := range []struct {
		value string
		want  uint64
		auto  bool
	}{
		{"", autoCacheLimit(total, defaultCacheMemoryShare, 0, 0, 0).points, true},
		{"0", 0, false},
		{"5", 5, false},
		{"", autoCacheLimit(total, defaultCacheMemoryShare, 0, 0, 0).points, true},
	} {
		body, err := os.ReadFile(app.ConfigFilename)
		if err != nil {
			t.Fatal(err)
		}
		body = regexp.MustCompile(`(?m)^\s*max-size\s*=.*\n`).ReplaceAll(body, nil)
		if tc.value != "" {
			body = []byte(strings.Replace(string(body), "[cache]", "[cache]\nmax-size = "+tc.value, 1))
		}
		if err := os.WriteFile(app.ConfigFilename, body, 0600); err != nil {
			t.Fatal(err)
		}
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if app.Config.Cache.MaxSize != tc.want || app.heartbeatCacheMax.Load() != int64(tc.want) {
			t.Fatalf("reload value %q: config=%d heartbeat=%d want=%d", tc.value, app.Config.Cache.MaxSize, app.heartbeatCacheMax.Load(), tc.want)
		}
		var cacheMax float64
		app.Cache.Stat(func(name string, value float64) {
			if name == "maxSize" {
				cacheMax = value
			}
		})
		if cacheMax != float64(tc.want) {
			t.Fatalf("cache maxSize=%v want=%d", cacheMax, tc.want)
		}
		wantAuto := 0.0
		wantBytes, wantBPP := 0.0, 0.0
		if tc.auto {
			wantAuto = 1
			wantBytes = float64(uint64(float64(total) * defaultCacheMemoryShare))
			wantBPP = autoCacheBytesPerPoint
		}
		wantMetrics := map[string]float64{
			"cache_limit_auto":            wantAuto,
			"cache_limit_points":          float64(tc.want),
			"cache_limit_memory_share":    defaultCacheMemoryShare,
			"cache_limit_bytes":           wantBytes,
			"cache_limit_bytes_per_point": wantBPP,
		}
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			want, ok := wantMetrics[family.GetName()]
			if !ok {
				t.Fatalf("unexpected metric %s", family.GetName())
			}
			if got := family.Metric[0].GetGauge().GetValue(); got != want {
				t.Errorf("%s=%v want=%v", family.GetName(), got, want)
			}
			delete(wantMetrics, family.GetName())
		}
		if len(wantMetrics) != 0 {
			t.Fatalf("missing metrics: %v", wantMetrics)
		}
	}
}
