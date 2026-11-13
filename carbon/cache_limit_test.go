package carbon

import (
	"os"
	"path/filepath"
	"testing"

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
	// misc today: 1.6 TB at the 3.0 B/point fallback gives ~80B points.
	if p := autoCacheLimit(tb, defaultCacheMemoryShare, 0, 0, 0).points; p < 80e9 || p > 90e9 {
		t.Fatal("default share no longer reproduces misc ceilings", p)
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
	for i := range 2_000_000 {
		core.Add(points.OnePoint("m", float64(i), int64(i)))
	}
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
	for _, body := range []string{"[cache]\nmemory-share = 0\n", "[cache]\nmemory-share = 0.95\n", "[cache]\nbytes-per-point = -1\n"} {
		path := filepath.Join(dir, "bad.conf")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadConfig(path); err == nil {
			t.Fatal("invalid setting accepted", body)
		}
	}
}
