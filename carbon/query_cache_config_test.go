package carbon

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
)

func TestQueryCacheCompressionConfig(t *testing.T) {
	t.Parallel()
	if got := NewConfig().Carbonserver.QueryCacheCompression; got != "gzip" {
		t.Fatalf("default query-cache-compression=%q; want gzip", got)
	}
	for _, tt := range []struct {
		value   string
		invalid bool
	}{{"gzip", false}, {"none", false}, {"zstd", true}} {
		t.Run(tt.value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "go-carbon.conf")
			body := "[carbonserver]\nenabled = true\nquery-cache-compression = \"" + tt.value + "\"\n"
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Whisper.DataDir = t.TempDir()
			if cfg.Carbonserver.QueryCacheCompression != tt.value {
				t.Fatalf("parsed %q; want %q", cfg.Carbonserver.QueryCacheCompression, tt.value)
			}
			app := &App{Config: cfg}
			_, err = app.configureCarbonserver(cache.New())
			if (err != nil) != tt.invalid {
				t.Fatalf("configureCarbonserver=%v; invalid=%t", err, tt.invalid)
			}
		})
	}
}

func TestQueryCacheCanonicalKeysConfig(t *testing.T) {
	if NewConfig().Carbonserver.QueryCacheCanonicalKeys {
		t.Fatal("canonical keys must be opt in")
	}
	path := filepath.Join(t.TempDir(), "go-carbon.conf")
	if err := os.WriteFile(path, []byte("[carbonserver]\nquery-cache-canonical-keys = true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ReadConfig(path)
	if err != nil || !cfg.Carbonserver.QueryCacheCanonicalKeys {
		t.Fatalf("config flag was not parsed: %v", err)
	}
}

func TestQueryRangeCacheConfig(t *testing.T) {
	defaults := NewConfig().Carbonserver
	if defaults.QueryRangeCacheEnabled || defaults.QueryRangeCacheSizeMB != 64 || defaults.QueryRangeCacheMutable.Value() != 30*time.Minute {
		t.Fatal("unexpected range cache defaults")
	}
	for _, size := range []int{0, 1} {
		path := filepath.Join(t.TempDir(), "go-carbon.conf")
		body := fmt.Sprintf("[carbonserver]\nenabled = true\nquery-range-cache-enabled = true\nquery-range-cache-size-mb = %d\nquery-range-cache-mutable-window = \"20m\"\n", size)
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := ReadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Whisper.DataDir = t.TempDir()
		if _, err := (&App{Config: cfg}).configureCarbonserver(cache.New()); (err != nil) != (size == 0) {
			t.Fatalf("size=%d configure err=%v", size, err)
		}
	}
}
