package carbon

import (
	"os"
	"path/filepath"
	"testing"

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
