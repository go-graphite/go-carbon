package carbon

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-graphite/go-carbon/cache"
)

func TestDiscoveryCacheTTLConfig(t *testing.T) {
	for _, tt := range []struct {
		name, prefix, ttl string
		invalid           bool
	}{
		{"metadata", "aggregations.secondly._meta", "15s", false},
		{"invalid-prefix", "aggregations.*._meta", "15s", true},
		{"zero", "metadata", "0s", true},
		{"fractional", "metadata", "1500ms", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "go-carbon.conf")
			body := "[carbonserver]\nenabled = true\n[carbonserver.discovery-cache-ttl]\n\"" + tt.prefix + "\" = \"" + tt.ttl + "\"\n"
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := ReadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Whisper.DataDir = t.TempDir()
			got := cfg.Carbonserver.DiscoveryCacheTTL[tt.prefix]
			want, _ := time.ParseDuration(tt.ttl)
			if got == nil || got.Value() != want {
				t.Fatalf("parsed TTL=%v; want %s", got, tt.ttl)
			}
			// The ordinary TOML encoder must preserve the quoted prefix and duration.
			encoded := filepath.Join(t.TempDir(), "roundtrip.conf")
			f, err := os.Create(encoded)
			if err != nil {
				t.Fatal(err)
			}
			encodeErr := toml.NewEncoder(f).Encode(cfg)
			closeErr := f.Close()
			if encodeErr != nil || closeErr != nil {
				t.Fatalf("encode=%v close=%v", encodeErr, closeErr)
			}
			roundtrip, err := ReadConfig(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if roundtrip.Carbonserver.DiscoveryCacheTTL[tt.prefix].Value() != got.Value() {
				t.Fatal("TTL changed in TOML round trip")
			}
			app := &App{Config: cfg}
			_, err = app.configureCarbonserver(cache.New())
			if (err != nil) != tt.invalid {
				t.Fatalf("configureCarbonserver=%v; invalid=%t", err, tt.invalid)
			}
		})
	}
	if got := NewConfig().Carbonserver.DiscoveryCacheTTL; len(got) != 0 {
		t.Fatalf("default overrides=%v; want none", got)
	}
}
