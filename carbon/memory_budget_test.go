package carbon

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestMemoryBudget(t *testing.T) {
	for _, tc := range []struct {
		name, budget, overrides, gomemlimit string
		invalid                             bool
	}{
		{name: "disabled"},
		{name: "zero", budget: "0"},
		{name: "bytes", budget: "107374182400"},
		{name: "unit", budget: "100GiB"},
		{name: "small unit", budget: "102400MiB"},
		{name: "percent", budget: "50%"},
		{name: "all", budget: "100%"},
		{name: "overrides", budget: "50%", overrides: "[whisper]\nstore-cache-size=1234\nstore-metadata-cache-size=0\n[cache]\nmax-bytes=0\n", gomemlimit: "off"},
		{name: "env", budget: "50%", gomemlimit: "4GiB"},
		{name: "too small", budget: "1000", invalid: true},
		{name: "zero percent", budget: "0%", invalid: true},
		{name: "above percent", budget: "101%", invalid: true},
		{name: "negative", budget: "-1", invalid: true},
		{name: "overflow", budget: "18446744073709551615", invalid: true},
		{name: "unit too small", budget: "63MiB", invalid: true},
		{name: "unit overflow", budget: "8388608TiB", invalid: true},
		{name: "unknown unit", budget: "1GB", invalid: true},
		{name: "spaced unit", budget: "1 GiB", invalid: true},
		{name: "bare unit", budget: "GiB", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewConfig()
			defined, err := toml.Decode("[common]\nmemory-budget="+fmt.Sprintf("%q", tc.budget)+"\n"+tc.overrides, cfg)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			available := func() (uint64, error) { calls++; return 200 << 30, nil }
			err = cfg.resolveMemoryBudget(defined, available, tc.gomemlimit)
			if (err != nil) != tc.invalid {
				t.Fatalf("resolve = %v", err)
			}
			if tc.invalid {
				return
			}
			if strings.HasSuffix(tc.budget, "%") != (calls == 1) {
				t.Fatalf("memory detection calls: %d", calls)
			}
			if tc.budget == "" || tc.budget == "0" {
				if cfg.Whisper.StoreCacheSize != 256<<20 || cfg.Whisper.StoreMetadataCacheSize != 0 || cfg.Cache.MaxBytes != 0 || cfg.memoryLimit != 0 {
					t.Fatal("defaults changed")
				}
				return
			}
			if tc.overrides != "" {
				if cfg.Whisper.StoreCacheSize != 1234 || cfg.Whisper.StoreMetadataCacheSize != 0 || cfg.Cache.MaxBytes != 0 {
					t.Fatal("explicit overrides lost")
				}
			} else {
				budget := uint64(100 << 30)
				if tc.budget == "100%" {
					budget = 200 << 30
				}
				if cfg.Whisper.StoreCacheSize != int64(budget*13/100) || cfg.Whisper.StoreMetadataCacheSize != int64(budget*13/100) {
					t.Fatalf("incorrect shares: %+v", cfg)
				}
				if cfg.Cache.MaxBytes != 0 {
					t.Fatal("write-cache byte limit must not be derived")
				}
			}
			if (cfg.memoryLimit == 0) != (tc.gomemlimit != "") {
				t.Fatal("GOMEMLIMIT override lost")
			}
		})
	}
}

func TestMemoryBudgetReadConfig(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")
	path := filepath.Join(t.TempDir(), "go-carbon.conf")
	if err := os.WriteFile(path, []byte("[common]\nmemory-budget=\"107374182400\"\n[whisper]\nstore-cache-size=0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ReadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Whisper.StoreCacheSize != 0 || cfg.Whisper.StoreMetadataCacheSize != 13*(1<<30) || cfg.Cache.MaxBytes != 0 || cfg.memoryLimit != 70<<30 || cfg.memoryBudget != 100<<30 {
		t.Fatal("ReadConfig lost budget or explicit zero")
	}
	old := NewConfig()
	if !storageSettingsChanged(old, cfg) {
		t.Fatal("budget change must require restart")
	}
	next := *cfg
	next.memoryLimit++
	if !storageSettingsChanged(cfg, &next) {
		t.Fatal("resolved limit change must require restart")
	}
	if storageSettingsChanged(cfg, cfg) {
		t.Fatal("unchanged budget requires restart")
	}
}

func TestMemoryBudgetRuntime(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	previous := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(previous)
	t.Setenv("GOMEMLIMIT", "")
	cfg := NewConfig()
	cfg.applyMemoryBudget()
	if debug.SetMemoryLimit(-1) != previous {
		t.Fatal("disabled budget changed runtime")
	}
	path, cfg := sharedAppConfig(t)
	cfg.Common.MemoryBudget = "1073741824"
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	if got := debug.SetMemoryLimit(-1); got != 751619276 {
		t.Fatalf("startup limit %d, want 70%% of 1 GiB", got)
	}
	cfg.Common.MemoryBudget = "2147483648"
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("live budget change accepted")
	}
	if debug.SetMemoryLimit(-1) != 751619276 {
		t.Fatal("rejected reload changed runtime limit")
	}
}

func TestMemoryBudgetDetectionFailure(t *testing.T) {
	cfg := NewConfig()
	cfg.Common.MemoryBudget = "50%"
	if err := cfg.resolveMemoryBudget(toml.MetaData{}, func() (uint64, error) { return 0, fmt.Errorf("unreadable limit") }, ""); err == nil {
		t.Fatal("used a host default after detection failed")
	}
}

func BenchmarkReadMemoryBudgetConfig(b *testing.B) {
	for _, budget := range []string{"", "107374182400", "80%"} {
		b.Run(fmt.Sprintf("budget=%s", budget), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "config")
			if err := os.WriteFile(path, []byte(fmt.Sprintf("[common]\nmemory-budget=%q\n", budget)), 0600); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := ReadConfig(path); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
