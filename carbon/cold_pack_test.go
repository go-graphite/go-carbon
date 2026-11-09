package carbon

import (
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestColdPackConfig(t *testing.T) {
	cfg := NewConfig()
	if o, err := store.NormalizeColdPack(coldPackOptions(cfg)); err != nil || o.Mode != "off" || o.PackTails || o.FloatCodec || o.FloatExhaustive || o.Workers != 1 || o.MaxCompactionDebt != 64<<20 {
		t.Fatalf("default %+v %v", o, err)
	}
	cfg.Whisper.StoreColdPackTails = true
	cfg.Whisper.StoreColdPackWorkers = 8
	cfg.Whisper.StoreColdPackMaxDebt = 8 << 30
	if o, err := store.NormalizeColdPack(coldPackOptions(cfg)); err != nil || o.MaxCompactionDebt != 8<<30 || o.Workers != 8 || !o.PackTails {
		t.Fatalf("max compaction debt %+v %v", o, err)
	}
	cfg.Whisper.StoreColdPack = "decimal"
	if err := validateStorageConfig(cfg); err == nil {
		t.Fatal("file backend accepted cold pack")
	}
	cfg.Whisper.StoreColdPack = "off"
	next := *cfg
	next.Whisper.StoreColdPack = "exact"
	next.Whisper.StoreColdPackTails = true
	next.Whisper.StoreColdPackWorkers = 2
	next.Whisper.StoreColdPackRate = 12345
	next.Whisper.StoreColdPackMaxDebt = 16 << 30
	next.Whisper.StoreColdPackFloatCodec = true
	next.Whisper.StoreColdPackFloatExhaustive = true
	if o := coldPackOptions(&next); !o.PackTails || !o.FloatCodec || !o.FloatExhaustive {
		t.Fatalf("combined options lost: %+v", o)
	}
	if storageSettingsChanged(cfg, &next) {
		t.Fatal("maintenance policy requires restart")
	}
}

func TestColdPackReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := incrementalAppFixture(t)
	cfg.Whisper.StoreTimeBuckets = true
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	db := app.MetricStore
	cfg.Whisper.StoreColdPack = "exact"
	cfg.Whisper.StoreColdPackRate = 123456
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if app.MetricStore != db || db.TransferFormat() != 9 || db.ColdPackStats().Mode != "exact" {
		t.Fatal("reload did not enable worker in place")
	}
	for _, workers := range []int{8, 2} {
		cfg.Whisper.StoreColdPackTails = workers == 8
		cfg.Whisper.StoreColdPackWorkers = workers
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if app.MetricStore != db {
			t.Fatal("workers reload replaced store")
		}
	}
	for _, mode := range []string{"float-repack", "float-downgrade"} {
		cfg.Whisper.StoreColdPack = mode
		cfg.Whisper.StoreColdPackFloatCodec = mode == "float-repack"
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if app.MetricStore != db || db.TransferFormat() != 9 || db.ColdPackStats().Mode != mode {
			t.Fatal("float policy did not reload in place without fencing")
		}
	}
	cfg.Whisper.StoreColdPack = "off"
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if db.ColdPackStats().Mode != "off" || db.TransferFormat() != 9 {
		t.Fatal("disable lost format fence")
	}
	cfg.Whisper.StoreColdPack = "invalid"
	writeSharedConfig(t, path, cfg)
	if err := app.ReloadConfig(); err == nil {
		t.Fatal("invalid policy accepted")
	}
	if db.ColdPackStats().Mode != "off" {
		t.Fatal("failed reload changed policy")
	}
}
