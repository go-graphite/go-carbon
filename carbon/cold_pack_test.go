package carbon

import (
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"testing"
)

func TestColdPackConfig(t *testing.T) {
	cfg := NewConfig()
	if o, err := store.NormalizeColdPack(coldPackOptions(cfg)); err != nil || o.Mode != "off" {
		t.Fatalf("default %+v %v", o, err)
	}
	cfg.Whisper.StoreColdPack = "decimal"
	if err := validateStorageConfig(cfg); err == nil {
		t.Fatal("file backend accepted cold pack")
	}
	cfg.Whisper.StoreColdPack = "off"
	next := *cfg
	next.Whisper.StoreColdPack = "exact"
	next.Whisper.StoreColdPackRate = 12345
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
