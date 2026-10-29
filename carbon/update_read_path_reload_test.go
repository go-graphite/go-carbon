package carbon

import "testing"

// store-update-point-lookups reloads on SIGHUP into the open store, without
// the restart other storage settings need.
func TestUpdatePointLookupsReload(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	path, cfg := sharedAppConfig(t)
	cfg.Whisper.Enabled = false
	cfg.Buckyd.Enabled, cfg.Carbonserver.Enabled = false, false
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	db := app.MetricStore
	if db == nil || db.Stats().UpdatePointLookups {
		t.Fatal("shared store missing or not on the shared update iterator by default")
	}
	for _, lookups := range []bool{true, false, true} {
		cfg.Whisper.StoreUpdatePointLookups = lookups
		writeSharedConfig(t, path, cfg)
		if err := app.ReloadConfig(); err != nil {
			t.Fatal(err)
		}
		if app.MetricStore != db || db.Stats().UpdatePointLookups != lookups {
			t.Fatalf("store-update-point-lookups = %v not applied to the open store", lookups)
		}
	}
}
