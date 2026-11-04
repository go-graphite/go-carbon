package carbon

import "reflect"

// Only worker topology, file identity, tag queue ownership and migration wiring
// require joining writers. The cache, metadata and restart budget stay live.
func (app *App) reloadPersister(previous *Config) error {
	a, b := previous.Whisper, app.Config.Whisper
	tagsChanged := !reflect.DeepEqual(previous.Tags, app.Config.Tags)
	rebuild := a.Workers != b.Workers || a.DataDir != b.DataDir || a.HashFilenames != b.HashFilenames ||
		a.StoreIncrementalMigration != b.StoreIncrementalMigration || a.StoreMigrationArchiveDir != b.StoreMigrationArchiveDir ||
		a.StoreMirrorFiles != b.StoreMirrorFiles || a.StoreLiveMigration != b.StoreLiveMigration || tagsChanged
	old := app.Persister
	if old != nil && (rebuild || !b.Enabled) {
		old.StopForReload()
	}
	if tagsChanged {
		// A queue's local database cannot be reopened while its preceding owner
		// is live. Join callbacks before closing it, including in-flight writes.
		if app.Tags != nil {
			app.Tags.Stop()
			app.Tags = nil
		}
		app.startTags()
	}
	if !b.Enabled {
		app.Persister = nil
		return nil
	}
	if old == nil {
		return app.startPersister()
	}
	next, err := app.newPersister()
	if err != nil {
		return err
	}
	if !rebuild {
		old.ReloadSettings(next)
		app.configurePersisterIO(old)
		return nil
	}
	next.InheritRuntime(old)
	app.configurePersisterIO(next)
	if app.Config.Prometheus.Enabled {
		next.InitPrometheus(app.PromRegisterer)
	}
	if err := next.Start(); err != nil {
		next.Stop()
		return err
	}
	app.Persister = next
	return nil
}
