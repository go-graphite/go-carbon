package persister

import (
	"time"

	whisper "github.com/go-graphite/go-whisper"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/helper"
)

// whisperSettings is immutable after publication. Reload never waits for a
// store operation: existing operations may finish using the preceding settings.
type whisperSettings struct {
	schemas                                                 WhisperSchemas
	aggregation                                             *WhisperAggregation
	maxUpdatesPerSecond, storeUpdateBatchSize               int
	sparse, flock, compressed, removeEmptyFile, tagsEnabled bool
	taggedFn                                                func(string, bool)
	updateTicker                                            *helper.ThrottleTicker
	onlineMigration                                         migrationSettings
	outOfOrder                                              outOfOrderSettings
}

type migrationSettings struct {
	enabled                        bool
	rate                           int
	ticker                         *helper.ThrottleTicker
	schema, xff, aggregationMethod bool
}

type outOfOrderSettings struct {
	enabled   bool
	rate      int
	threshold int64
	ticker    *helper.ThrottleTicker
	policy    whisper.OutOfOrderMergeOptions
}

func (p *Whisper) settings() *whisperSettings {
	if settings := p.liveSettings.Load(); settings != nil {
		return settings
	}
	// Original fields are configured only before Start. Keeping this fallback
	// also allows offline file/migration helpers to use an unstarted persister.
	return &whisperSettings{
		schemas: p.schemas, aggregation: p.aggregation,
		maxUpdatesPerSecond: p.maxUpdatesPerSecond, storeUpdateBatchSize: p.storeUpdateBatchSize,
		sparse: p.sparse, flock: p.flock, compressed: p.compressed,
		removeEmptyFile: p.removeEmptyFile, tagsEnabled: p.tagsEnabled, taggedFn: p.taggedFn,
		updateTicker: p.updateTicker,
		onlineMigration: migrationSettings{p.onlineMigration.enabled, p.onlineMigration.rate,
			p.onlineMigration.ticker, p.onlineMigration.schema, p.onlineMigration.xff, p.onlineMigration.aggregationMethod},
		outOfOrder: outOfOrderSettings{p.outOfOrder.enabled, p.outOfOrder.rate,
			p.outOfOrder.threshold, p.outOfOrder.ticker, p.outOfOrder.policy},
	}
}

// ReloadSettings applies a configured, unstarted persister's reloadable settings
// to this running persister. Queue readers, workers and CPU admission stay live.
// Lifecycle calls must be serialized by the owner.
func (p *Whisper) ReloadSettings(next *Whisper) {
	p.reloadMu.Lock()
	defer p.reloadMu.Unlock()
	old, settings := p.settings(), next.settings()
	settings.updateTicker = old.updateTicker
	if settings.maxUpdatesPerSecond != old.maxUpdatesPerSecond {
		settings.updateTicker = helper.NewThrottleTicker(max(0, settings.maxUpdatesPerSecond))
	}
	settings.onlineMigration.ticker = old.onlineMigration.ticker
	if settings.onlineMigration.enabled && (!old.onlineMigration.enabled || old.onlineMigration.ticker == nil || settings.onlineMigration.rate != old.onlineMigration.rate) {
		settings.onlineMigration.ticker = helper.NewHardThrottleTicker(settings.onlineMigration.rate)
	}
	settings.outOfOrder.ticker = old.outOfOrder.ticker
	if settings.outOfOrder.enabled && (!old.outOfOrder.enabled || old.outOfOrder.ticker == nil || settings.outOfOrder.rate != old.outOfOrder.rate) {
		settings.outOfOrder.ticker = helper.NewHardThrottleTicker(settings.outOfOrder.rate)
	}
	a := &p.autoUpdate
	var state cache.WriteoutState
	var capacity AutoCapacitySample
	if settings.maxUpdatesPerSecond == -1 {
		// Scan before locking: readers of autoUpdate.mu must not wait for it.
		state = p.autoUpdateState(next.autoUpdate.state)
		if next.autoUpdate.ceiling == -1 {
			capacity = p.capacityState(next.autoUpdate.capacitySource)
		}
	}
	a.mu.Lock()
	previousCeiling := a.ceiling
	a.generation++
	a.state, a.floor, a.ceiling = next.autoUpdate.state, next.autoUpdate.floor, next.autoUpdate.ceiling
	if a.ceiling == -1 && (previousCeiling != -1 || old.maxUpdatesPerSecond != -1) {
		a.capacity.at = time.Time{}
		a.capacity.limit = 0
		seed := a.applied
		if old.maxUpdatesPerSecond != -1 {
			seed = max(0, old.maxUpdatesPerSecond)
		}
		if seed > 0 {
			a.capacity.limit = max(a.floor, seed)
		}
	}
	a.capacitySource, a.capacitySample = next.autoUpdate.capacitySource, capacity
	a.observeTokens.Store(a.ceiling == -1)
	// Publish rate mode and its token source together. The automatic sampler
	// runs in every mode, so a fixed -> automatic switch needs no new worker.
	if settings.maxUpdatesPerSecond == -1 {
		if old.maxUpdatesPerSecond != -1 {
			a.applied = 0
		}
		p.sampleAutoUpdateRateLocked(state)
	}
	p.liveSettings.Store(settings)
	a.mu.Unlock()
	if p.restartBudget != nil && p.restartBudget.Active() {
		p.setRestartRate()
	} else {
		p.stopRestartTicker()
	}
	if settings.maxUpdatesPerSecond != -1 {
		// Retain the closed channel for readers that saw the old automatic mode.
		// Returning nil here could park them until the next process shutdown.
		if ticker := a.ticker.Load(); ticker != nil {
			ticker.Stop()
		}
	}
	if old.updateTicker != settings.updateTicker {
		old.updateTicker.Stop()
	}
	if old.onlineMigration.ticker != nil && (old.onlineMigration.ticker != settings.onlineMigration.ticker || !settings.onlineMigration.enabled) {
		old.onlineMigration.ticker.Stop()
	}
	if old.outOfOrder.ticker != nil && (old.outOfOrder.ticker != settings.outOfOrder.ticker || !settings.outOfOrder.enabled) {
		old.outOfOrder.ticker.Stop()
	}
}

// InheritRuntime continues the same process startup after a structural rebuild.
// previous must have been joined with StopForReload. The store's metadata cache,
// cache restart scheduling and shared CPU budget belong to the application.
func (p *Whisper) InheritRuntime(previous *Whisper) {
	p.restartBudget, p.restartFloor = previous.restartBudget, previous.restartFloor
	p.restartBacklog = previous.restartBacklog
	p.warmFirst = previous.warmFirst && (p.restartBudget == nil || p.restartBudget.Active())
	p.startupReady, p.startupDone = previous.startupReady, previous.startupDone
	p.startupWait, p.startupFull = previous.startupWait, previous.startupFull
	p.startupProgress, p.startupCheckpointLoading = previous.startupProgress, previous.startupCheckpointLoading
	p.startupReleased, p.startupGrace = previous.startupReleased, previous.startupGrace
	p.startupReleases = previous.startupReleases
	if previous.startupGate != nil {
		select {
		case <-previous.startupGate:
			p.startupWait = 0
		default:
			if started := previous.startupStarted.Load(); started != 0 {
				p.startupWait = max(time.Nanosecond, p.startupWait-time.Since(time.Unix(0, started)))
			}
		}
	}
	p.incrementalComplete.Store(previous.incrementalComplete.Load())
	previous.autoUpdate.mu.Lock()
	p.autoUpdate.controller = previous.autoUpdate.controller
	p.autoUpdate.capacity = previous.autoUpdate.capacity
	p.autoUpdate.rateLog, p.autoUpdate.capacityLog = previous.autoUpdate.rateLog, previous.autoUpdate.capacityLog
	if p.autoUpdate.ceiling == -1 && (previous.autoUpdate.ceiling != -1 || previous.settings().maxUpdatesPerSecond != -1) {
		seed := previous.autoUpdate.applied
		if mode := previous.settings().maxUpdatesPerSecond; mode != -1 {
			seed = max(0, mode)
		}
		p.autoUpdate.capacity.limit = seed
		p.autoUpdate.capacity.at = time.Time{}
	}
	previous.autoUpdate.mu.Unlock()
}

// StopForReload joins workers without clearing the process's restart allowance.
func (p *Whisper) StopForReload() { p.stop(true) }
