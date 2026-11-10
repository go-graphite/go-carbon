package carbon

import (
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/hostcapacity"
	"github.com/go-graphite/go-carbon/persister"
)

// Caller holds App's lifecycle lock. Captured store/read listener identities
// remain valid until the owning persister is joined; no callback takes App's
// lock or scrapes Prometheus. Sampling/slow filesystem calls run outside the
// persister's autoUpdate mutex, including during an in-place reload.
func (app *App) configureAutoCapacity(p *persister.Whisper) {
	db, reads := app.MetricStore, &app.capacityReadLatency
	directory := app.Config.Whisper.DataDir
	if db != nil {
		directory = sharedStorePath(app.Config)
	}
	var mu sync.Mutex
	var host hostcapacity.Sampler
	p.SetAutoCapacity(func() persister.AutoCapacitySample {
		mu.Lock()
		defer mu.Unlock()
		sample := persister.AutoCapacitySample{Host: host.Sample(time.Now())}
		free, size, err := hostcapacity.DiskSpace(directory)
		sample.FreeBytes, sample.DiskBytes, sample.SpaceValid = free, size, err == nil
		if db != nil {
			s := db.Stats()
			sample.Stalled, sample.Stalls, sample.Debt, sample.L0, sample.L0Stop = s.WriteStallActive, s.WriteStalls, s.CompactionDebt, s.L0Sublevels, db.TransferL0StopWrites()
		}
		sample.Reads = reads.Snapshot()
		return sample
	})
}
