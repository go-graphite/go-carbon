package persister

import (
	"context"
	"iter"
	"slices"
	"sync"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type writeoutWarmer interface {
	MetadataCached(string) bool
	PrewarmWriteout(context.Context, []string)
}

// checkpointValidator promotes provisional legacy checkpoint records for the
// offered names using one ID-sorted revision iterator per batch.
type checkpointValidator interface {
	ValidateMetadataCheckpoint(context.Context, iter.Seq[string], store.PrewarmOptions) (store.MetadataCheckpointStats, error)
}

// SetWarmFirst enables bounded writeout-order prefetch. Set before Start.
func (p *Whisper) SetWarmFirst(enabled bool) { p.warmFirst = enabled }

func (p *Whisper) startWarmFirst() {
	// Restore the original callbacks only after the preceding Stop joined all
	// workers. Mutating callbacks inside Stop's pre-join hook would race readers.
	if p.warmOriginalRecv != nil {
		p.recv, p.tryRecv = p.warmOriginalRecv, p.warmOriginalTryRecv
		p.warmOriginalRecv, p.warmOriginalTryRecv = nil, nil
	}
	p.warmPending = nil
	warmer, ok := p.metricStore.(writeoutWarmer)
	if !p.warmFirst || !ok {
		return
	}
	validator, _ := p.metricStore.(checkpointValidator)
	p.warmPending = &warmReservations{names: make(map[string]struct{})}
	recv, tryRecv := p.recv, p.tryRecv
	p.warmOriginalRecv, p.warmOriginalTryRecv = recv, tryRecv
	ready := make(chan string, store.DefaultPrewarmBatch)
	raw := make(chan string, store.DefaultPrewarmBatch)
	p.recv = func(exit chan bool) string {
		// Prefer prefetched names, but an empty prefetch window must not
		// park the writer pool. Raw names use ordinary CPU-admitted reads.
		select {
		case name := <-ready:
			return name
		default:
		}
		select {
		case name := <-ready:
			return name
		case name := <-raw:
			return name
		case <-exit:
			return ""
		}
	}
	p.tryRecv = func() string {
		select {
		case name := <-ready:
			return name
		default:
		}
		select {
		case name := <-raw:
			return name
		default:
			return ""
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	// Keep fetching independent of prefetch I/O. One bounded source pump
	// also drives queue rebuilds while the prefetcher prepares a cold batch.
	p.Go(func(exit chan bool) {
		for {
			name := recv(exit)
			if name == "" {
				return
			}
			if !p.claimWarmName(name) {
				continue
			}
			select {
			case raw <- name:
			case <-exit:
				return
			}
		}
	})
	p.Go(func(exit chan bool) {
		select {
		case <-exit:
			cancel()
		case <-finished:
		}
	})
	p.Go(func(exit chan bool) {
		defer close(finished)
		defer cancel()
		send := func(name string) bool {
			select {
			case ready <- name:
				return true
			case <-exit:
				return false
			}
		}
		cold := make([]string, 0, store.DefaultPrewarmBatch)
		for {
			var first string
			select {
			case first = <-raw:
			case <-exit:
				return
			}
			cold = cold[:0]
			name := first
			for i := 0; i < store.DefaultPrewarmBatch && name != ""; i++ {
				if warmer.MetadataCached(name) {
					if !send(name) {
						return
					}
				} else {
					cold = append(cold, name)
				}
				if i+1 == store.DefaultPrewarmBatch {
					break
				}
				select {
				case name = <-raw:
				default:
					name = ""
				}
			}

			if len(cold) == 0 {
				continue
			}
			// Offer provisional checkpoint names before writeout admission and
			// before any writeout/metric lock. Authoritative, absent and pending
			// names need no work. Admission uses the checkpoint class: it shares
			// process credit but cannot starve behind writers or a below-floor
			// drain, which this stalled window would itself cause.
			if validator != nil {
				options := store.PrewarmOptions{Batch: store.DefaultPrewarmBatch}
				if p.restartBudget != nil {
					options.Wait = p.restartBudget.WaitCheckpoint
				}
				if _, err := validator.ValidateMetadataCheckpoint(ctx, slices.Values(cold), options); err != nil && ctx.Err() != nil {
					return
				}
			}
			// This batch is needed by the oldest queued cold writes. It must not
			// compete as optional catalog work or wait while holding store locks.
			if p.restartBudget != nil {
				if p.restartPaced() {
					if !p.restartBudget.WaitPrefetch(exit) {
						return
					}
				} else {
					p.restartBudget.BeginPrefetch()
				}
			}
			warmer.PrewarmWriteout(ctx, cold)
			if p.restartBudget != nil {
				p.restartBudget.EndPrefetch()
			}
			for _, name := range cold {
				if !send(name) {
					return
				}
			}
		}
	})
}

// Reservations are bounded by worker batch slots, the raw/ready/prefetch
// windows and one source-pump name. They prevent refreshed snapshots from
// spending rate/CPU grants on names already in the pipeline.
type warmReservations struct {
	mu    sync.Mutex
	names map[string]struct{}
}

func (p *Whisper) claimWarmName(name string) bool {
	pending := p.warmPending
	pending.mu.Lock()
	defer pending.mu.Unlock()
	if _, exists := pending.names[name]; exists {
		return false
	}
	pending.names[name] = struct{}{}
	return true
}
func (p *Whisper) finishWarmName(name string) {
	if pending := p.warmPending; pending != nil {
		pending.mu.Lock()
		delete(pending.names, name)
		pending.mu.Unlock()
	}
}
