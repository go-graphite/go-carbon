package chunkstore

import (
	"context"
	"errors"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
)

type nativePreparedRecord struct {
	index    int
	prepared nativeCombinePrepared
	keys     []RelayoutKV
	err      error
	release  chan struct{}
}

// Prepare independent names concurrently against the same immutable view.
// Each worker owns its cursors and arena, and keeps at most one completed name
// until the ordered SST writer consumes it. No work or sync runs under a name
// stripe; publication still performs the original revision check afterward.
func (s *Store) prepareNativeCombineRecords(ctx context.Context, view *pebble.Snapshot, intent *nativeCombineIntent, first uint64, pending []int, consume func(nativeCombinePrepared, []RelayoutKV) error) error {
	workers := min(len(pending), runtime.GOMAXPROCS(0), s.nativeCombineWorkers)
	if workers == 0 {
		return nil
	}
	workctx, cancel := context.WithCancel(ctx)
	var running sync.WaitGroup
	defer func() { cancel(); running.Wait() }()
	type job struct{ index, input int }
	jobs := make(chan job)
	results := make(chan nativePreparedRecord, workers)
	running.Add(1)
	go func() {
		defer running.Done()
		defer close(jobs)
		for j, i := range pending {
			select {
			case jobs <- job{j, i}:
			case <-workctx.Done():
				return
			}
		}
	}()
	for range workers {
		running.Add(1)
		go func() {
			defer running.Done()
			sourceWalk := &transferReader{reader: view, native: true, ordered: true}
			targetWalk := &transferReader{reader: view, native: true}
			defer sourceWalk.close()
			defer targetWalk.close()
			release := make(chan struct{})
			for {
				var task job
				select {
				case <-workctx.Done():
					return
				case next, ok := <-jobs:
					if !ok {
						return
					}
					task = next
				}
				sourceWalk.arena.free = sourceWalk.arena.slab
				targetWalk.arena.free = targetWalk.arena.slab
				p, keys, err := s.prepareNativeCombineRecord(workctx, view, Metadata(intent.Raw[task.input]), intent.Positions[task.input], first+uint64(task.index), sourceWalk, targetWalk, intent.Request.Window)
				var skip *NativeSkip
				if errors.As(err, &skip) {
					skip.Ordinal = p.Position
					p.Deferred, p.Skipped, p.Old = true, skip, nativeCombineMetadata{}
					keys, err = nil, nil
				}
				result := nativePreparedRecord{task.index, p, keys, err, release}
				select {
				case results <- result:
				case <-workctx.Done():
					return
				}
				// The result may borrow this worker's arenas. Reuse them only after the
				// SST writer has consumed the values; cancellation exits without reuse.
				select {
				case <-release:
				case <-workctx.Done():
					return
				}
			}
		}()
	}
	waiting := make(map[int]nativePreparedRecord, workers)
	for next := 0; next < len(pending); {
		var result nativePreparedRecord
		select {
		case <-workctx.Done():
			return workctx.Err()
		case result = <-results:
		}
		if result.err != nil {
			return result.err
		}
		waiting[result.index] = result
		for {
			ready, ok := waiting[next]
			if !ok {
				break
			}
			if err := consume(ready.prepared, ready.keys); err != nil {
				return err
			}
			delete(waiting, next)
			next++
			select {
			case ready.release <- struct{}{}:
			case <-workctx.Done():
				return workctx.Err()
			}
		}
	}
	return nil
}

func (s *Store) prepareNativeCombineRecord(ctx context.Context, view *pebble.Snapshot, raw Metadata, position int, destination uint64, sourceWalk, targetWalk *transferReader, window *NativeWindow) (nativeCombinePrepared, []RelayoutKV, error) {
	p := nativeCombinePrepared{nativeCombinePiece: nativeCombinePiece{Position: position, Meta: nativeCombineMetadata(raw)}}
	source, err := s.nativeTransferRecord(ctx, view, raw, sourceWalk)
	if err != nil {
		return p, nil, err
	}
	if err := validateNativeMemory(source); err != nil {
		return p, nil, err
	}
	old, err := targetWalk.metadata(raw.Name)
	var keys []RelayoutKV
	if err == nil {
		if err := nativeCompatibleTarget(s, old, source); err != nil {
			return p, nil, err
		}
		target, err := s.nativeTransferRecord(ctx, view, old, targetWalk)
		if err != nil {
			return p, nil, err
		}
		if err := validateNativeMemory(target); err != nil {
			return p, nil, err
		}
		var result TransferMetric
		var class NativeCombineClass
		if window == nil {
			result, class, err = s.nativeCombineRecord(ctx, source, target)
		} else {
			result, class, err = s.nativeCombineWindowRecord(ctx, source, target, *window)
		}
		if err != nil {
			return p, nil, err
		}
		if err := validateNativeMemory(result); err != nil {
			return p, nil, err
		}
		p.Old, p.Class = nativeCombineMetadata(old), class
		p.Meta.ID = destination
		p.Meta.LastUpdate = s.nextActivity(old.LastUpdate)
		keys, source = result.Keys, result
	} else if !errors.Is(err, ErrNotFound) {
		return p, nil, err
	}
	if source.Continuation {
		kv := transferRecordIO(source)
		if value, e := kv.Get(SidecarSummaryKey); e == nil {
			p.Summary, err = DecodeSidecarSummary(value)
		} else if inspector, ok := s.continuation.(ContinuationSidecars); ok {
			p.Summary, err = inspector.InspectSidecar(kv)
		} else if !errors.Is(e, ErrNotFound) {
			err = e
		}
		if err != nil {
			return p, nil, err
		}
	}
	return p, keys, nil
}

func (s *Store) nativeTransferRecord(ctx context.Context, view *pebble.Snapshot, m Metadata, walk *transferReader) (TransferMetric, error) {
	n := nativeSize(m.MetricConfig)
	measure := &transferReader{reader: view, native: true}
	defer measure.close()
	if err := s.visitTransferKeys(ctx, view, m, n.add, measure); err != nil {
		return TransferMetric{}, err
	}
	if n.exceedsLimit() {
		n.Reason = "name limit"
		return TransferMetric{}, &n
	}
	return s.transferFromLimit(ctx, view, m, nativeMetricBytes, walk)
}
