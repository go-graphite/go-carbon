package downsize

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// The same NativeSource/NativeImport methods used by Buckyd, connected by a
// streaming pipe. The entire export/import/publish is timed, with no wire buffer.
func measureNative(ctx context.Context, source, target *store.Store, names []string, batch, workers int, combine ...bool) (result, []string, error) {
	exactCombine := len(combine) > 0 && combine[0]
	began := time.Now()
	pin, err := source.PinNativeSource()
	if err != nil {
		return result{}, nil, err
	}
	defer pin.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wire, importNanos, exportNanos, copied atomic.Int64
	var mu sync.Mutex
	var first error
	var deferred []string
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for start := range jobs {
				end := min(start+batch, len(names))
				group := names[start:end]
				request := store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "fixture", Start: int64(start), End: int64(end), Names: group, Combine: exactCombine}
				candidates, e := target.NativeCandidates(ctx, group)
				if e != nil {
					mu.Lock()
					if first == nil {
						first = e
						cancel()
					}
					mu.Unlock()
					continue
				}
				request.Candidates = candidates
				if exactCombine {
					for i := range group {
						candidates[i/8] |= 1 << uint(i%8)
					}
				}
				var selected []string
				for i, name := range group {
					if candidates[i/8]&(1<<uint(i%8)) != 0 {
						selected = append(selected, name)
					}
				}
				read, write := io.Pipe()
				exportDone := make(chan error, 1)
				go func() {
					begin := time.Now()
					e := pin.Write(ctx, selected, write)
					exportNanos.Add(time.Since(begin).Nanoseconds())
					_ = write.CloseWithError(e)
					exportDone <- e
				}()
				begin := time.Now()
				receipt, e := target.NativeImport(ctx, request, read)
				importNanos.Add(time.Since(begin).Nanoseconds())
				_ = read.CloseWithError(e)
				exportErr := <-exportDone
				if e == nil {
					e = exportErr
				}
				mu.Lock()
				if e != nil {
					if first == nil {
						first = e
						cancel()
					}
				} else {
					wire.Add(receipt.Bytes)
					copied.Add(int64(receipt.CopiedCount))
					for i, name := range group {
						if !receipt.IsCopied(i) {
							deferred = append(deferred, name)
						}
					}
				}
				mu.Unlock()
			}
		}()
	}
enqueue:
	for start := 0; start < len(names); start += batch {
		select {
		case jobs <- start:
		case <-ctx.Done():
			break enqueue
		}
	}
	close(jobs)
	wg.Wait()
	seconds := time.Since(began).Seconds()
	return result{Mode: "native", Metrics: copied.Load(), WireBytes: wire.Load(), Seconds: seconds, ExportSeconds: float64(exportNanos.Load()) / 1e9, ImportSeconds: float64(importNanos.Load()) / 1e9, NamesPerSecond: float64(copied.Load()) / seconds, MBPerSecond: float64(wire.Load()) / seconds / 1e6}, deferred, first
}
