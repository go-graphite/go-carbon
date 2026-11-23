package downsize

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/ingestreplay"
)

type syncIngestOptions struct {
	addr       string
	holes      map[string][]syncHole
	windows    syncRanges
	windowOnly bool
	now        func() time.Time
	measure    string
}

type syncIngestBackend interface {
	Fill(context.Context, string, string, []string, string, syncIngestOptions) (syncCopyResult, error)
}

// Only the last archive has no propagation side effects. Ingest routes a past
// point to the first retention covering its age, not necessarily archive zero.
// Leave a two-minute boundary margin for clocks and asynchronous persistence.
func ingestFillBounds(source, name string, archives []syncArchive, holes []syncHole, now int64) (int64, int64, bool) {
	if !ingestreplay.ValidName(name) || len(archives) == 0 {
		return 0, 0, false
	}
	last := archives[len(archives)-1]
	if last.Step <= 0 || last.Points <= 0 || last.Step > math.MaxUint32/last.Points {
		return 0, 0, false
	}
	first, until := int64(math.MaxInt64), int64(0)
	for _, h := range holes {
		if h.Source != source {
			continue
		}
		if h.Step != last.Step || h.First <= 0 || h.Last < h.First || h.Last >= now-120 {
			return 0, 0, false
		}
		first = min(first, h.First)
		until = max(until, h.Last+1)
	}
	if until == 0 || now-first+120 >= last.Step*last.Points {
		return 0, 0, false
	}
	if len(archives) > 1 {
		prev := archives[len(archives)-2]
		if prev.Step <= 0 || prev.Points <= 0 || prev.Step > math.MaxUint32/prev.Points || now-(until-1) <= prev.Step*prev.Points+120 {
			return 0, 0, false
		}
	}
	return first, until, true
}

func ingestHoleContains(holes []syncHole, source string, ts int64) bool {
	for _, h := range holes {
		if h.Source == source && ts >= h.First && ts <= h.Last {
			return true
		}
	}
	return false
}

// Fill reuses ordinary copy receipts, including receipts written before ingest
// fill existed. A done range is fsynced only after every replayed point is visible
// with identical bits through the target reader. Interrupted sends are compared
// afresh on resume. Unsupported names retain the existing transfer engine.
func (b *syncLiveBackend) Fill(ctx context.Context, source, target string, names []string, path string, o syncIngestOptions) (syncCopyResult, error) {
	transfer := func(names []string, path string) (syncCopyResult, error) {
		if o.windowOnly {
			return b.CopyWindow(ctx, source, target, names, path, o.windows)
		}
		return b.Copy(ctx, source, target, names, path)
	}
	if b.health == nil || b.o.noCopy {
		return transfer(names, path)
	}
	if o.addr == "" {
		o.addr = "127.0.0.1:2003"
	}
	if o.now == nil {
		o.now = time.Now
	}
	// Native window intents bind ordinals to the original input. Let their
	// existing recovery protocol complete an already-entered window transaction.
	if o.windowOnly {
		// Freeze fallback membership before its first native window intent. A
		// resumed replay must not change the ordinals of an in-flight transfer.
		members, err := os.ReadFile(path + ".transfer.members")
		if err == nil {
			remaining := strings.Split(strings.TrimSuffix(string(members), "\n"), "\n")
			res, err := transfer(remaining, path+".transfer")
			res.Copied += len(names) - len(remaining)
			return res, err
		}
		if !errors.Is(err, os.ErrNotExist) {
			return syncCopyResult{}, err
		}
		old, err := filepath.Glob(path + ".window-*")
		if err != nil {
			return syncCopyResult{}, err
		}
		if len(old) > 0 {
			return transfer(names, path)
		}
	}
	input, err := syncCopyInput(path, names)
	if err != nil {
		return syncCopyResult{}, err
	}
	identity, err := copyIdentity(b.copyOptions(source, target, input, path, len(names)))
	if err != nil {
		return syncCopyResult{}, err
	}
	receiptPath := path
	if o.windowOnly {
		receiptPath += ".ingest"
	}
	j, err := openCopyJournal(receiptPath, identity)
	if err != nil {
		return syncCopyResult{}, err
	}
	// cover is a sorted cursor: consume it before dispatching parallel jobs.
	covered := j.cover(source)
	type job struct {
		names []string
		start int
	}
	jobs := make(chan job)
	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	var needsTransfer atomic.Bool
	var copied, resumed, points atomic.Int64
	for range max(1, b.o.copyStreams) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				if workCtx.Err() != nil {
					continue
				}
				batchOptions := o
				if o.windowOnly {
					batchOptions.measure = fmt.Sprintf("%s.ingest-%d.window-points.json", path, batch.start)
				}
				good, n, err := b.ingestFillBatch(workCtx, source, target, batch.names, batchOptions)
				if err != nil {
					cancel(err)
					continue
				}
				points.Add(n)
				var rows []copyRow
				// A range may include only successful members; fallback names
				// between two replayed names must never receive a false receipt.
				for i := 0; i < len(batch.names); {
					if !good[batch.names[i]] {
						needsTransfer.Store(true)
						i++
						continue
					}
					start := i
					for i < len(batch.names) && good[batch.names[i]] {
						i++
					}
					rows = append(rows, copyRow{Kind: "done", Source: source, First: batch.names[start], Last: batch.names[i-1], Names: i - start})
				}
				if len(rows) > 0 {
					if err := j.appendRows(rows); err != nil {
						cancel(err)
						continue
					}
					count := len(good)
					copied.Add(int64(count))
					if report, ok := ctx.Value(syncCopyProgressKey{}).(func(int)); ok {
						report(count)
					}
				}
			}
		}()
	}
	started := time.Now()
feed:
	for start := 0; start < len(names); start += 64 {
		var pending []string
		for _, n := range names[start:min(start+64, len(names))] {
			if covered(n) {
				resumed.Add(1)
			} else {
				pending = append(pending, n)
			}
		}
		if len(pending) == 0 {
			continue
		}
		select {
		case jobs <- job{pending, start}:
		case <-workCtx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	j.close()
	event("sync_fill_ingest", map[string]any{"source": source, "names": copied.Load(), "resumed_names": resumed.Load(), "points": points.Load(), "seconds": time.Since(started).Seconds(), "transfer_remaining": needsTransfer.Load()})
	if err := context.Cause(workCtx); err != nil {
		return syncCopyResult{}, err
	}
	if !needsTransfer.Load() {
		return syncCopyResult{Copied: len(names)}, nil
	}
	if !o.windowOnly {
		return transfer(names, path)
	} // shared receipts skip replayed names
	remaining, err := copyJournalFailures(receiptPath, source, names)
	if err != nil {
		return syncCopyResult{}, err
	}
	// Keep deterministic original membership for the native window fallback.
	child := path + ".transfer"
	if err := atomicSyncConfigFile(child+".members", syncNamesBytes(remaining)); err != nil {
		return syncCopyResult{}, err
	}
	res, err := transfer(remaining, child)
	res.Copied += int(copied.Load() + resumed.Load())
	return res, err
}

func (b *syncLiveBackend) ingestFillBatch(ctx context.Context, source, target string, names []string, o syncIngestOptions) (map[string]bool, int64, error) {
	good := map[string]bool{}
	// Multiple hole steps already prove that this name cannot be repaired in
	// the last archive alone. Avoid metadata RPCs for an all-transfer campaign.
	var candidates []string
	for _, name := range names {
		step, mixed := int64(0), false
		for _, h := range o.holes[name] {
			if h.Source != source {
				continue
			}
			if h.Step <= 0 || step != 0 && step != h.Step {
				mixed = true
				break
			}
			step = h.Step
		}
		if step > 0 && !mixed && ingestreplay.ValidName(name) {
			candidates = append(candidates, name)
		}
	}
	if len(candidates) == 0 {
		return good, 0, nil
	}
	info, err := b.Info(ctx, target, candidates)
	if err != nil {
		return nil, 0, err
	}
	type bounds struct{ from, until, step int64 }
	groups := map[int64][]string{}
	limits := map[string]bounds{}
	for _, name := range candidates {
		from, until, ok := ingestFillBounds(source, name, info[name], o.holes[name], o.now().Unix())
		if !ok {
			continue
		}
		step := info[name][len(info[name])-1].Step
		limits[name] = bounds{from, until, step}
		groups[step] = append(groups[step], name)
		good[name] = true
	}
	var conn net.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	var sent int64
	var measurement *windowPointCount
	var previous int64
	for step, group := range groups {
		first, last := int64(math.MaxInt64), int64(0)
		for _, name := range group {
			first = min(first, limits[name].from)
			last = max(last, limits[name].until)
		}
		// Bound materialized response memory per worker, including long histories.
		for from := first; from < last; {
			until := min(last, from+4096*step)
			var src, dst map[string]syncSeries
			var sourceErr, targetErr error
			var reads sync.WaitGroup
			reads.Add(2)
			go func() { defer reads.Done(); src, sourceErr = b.Fetch(ctx, source, group, from-1, until) }()
			go func() { defer reads.Done(); dst, targetErr = b.Fetch(ctx, target, group, from-1, until) }()
			reads.Wait()
			if err := errors.Join(sourceErr, targetErr); err != nil {
				return nil, sent, err
			}
			pending := map[string][]ingestreplay.Point{}
			for _, name := range group {
				if !good[name] {
					continue
				}
				if src[name].Step != step || dst[name].Step != step || src[name].Start%step != 0 || dst[name].Start%step != 0 {
					delete(good, name)
					continue
				}
				for _, p := range ingestreplay.Missing(ingestreplay.Series(src[name]), ingestreplay.Series(dst[name]), max(from, limits[name].from), min(until, limits[name].until)) {
					inside := !o.windowOnly
					for _, w := range o.windows {
						inside = inside || p.Timestamp >= w.From && p.Timestamp < w.Until
					}
					if inside && ingestHoleContains(o.holes[name], source, p.Timestamp) {
						pending[name] = append(pending[name], p)
					}
				}
			}
			var buf []byte
			flush := func() error {
				if len(buf) == 0 {
					return nil
				}
				if err := b.health.waitIngest(ctx); err != nil {
					return err
				}
				if o.measure != "" && measurement == nil {
					zero := int64(0)
					count, err := readWindowPointCount(o.measure, windowPointCount{Source: source, Before: &zero})
					if err != nil {
						return err
					}
					if count.After != nil {
						previous = *count.After
					}
					// A crash after admission leaves an explicitly uncertain count.
					count.After = nil
					if err := atomicStateJSON(o.measure, count); err != nil {
						return err
					}
					measurement = &count
				}
				for name := range pending {
					if _, _, ok := ingestFillBounds(source, name, info[name], o.holes[name], o.now().Unix()); !ok {
						return fmt.Errorf("ingest retention boundary moved for %s; resume to use transfer", name)
					}
				}
				if conn == nil {
					var err error
					conn, err = (&net.Dialer{Timeout: 30 * time.Second}).DialContext(ctx, "tcp", o.addr)
					if err != nil {
						return err
					}
				}
				stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
				err := ingestreplay.Send(conn, buf, 30*time.Second)
				if err == nil {
					err = b.o.rate.wait(ctx, int64(len(buf)))
				}
				stop()
				buf = buf[:0]
				return err
			}
			buffered := 0
			for name, pts := range pending {
				for _, p := range pts {
					buf = ingestreplay.AppendPoint(buf, name, p.Value, p.Timestamp)
					buffered++
					sent++
					if buffered == 8192 {
						if err := flush(); err != nil {
							return nil, sent, err
						}
						buffered = 0
					}
				}
			}
			if err := flush(); err != nil {
				return nil, sent, err
			}
			if len(pending) > 0 {
				accepted, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := b.confirmIngest(accepted, target, pending, from-1, until)
				cancel()
				if err != nil {
					return nil, sent, err
				}
			}
			from = until
		}
	}
	if measurement != nil {
		after := previous + sent
		measurement.After = &after
		if err := atomicStateJSON(o.measure, measurement); err != nil {
			return nil, sent, err
		}
	}
	return good, sent, nil
}

func (b *syncLiveBackend) confirmIngest(ctx context.Context, target string, pending map[string][]ingestreplay.Point, from, until int64) error {
	names := make([]string, 0, len(pending))
	for name := range pending {
		names = append(names, name)
	}
	for {
		got, err := b.Fetch(ctx, target, names, from, until)
		if err != nil {
			return err
		}
		complete := true
		for name, pts := range pending {
			for _, p := range pts {
				v, ok := ingestreplay.Series(got[name]).At(p.Timestamp)
				if !ok || math.Float64bits(v) != math.Float64bits(p.Value) {
					complete = false
					break
				}
			}
		}
		if complete {
			return b.health.waitIngest(ctx)
		}
		if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
			return fmt.Errorf("ingest read-back before receipt: %w", err)
		}
	}
}
