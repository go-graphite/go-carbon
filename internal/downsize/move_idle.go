package downsize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptrace"
	"time"

	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

// An importer pull/import/verify answers only after the target finished the
// whole batch, so "idle" means no request or response bytes moved for the
// bound. One 13 Nov transfer sat for 4 min with both sockets idle; the 10 min
// client timeout was the only bound. The pull of a plain metric task is a
// target-wins union and safe to repeat; a native child keeps its uncertain
// bulk intent and stays fail-closed.
const (
	defaultTransferIdle  = 2 * time.Minute
	transferIdleAttempts = 3
)

var errTransferIdleCause = errors.New("transfer idle timeout")

type transferIdleError struct {
	endpoint, path string
	idle           time.Duration
}

func (e *transferIdleError) Error() string {
	return fmt.Sprintf("%s %s: no request or response bytes for %s (transfer idle timeout)", e.endpoint, e.path, e.idle)
}

type idleWatch struct {
	timer *time.Timer
	idle  time.Duration
}

// watchIdle cancels the returned context with errTransferIdleCause once touch
// has not been called for idle.
func watchIdle(ctx context.Context, idle time.Duration) (context.Context, *idleWatch, func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	w := &idleWatch{idle: idle}
	w.timer = time.AfterFunc(idle, func() { cancel(errTransferIdleCause) })
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest:         func(httptrace.WroteRequestInfo) { w.touch() },
		GotFirstResponseByte: w.touch,
	})
	return ctx, w, func() { w.timer.Stop(); cancel(nil) }
}

func (w *idleWatch) touch() { w.timer.Reset(w.idle) }

type idleReader struct {
	io.Reader
	watch *idleWatch
}

func (r idleReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.watch.touch()
	}
	return n, err
}

func (m *mover) idleBounded(path string) bool {
	return m.transferIdle > 0 && m.engine != "bulk" && (path == "/v1/transfer/pull" || path == "/v1/transfer/import" || path == "/v1/transfer/verify")
}

func idleEvent(t task, path, action string, attempt int, err error) {
	event("transfer_idle_timeout", map[string]any{"level": "error", "path": path, "action": action, "attempt": attempt, "stream": t.Stream, "start": t.Start, "end": t.End, "source": t.Source, "target": t.Target, "metrics": len(t.Names), "error": err.Error()})
}

// verify repeats a read-only verification that went idle.
func (m *mover) verify(ctx context.Context, t task, proofs any) error {
	for attempt := 1; ; attempt++ {
		_, err := m.request(ctx, t.Target, "/v1/transfer/verify", proofs, &map[string]int{})
		var idle *transferIdleError
		if err == nil || !errors.As(err, &idle) {
			return err
		}
		if attempt >= transferIdleAttempts {
			idleEvent(t, "/v1/transfer/verify", "abort", attempt, err)
			return err
		}
		idleEvent(t, "/v1/transfer/verify", "retry", attempt, err)
		if err := sleepContext(ctx, m.tick); err != nil {
			return err
		}
	}
}

// A per-name estimate above the receiver's per-name limit cannot be real: an
// inventory total was planned as a per-record default (13 Nov user/vm specs:
// 1.2 GB per name, one name per transfer). Batch such rows by the names-plan
// default instead; the target's 413 still splits a batch whose real export is
// larger than --batch-bytes.
const impossibleRecordEstimate = 128 << 20

func (m *mover) recordEstimate(row planRow) int64 {
	if row.EstimatedBytes <= impossibleRecordEstimate {
		return row.EstimatedBytes
	}
	if m.warnedEstimate.CompareAndSwap(false, true) {
		event("implausible_estimate", map[string]any{"level": "error", "metric": row.Metric, "estimated_bytes": row.EstimatedBytes, "used_bytes": ringplan.DefaultNamesEstimate, "hint": "plan estimated_bytes is per name, not an inventory total; batching by the default and relying on 413 splits"})
	}
	return ringplan.DefaultNamesEstimate
}
