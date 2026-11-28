package downsize

import (
	"fmt"
	"sync"
)

// windowBudgets bound native window work per range across CopyWindow passes.
//
// The CopyWindow host wait reruns a whole donor pass after any restart-class
// error. Without per-range memory, each rerun granted every deferred range a
// fresh native retry budget, and one range that kept timing out kept all the
// others retrying forever. A budget lasts for one process: a resume grants each
// unfinished range one more bounded budget. Exhausted names stay Failed, so
// verification decides whether they already match (done), are expected absent,
// or remain residual with the recorded reason.
type windowBudgets struct {
	ranges sync.Map // window journal + range -> *windowRangeBudget
	names  sync.Map // metric name -> reason it was left unconverged
}

type windowRangeBudget struct {
	mu       sync.Mutex
	failures int
	reason   string
}

const windowTransferExhausted = "native transfer failed"

// windowRangeRestartLimit is the --copy-attempts default. Library callers that
// leave copyAttempts unset keep tolerating brief restarts rather than failing a
// range on its first restart-class error.
const windowRangeRestartLimit = 6

func windowRangeKey(journal string, start, end int) string {
	return fmt.Sprintf("%s:%d:%d", journal, start, end)
}

func (w *windowBudgets) budget(key string) *windowRangeBudget {
	v, _ := w.ranges.LoadOrStore(key, &windowRangeBudget{})
	return v.(*windowRangeBudget)
}

// exhausted returns why native work on this range stopped, or "".
func (w *windowBudgets) exhausted(key string) string {
	v, ok := w.ranges.Load(key)
	if !ok {
		return ""
	}
	r := v.(*windowRangeBudget)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reason
}

// exhaust ends native work on a range for this process. The first reason wins.
func (w *windowBudgets) exhaust(key, reason string) {
	r := w.budget(key)
	r.mu.Lock()
	if r.reason == "" {
		r.reason = reason
	}
	r.mu.Unlock()
}

// failed counts one restart-class failure of a range. Callers count only after
// both stores answered this pass's capability checks, so an outage, which fails
// those checks, keeps waiting rather than exhausting ranges. It returns the
// exhaustion reason once the range reaches its limit.
func (w *windowBudgets) failed(key string, limit int, err error) string {
	r := w.budget(key)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reason != "" {
		return ""
	}
	if limit <= 0 {
		limit = windowRangeRestartLimit
	}
	r.failures++
	if r.failures < limit {
		return ""
	}
	r.reason = fmt.Sprintf("%s %d times: %v", windowTransferExhausted, r.failures, err)
	return r.reason
}

func (w *windowBudgets) recordName(name, reason string) {
	w.names.Store(name, reason)
}

func (w *windowBudgets) nameReason(name string) string {
	if v, ok := w.names.Load(name); ok {
		return v.(string)
	}
	return ""
}

// windowFailureReason lets final verification attach the native reason to a
// residual name, which quarantine.json then records.
func (b *syncLiveBackend) windowFailureReason(name string) string {
	return b.windowBudget.nameReason(name)
}
