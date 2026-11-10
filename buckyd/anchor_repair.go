package buckyd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

const (
	// maxAnchorNames bounds one maintenance request; it also caps the useful
	// per-request worker count.
	maxAnchorNames             = 32
	maxMaintenanceRequests     = 64
	defaultMaintenanceRequests = 2
	defaultMaintenanceWorkers  = 8
)

// archiveAnchors inspects at most 32 explicitly named metrics. Pagination and
// durable per-name progress belong to the caller; repeating a successful name
// is a no-op. A read token can only dry-run. No cache drain is requested.
func (s *Service) archiveAnchors(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, errors.New("POST required"), 405)
		return
	}
	var request struct {
		Names []string `json:"names"`
		Apply bool     `json:"apply"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, err, 400)
		return
	}
	if len(request.Names) == 0 || len(request.Names) > maxAnchorNames {
		writeError(w, fmt.Errorf("anchor maintenance needs 1..%d explicit names", maxAnchorNames), 400)
		return
	}
	op := "read"
	if request.Apply {
		op = "update"
	}
	if err := s.allowedRoot(op, r); err != nil {
		writeError(w, err, 403)
		return
	}
	if request.Apply {
		s.copyMu.RLock()
		defer s.copyMu.RUnlock()
		if s.StoreCopyActive() {
			writeError(w, errors.New("maintenance fenced by whole-store copy"), 409)
			return
		}
	}
	// Resize controllers can occupy every transfer slot continuously. Keep
	// repair admission independent and bounded, including while replying.
	select {
	case s.maintenance <- struct{}{}:
	default:
		writeError(w, errors.New("too many concurrent maintenance requests"), 429)
		return
	}
	defer func() { <-s.maintenance }()
	rows := anchorRows(r.Context(), request.Names, s.config.MaintenanceWorkers, func(ctx context.Context, name string) (chunkstore.AnchorRepairResult, error) {
		return s.store.RepairArchiveAnchors(ctx, name, request.Apply)
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rows)
}

type anchorRow struct {
	chunkstore.AnchorRepairResult
	Error string `json:"error,omitempty"`
}

// anchorRows prepares at most workers names at once; per-name production
// latency is heavy-tailed and mostly waits on reads, so a serial pass leaves
// the host idle. Each call keeps its own semantics: dry-run reads one
// immutable view, apply takes only that name's transfer stripe, and errors
// stay per row. Repeated names run in request order on one worker, so each
// occurrence observes its predecessor as in a serial pass. Rows keep request
// order. Once ctx is done no further name starts and the reply is the longest
// completed prefix, the shape a serial pass returns. A panic is re-raised on
// the calling (handler) goroutine with the worker stack, where net/http
// contains it as before.
func anchorRows(ctx context.Context, names []string, workers int, repair func(context.Context, string) (chunkstore.AnchorRepairResult, error)) []anchorRow {
	groups := make([][]int, 0, len(names))
	first := make(map[string]int, len(names))
	for i, name := range names {
		if g, ok := first[name]; ok {
			groups[g] = append(groups[g], i)
			continue
		}
		first[name] = len(groups)
		groups = append(groups, []int{i})
	}
	rows := make([]anchorRow, len(names))
	done := make([]bool, len(names))
	run := func(i int) {
		result, err := repair(ctx, names[i])
		result.Name = names[i]
		rows[i] = anchorRow{AnchorRepairResult: result}
		if err != nil {
			rows[i].Error = err.Error()
		}
		done[i] = true
	}
	if workers = min(max(workers, 1), len(groups)); workers == 1 {
		for i := range names {
			run(i)
			if ctx.Err() != nil {
				break
			}
		}
	} else {
		var (
			next      atomic.Int64
			stop      atomic.Bool
			wg        sync.WaitGroup
			panicOnce sync.Once
			panicked  any
		)
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if p := recover(); p != nil {
						panicOnce.Do(func() { panicked = fmt.Sprintf("anchor maintenance worker: %v\n%s", p, debug.Stack()) })
						stop.Store(true)
					}
				}()
				for g := int(next.Add(1)) - 1; g < len(groups); g = int(next.Add(1)) - 1 {
					for _, i := range groups[g] {
						if ctx.Err() != nil || stop.Load() {
							return
						}
						run(i)
					}
				}
			}()
		}
		wg.Wait()
		if panicked != nil {
			panic(panicked)
		}
	}
	n := 0
	for n < len(done) && done[n] {
		n++
	}
	return rows[:n]
}
