package downsize

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Completed rounds are scheduling evidence. A legacy receipt proves only its
// exact membership in that round's input, independent of today's batch size.
// Gaps and overlapping observations are unproven; only their names need another
// observation. Recovery is a subtask of the saved phase, never a phase rewind.
func (s *syncer) replayVerifyRound(ctx context.Context, round int, set syncNameSource) (outHoles []syncHole, outUnindexed []string, outSkipped []syncSkip, retErr error) {
	defer set.close()
	var rows []syncRow
	type recoveredName struct {
		holes     []syncHole
		skipped   []syncSkip
		unindexed bool
	}
	repaired := map[string]*recoveredName{}
	planned := map[string]bool{}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Round == round }) {
		if r.Kind == "ambiguous-receipt" {
			for _, n := range r.Selected {
				planned[n] = true
			}
		}
		if r.Kind == "verified" {
			if r.Names <= 0 || r.First == "" || r.Last < r.First {
				return nil, nil, nil, s.receiptContradiction("invalid completed verification receipt")
			}
			valid := func(n string) bool { return n >= r.First && n <= r.Last }
			for _, h := range r.Holes {
				if !valid(h.Name) {
					return nil, nil, nil, s.receiptContradiction("hole outside verification receipt")
				}
			}
			for _, n := range r.Unindexed {
				if !valid(n) {
					return nil, nil, nil, s.receiptContradiction("unindexed name outside verification receipt")
				}
			}
			for _, k := range r.Skipped {
				if !valid(k.Name) {
					return nil, nil, nil, s.receiptContradiction("skip outside verification receipt")
				}
			}
			rows = append(rows, r)
		}
		if r.Kind == "receipt-reverified" {
			for _, n := range r.Selected {
				repaired[n] = &recoveredName{}
			}
			for _, h := range r.Holes {
				if got := repaired[h.Name]; got != nil {
					got.holes = append(got.holes, h)
				}
			}
			for _, k := range r.Skipped {
				if got := repaired[k.Name]; got != nil {
					got.skipped = append(got.skipped, k)
				}
			}
			for _, n := range r.Unindexed {
				if got := repaired[n]; got != nil {
					got.unindexed = true
				}
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].First < rows[j].First })
	// Re-appending the same observation is harmless. Different observations
	// sharing a range remain ambiguous and require a fresh sparse observation.
	unique := rows[:0]
	for _, r := range rows {
		if len(unique) > 0 {
			prior := unique[len(unique)-1]
			if prior.First == r.First && prior.Last == r.Last && prior.Names == r.Names && reflect.DeepEqual(prior.Holes, r.Holes) && reflect.DeepEqual(prior.Unindexed, r.Unindexed) && reflect.DeepEqual(prior.Skipped, r.Skipped) && reflect.DeepEqual(prior.Resolved, r.Resolved) {
				unique[len(unique)-1] = r
				continue
			}
		}
		unique = append(unique, r)
	}
	rows = unique
	var holes []syncHole
	var unindexed []string
	var skipped []syncSkip
	var collectMu sync.Mutex
	collect := func(r syncRow) {
		collectMu.Lock()
		defer collectMu.Unlock()
		holes = append(holes, r.Holes...)
		unindexed = append(unindexed, r.Unindexed...)
		skipped = append(skipped, r.Skipped...)
	}
	// Keep legacy reconstruction at the same bounded concurrency as an active
	// verification round. The feeder owns membership/planning; workers receive
	// immutable batches and publish durable observations before collecting them.
	type repairJob struct {
		names       []string
		ids         []int64
		masks       []uint64
		key, reason string
	}
	work, cancel := context.WithCancel(ctx)
	group, work := errgroup.WithContext(work)
	jobs := make(chan repairJob)
	for range s.o.verifyConcurrency() {
		group.Go(func() error {
			for jb := range jobs {
				if work.Err() != nil {
					return work.Err()
				}
				for i, n := range jb.names {
					s.masks.Store(n, jb.masks[i])
				}
				workCtx := redoBatchContext(work, jb.names, jb.ids)
				// Old seals cannot prove membership of this ambiguous set.
				r, err := s.verifyBatch(workCtx, max(round, s.resume.Round), jb.names)
				for _, n := range jb.names {
					s.masks.Delete(n)
				}
				if err != nil {
					return err
				}
				r.Kind, r.Round, r.Identity = "receipt-reverified", round, jb.key
				r.Selected = jb.names
				r.InvalidationReason = jb.reason
				r.Work = s.redo.cleanWork(workCtx, r)
				if err := s.j.append(r); err != nil {
					return err
				}
				collect(r)
			}
			return nil
		})
	}
	defer func() {
		if retErr != nil {
			cancel()
		}
		close(jobs)
		if err := group.Wait(); err != nil && (retErr == nil || errors.Is(retErr, context.Canceled)) {
			retErr = err
		}
		cancel()
		outHoles, outUnindexed, outSkipped = holes, unindexed, skipped
	}()
	var names []string
	var ids []int64
	var masks []uint64
	repair := func(reason string) error {
		if len(names) == 0 {
			return nil
		}
		// Recovery receipts carry explicit membership, so later batch-size changes
		// cannot lose the completed recovery observation.
		pendingNames := make([]string, 0, len(names))
		pendingIDs := make([]int64, 0, len(ids))
		pendingMasks := make([]uint64, 0, len(masks))
		for i, n := range names {
			// A destructive or ambiguous historical receipt cannot expand
			// recovery beyond the target's current ring responsibility.
			if !s.ownsSyncName(n, masks[i]) {
				continue
			}
			if r := repaired[n]; r != nil {
				row := syncRow{Holes: r.holes, Skipped: r.skipped}
				if r.unindexed {
					row.Unindexed = []string{n}
				}
				collect(row)
				continue
			}
			pendingNames = append(pendingNames, n)
			pendingIDs = append(pendingIDs, ids[i])
			pendingMasks = append(pendingMasks, masks[i])
		}
		names, ids, masks = pendingNames, pendingIDs, pendingMasks
		if len(names) == 0 {
			return nil
		}
		encoded, _ := json.Marshal(struct {
			Round  int
			Names  []string
			Reason string
		}{round, names, reason})
		key := fmt.Sprintf("%x", sha256.Sum256(encoded))
		// The plan is durable before data reads, and is also the once-only log and
		// metric receipt. A crash before its observation retries this same small set.
		var newlyPlanned []string
		for _, n := range names {
			if !planned[n] {
				newlyPlanned = append(newlyPlanned, n)
			}
		}
		if len(newlyPlanned) > 0 {
			if err := s.j.append(syncRow{Kind: "ambiguous-receipt", Identity: key, Round: round, First: newlyPlanned[0], Last: newlyPlanned[len(newlyPlanned)-1], Names: len(newlyPlanned), Selected: newlyPlanned, InvalidationReason: reason}); err != nil {
				return err
			}
			for _, n := range newlyPlanned {
				planned[n] = true
			}
			event("sync_ambiguous_receipt", map[string]any{"reason": reason, "first": newlyPlanned[0], "last": newlyPlanned[len(newlyPlanned)-1], "names": len(newlyPlanned), "round": round, "current_round": s.resume.Round, "phase": s.resume.Phase, "prior_completed_work_retained": true})
		}
		for start := 0; start < len(names); start += max(1, s.o.batch) {
			end := min(start+max(1, s.o.batch), len(names))
			select {
			case jobs <- repairJob{append([]string(nil), names[start:end]...), append([]int64(nil), ids[start:end]...), append([]uint64(nil), masks[start:end]...), key, reason}:
			case <-work.Done():
				return work.Err()
			}
		}
		return nil
	}
	// One lookahead lets us stream the catalog without allocating a round-sized
	// name map. Ambiguous ranges are submitted in bounded batches.
	var name string
	var mask uint64
	var id int64
	advance := func() error {
		for {
			var err error
			name, mask, err = set.next()
			if err != nil {
				return err
			}
			if set.id != nil {
				id = set.id()
			}
			skip, err := s.excludePolicy(name, "verify")
			if err != nil {
				return err
			}
			if !skip {
				return nil
			}
		}
	}
	err := advance()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, nil, err
	}
	if len(rows) == 0 && err == nil {
		return nil, nil, nil, s.receiptContradiction("completed verification has no receipts")
	}
	add := func() { names = append(names, name); ids = append(ids, id); masks = append(masks, mask) }
	reset := func() { names = names[:0]; ids = ids[:0]; masks = masks[:0] }
	for i := 0; i < len(rows); {
		r := rows[i]
		end, j := r.Last, i+1
		for j < len(rows) && rows[j].First <= end {
			end = max(end, rows[j].Last)
			j++
		}
		// Missing receipts affect the gap, not the following proven interval.
		reset()
		for err == nil && name < r.First {
			add()
			err = advance()
			if len(names) >= max(1, s.o.batch) {
				if e := repair("missing legacy receipt"); e != nil {
					return nil, nil, nil, e
				}
				reset()
			}
		}
		if e := repair("missing legacy receipt"); e != nil {
			return nil, nil, nil, e
		}
		reset()
		// Buffer at most one receipt. If its bounds contain too many input names,
		// stream the remaining ambiguous membership instead of growing memory.
		ambiguous := j != i+1
		reason := "overlapping legacy receipts"
		if !ambiguous {
			reason = "legacy receipt membership differs"
		}
		for err == nil && name <= end {
			add()
			err = advance()
			if len(names) > r.Names || ambiguous && len(names) >= max(1, s.o.batch) {
				ambiguous = true
				if e := repair(reason); e != nil {
					return nil, nil, nil, e
				}
				reset()
			}
		}
		if !ambiguous && len(names) == r.Names && names[0] == r.First && names[len(names)-1] == r.Last {
			collect(r)
		} else if e := repair(reason); e != nil {
			return nil, nil, nil, e
		}
		i = j
	}
	reset()
	for err == nil {
		add()
		err = advance()
		if len(names) >= max(1, s.o.batch) {
			if e := repair("missing legacy receipt"); e != nil {
				return nil, nil, nil, e
			}
			reset()
		}
	}
	if !errors.Is(err, io.EOF) {
		return nil, nil, nil, err
	}
	if e := repair("missing legacy receipt"); e != nil {
		return nil, nil, nil, e
	}
	return nil, nil, nil, nil
}

func (s *syncer) receiptContradiction(reason string) error {
	err := fmt.Errorf("contradictory journal: %s", reason)
	appendErr := s.j.append(syncRow{Kind: "receipt-contradiction", InvalidationReason: reason})
	s.j.mu.Lock()
	snapshot := s.j.progress
	snapshot.RedoNames = cloneRedoCounts(snapshot.RedoNames)
	snapshot.Cursors = cloneCursors(snapshot.Cursors)
	s.j.mu.Unlock()
	snapshot.Phase, snapshot.Round = s.resume.Phase, s.resume.Round
	snapshot.Done, snapshot.Total = s.resume.Done, s.resume.Total
	snapshot.Status, snapshot.Error = "incomplete", err.Error()
	outputMu.Lock()
	if progressObserver != nil {
		progressObserver(filepath.Join(s.o.state, "progress.json"), snapshot)
	}
	outputMu.Unlock()
	event("sync_receipt_contradiction", map[string]any{"level": "ERROR", "error": err.Error(), "receipt_contradictions": snapshot.ReceiptContradictions})
	return errors.Join(err, appendErr)
}
