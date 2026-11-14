package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

// storectl sync brings one target store to complete history from every
// replica that may hold its names, in one resumable invocation:
//
//  1. discover: union of the source catalogs (/v1/transfer/list) that the
//     target owns under --ring, plus the target's own names. Names whose every
//     archive keeps at most 3 days are skipped.
//  2. copy missing: names absent on the target, from the first source in
//     --from order that holds them, through the plan-free copy engine.
//  3. verify: every name present on both sides, every archive. A hole is a
//     timestamp where a source has a finite point and the target has none.
//  4. fill: re-pull names with holes from each source that has the points
//     (the importer keeps existing target points), then re-verify only them.
//  5. repeat 3-4 while verification makes progress.
//
// Every phase is journaled under --state; --resume restores the saved run.
// The stores are reached through syncBackend so the copy engine and the read
// path can change underneath without touching the orchestration.

const (
	syncShortRetention = 3 * 86400
	syncListPage       = 10000
	exitSyncResidual   = 2
)

// ErrSyncResidual is returned when holes or missing names remain that a
// source holds but the run could not fill; the summary lists them.
var ErrSyncResidual = errors.New("sync finished with residual holes")

type syncArchive struct{ Step, Points int64 }

// syncSeries is one archive's values as carbonserver returns them: Values[i]
// belongs to Start + i*Step, NaN when absent.
type syncSeries struct {
	Start, Step int64
	Values      []float64
}

func (s syncSeries) finite(ts int64) bool {
	if s.Step <= 0 || ts < s.Start || (ts-s.Start)%s.Step != 0 {
		return false
	}
	i := (ts - s.Start) / s.Step
	return i < int64(len(s.Values)) && !math.IsNaN(s.Values[i]) && !math.IsInf(s.Values[i], 0)
}

type syncCopyResult struct {
	Copied    int
	Failed    []string
	Forbidden bool
}

// syncBackend is everything sync needs from the stores. Info and Fetch omit
// names the store does not know; Copy pulls names from source into target
// with existing target points winning, journaled at journal.
type syncBackend interface {
	List(ctx context.Context, store, prefix, after string, limit int) ([]string, error)
	Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error)
	Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error)
	Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error)
	// Digest returns per-archive digests over [from, until); errSyncNoDigest
	// when the store has no digest endpoint.
	Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error)
}

type syncOptions struct {
	noCopy, noVerify bool
	verifyStreams    int
	policy           *effectiveSyncPolicy
	namesHash        string
	namesFile        string
	to               string
	from             []string
	ring, state      string
	ringIdentity     string
	prefix           string
	streams          int
	batch            int
	maxRounds        int
	settle           time.Duration
	windows          syncRanges
	sampleEvery      int
	includeShort     bool
	fullHistory      bool
	progress         time.Duration
	now              func() time.Time
}

func (o *syncOptions) normalize() error {
	var err error
	if o.to, err = origin(o.to); err != nil {
		return fmt.Errorf("--to: %w", err)
	}
	if len(o.from) == 0 {
		return errors.New("at least one --source URL required (--from is a compatibility alias)")
	}
	if len(o.from) > 63 {
		return errors.New("at most 63 --source URLs")
	}
	seen := map[string]bool{o.to: true}
	for i, f := range o.from {
		if o.from[i], err = origin(f); err != nil {
			return fmt.Errorf("--source: %w", err)
		}
		if seen[o.from[i]] {
			return fmt.Errorf("--source %s repeats the target or another source", o.from[i])
		}
		seen[o.from[i]] = true
	}
	if o.state == "" {
		return errors.New("--state directory required (rerun the same command with it to resume)")
	}
	if o.verifyStreams < 0 || o.streams < 1 || o.batch < 1 || o.maxRounds < 0 || o.settle < 0 || o.sampleEvery < 0 {
		return errors.New("invalid --streams/--verify-batch/--max-rounds/--settle")
	}
	if o.now == nil {
		o.now = time.Now
	}
	return nil
}

// syncName is one discovered name: which sources hold it, whether the target
// holds it, and whether every archive is short-retention.
type syncName struct {
	Name     string
	Sources  uint64
	OnTarget bool
	Short    bool
}

type syncHole struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	Window string `json:"window,omitempty"`
	Step   int64  `json:"step"`
	Points int    `json:"points"`
	First  int64  `json:"first"`
	Last   int64  `json:"last"`
}

type syncRow struct {
	Resolved          []syncSkip               `json:"resolved,omitempty"`
	Discovery         *syncDiscoveryCheckpoint `json:"discovery,omitempty"`
	Selected          []string                 `json:"selected,omitempty"`
	SelectionComplete bool                     `json:"selection_complete,omitempty"`
	Kind              string                   `json:"kind"`
	Identity          string                   `json:"identity,omitempty"`
	Phase             string                   `json:"phase,omitempty"`
	Round             int                      `json:"round,omitempty"`
	Source            string                   `json:"source,omitempty"`
	First             string                   `json:"first,omitempty"`
	Last              string                   `json:"last,omitempty"`
	Names             int                      `json:"names,omitempty"`
	Copied            int                      `json:"copied,omitempty"`
	Failed            []string                 `json:"failed,omitempty"`
	Forbidden         bool                     `json:"forbidden,omitempty"`
	Holes             []syncHole               `json:"holes,omitempty"`
	Absent            []string                 `json:"absent,omitempty"`
	Unindexed         []string                 `json:"unindexed,omitempty"`
	Short             []string                 `json:"short,omitempty"`
	Skipped           []syncSkip               `json:"skipped,omitempty"`
	Missing           int                      `json:"missing,omitempty"`
	OnTarget          int                      `json:"on_target,omitempty"`
	Both              int                      `json:"both,omitempty"`
	NotOwned          int                      `json:"not_owned,omitempty"`
	Time              string                   `json:"time"`
}

type syncJournal struct {
	lock     *os.File
	mu       sync.Mutex
	file     *os.File
	rows     []syncRow
	progress progressSnapshot
}

func openSyncJournal(path, identity string) (*syncJournal, error) {
	lock, err := lockResumeState(path)
	if err != nil {
		return nil, err
	}
	opened := false
	defer func() {
		if !opened {
			lock.Close()
		}
	}()
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	j := &syncJournal{file: f, lock: lock}
	reader := bufio.NewReaderSize(f, 1<<20)
	var good int64
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			break // a torn final line is truncated below
		}
		if err != nil {
			f.Close()
			return nil, err
		}
		var row syncRow
		if err := json.Unmarshal(line, &row); err != nil {
			f.Close()
			return nil, fmt.Errorf("journal %s: corrupt row at byte %d: %w", path, good, err)
		}
		good += int64(len(line))
		if row.Kind == "header" && row.Identity != identity {
			f.Close()
			return nil, fmt.Errorf("journal %s belongs to another sync (%s); use a new --state", path, row.Identity)
		}
		j.rows = append(j.rows, row)
		j.progress.apply(row)
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if len(j.rows) == 0 {
		if err := j.append(syncRow{Kind: "header", Identity: identity}); err != nil {
			f.Close()
			return nil, err
		}
	}
	opened = true
	return j, nil
}

func (j *syncJournal) append(row syncRow) error {
	row.Time = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.file.Write(append(data, '\n')); err != nil {
		return err
	}
	j.rows = append(j.rows, row)
	j.progress.apply(row)
	return j.file.Sync()
}

func (j *syncJournal) find(match func(syncRow) bool) []syncRow {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []syncRow
	for _, r := range j.rows {
		if match(r) {
			out = append(out, r)
		}
	}
	return out
}

func (j *syncJournal) close() { _ = j.file.Close(); _ = j.lock.Close() }

type syncPhase struct {
	names   int
	started time.Time
	seconds float64
}

type syncSummary struct {
	Discovered, OnTarget, Missing, NotOwned, Short  int
	Copied, CopyFailed                              int
	Verified, HoleNames, HolePoints                 int
	Filled, Residual, ResidualPoints, ResidualNames int
	Rounds                                          int
	Forbidden                                       []string
	phases                                          map[string]*syncPhase
	residual                                        []syncResidual
}

type syncResidual struct {
	syncHole
	Reason string `json:"reason"`
}

type syncer struct {
	legacyCopyReceipts bool // keep the selection and transport of old window campaigns
	o                  syncOptions
	b                  syncBackend
	j                  *syncJournal
	copied             map[string]bool // missing names copied in this sync
	masks              sync.Map        // name -> source mask, for names being verified
	owner              func(string) bool
	forbid             map[string]bool
	sum                syncSummary
	started            time.Time
	failedCp           map[string]bool // names whose copy failed
	both               int             // names on the target and on a source
	down               sync.Map        // store -> reason: unavailable for --host-wait
	digests            sync.Map        // store -> bool: serves /v1/transfer/digest
	prog               syncProgress
}

func syncIdentity(o syncOptions) (string, error) {
	identityFields := map[string]any{"to": o.to, "from": o.from, "ring": o.ring, "prefix": o.prefix, "windows": o.windows.String(), "sample_every": o.sampleEvery}
	if o.namesHash != "" {
		identityFields["names_sha256"] = o.namesHash
	}
	// Keep old default journals resumable, but never reuse skipped comparisons
	// when a recovery opts into short-retention names.
	if o.fullHistory && len(o.windows) > 0 {
		identityFields["full_history"] = true
	}
	if o.includeShort {
		identityFields["include_short_retention"] = true
	}
	if o.ringIdentity != "" {
		identityFields["ring"] = o.ringIdentity
	}
	identity, err := json.Marshal(identityFields)
	return string(identity), err
}

func runSync(ctx context.Context, o syncOptions, b syncBackend) error {
	if err := os.MkdirAll(o.state, 0o755); err != nil {
		return err
	}
	identity, err := syncIdentity(o)
	if err != nil {
		return err
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity)
	if err != nil {
		return err
	}
	defer j.close()
	s := &syncer{o: o, b: b, j: j, forbid: map[string]bool{}, failedCp: map[string]bool{}, copied: map[string]bool{}, started: time.Now()}
	s.sum.phases = map[string]*syncPhase{}
	if o.ring != "" {
		if s.owner, err = ringplan.TargetOwner(o.ring, o.to); err != nil {
			return err
		}
	}
	for _, r := range j.find(func(r syncRow) bool { return r.Kind == "copied" && r.Forbidden }) {
		s.forbid[r.Source] = true
	}
	event("sync_start", map[string]any{"to": o.to, "from": o.from, "ring": o.ring, "prefix": o.prefix, "state": o.state, "streams": o.streams, "verify_batch": o.batch, "max_rounds": o.maxRounds, "windows": o.windows.String(), "sample_every": o.sampleEvery, "resumed_rows": len(j.rows) - 1, "effective": o.policy})
	ctx, cancelFile := context.WithCancelCause(ctx)
	defer cancelFile(nil)
	progressCtx, stopProgress := context.WithCancel(ctx)
	go s.progressLoop(progressCtx, o.progress)
	stopFile := startProgressFile(progressCtx, filepath.Join(o.state, "progress.json"), func() progressSnapshot {
		j.mu.Lock()
		snap := j.progress
		snap.Cursors = cloneCursors(snap.Cursors)
		j.mu.Unlock()
		s.prog.mu.Lock()
		snap.Phase = s.prog.phase
		snap.Done = s.prog.done.Load()
		snap.Total = int64(s.prog.total)
		s.prog.mu.Unlock()
		snap.Effective = o.policy
		return snap
	}, cancelFile)
	err = s.run(ctx)
	if cause := context.Cause(ctx); cause != nil && err != nil {
		err = cause
	}
	stopProgress()
	progressErr := err
	if progressErr == nil && len(s.sum.residual) > 0 {
		progressErr = ErrSyncResidual
	}
	if perr := stopFile(progressErr); perr != nil {
		err = errors.Join(err, perr)
	}
	s.summary(err)
	if err != nil {
		return err
	}
	if s.sum.Residual > 0 {
		return fmt.Errorf("%w: %d residual (names or holes), see sync_done", ErrSyncResidual, s.sum.Residual)
	}
	return nil
}

func (s *syncer) phase(name string) *syncPhase {
	p := s.sum.phases[name]
	if p == nil {
		p = &syncPhase{started: time.Now()}
		s.sum.phases[name] = p
	}
	return p
}

func (s *syncer) endPhase(name string, names int) {
	p := s.phase(name)
	p.names += names
	p.seconds = time.Since(p.started).Seconds()
	fields := map[string]any{"phase": name, "names": p.names, "seconds": p.seconds}
	if p.seconds > 0 {
		fields["names_per_second"] = float64(p.names) / p.seconds
	}
	event("sync_phase", fields)
}

func (s *syncer) run(ctx context.Context) error {
	if err := s.discover(ctx); err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	if !s.o.noCopy {
		if err := s.copyMissing(ctx); err != nil {
			return fmt.Errorf("copy missing: %w", err)
		}
	}
	if s.o.noVerify {
		event("phase_skipped", map[string]any{"phase": "verify", "readiness": false})
		return nil
	}
	if err := s.retryWindowSelections(ctx); err != nil {
		return fmt.Errorf("retry window selections: %w", err)
	}
	err := s.verifyAndFill(ctx)
	if s.o.noCopy && s.sum.Missing > 0 {
		return errors.Join(err, fmt.Errorf("%w: --no-copy leaves %d names from discovery unimported; use a fresh state for an independent audit", ErrSyncResidual, s.sum.Missing))
	}
	return err
}

func (s *syncer) namesFile(kind string, round int, source int, names []string) (string, error) {
	base := filepath.Join(s.o.state, fmt.Sprintf("%s-r%d-s%d", kind, round, source))
	sort.Strings(names)
	tmp := base + ".names.tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	w := bufio.NewWriter(f)
	for _, n := range names {
		fmt.Fprintln(w, n)
	}
	if err := errors.Join(w.Flush(), f.Close()); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, base+".names"); err != nil {
		return "", err
	}
	return base + ".jsonl", nil
}

// copyFrom copies names from one source, recording the outcome. A source the
// target refuses (403: not in transfer-sources) is marked forbidden.
func (s *syncer) copyFrom(ctx context.Context, phase string, round, src int, names []string, short ...string) (syncCopyResult, error) {
	source := s.o.from[src]
	// The caller owns phase progress; a source batch must not reset its totals
	// or replace a stable phase with an immutable per-batch journal identity.
	journal, err := s.namesFile(phase, round, src, names)
	if err != nil {
		return syncCopyResult{}, err
	}
	event("sync_copy", map[string]any{"phase": phase, "round": round, "source": source, "names": len(names), "journal": journal})
	var res syncCopyResult
	if s.windowOnly() && !s.legacyCopyReceipts {
		backend, ok := s.b.(syncWindowBackend)
		if !ok {
			return res, errors.New("backend does not support window-only copy")
		}
		res, err = backend.CopyWindow(ctx, source, s.o.to, names, journal, s.copyWindows())
	} else {
		res, err = s.b.Copy(ctx, source, s.o.to, names, journal)
	}
	if err != nil {
		return res, err
	}
	if res.Forbidden {
		s.forbid[source] = true
		event("sync_source_forbidden", map[string]any{"level": "error", "source": source, "target": s.o.to, "hint": "the target transfer-sources allowlist does not list this source; continuing with the others"})
	}
	var selected []string
	if s.windowOnly() && !s.legacyCopyReceipts {
		selected = names
	}
	if err := s.j.append(syncRow{Kind: "copied", Phase: phase, Round: round, Source: source, First: names[0], Last: names[len(names)-1], Names: len(names), Copied: res.Copied, Failed: res.Failed, Forbidden: res.Forbidden, Short: short, Selected: selected, SelectionComplete: s.windowOnly()}); err != nil {
		return res, err
	}
	return res, nil
}

func (s *syncer) sourcesOf(mask uint64) string {
	var out []string
	for i, f := range s.o.from {
		if mask&(1<<uint(i)) != 0 {
			out = append(out, f)
		}
	}
	return strings.Join(out, ",")
}

func (s *syncer) addResidual(h syncHole, reason string) {
	s.sum.residual = append(s.sum.residual, syncResidual{h, reason})
}

type syncKey struct{ name, source string }

// verifyAndFill runs verify/fill rounds. Round 1 verifies every name present
// on the target and on at least one source; later rounds only names filled
// or not yet indexed in the previous round.
func (s *syncer) verifyAndFill(ctx context.Context) error {
	var set []string // rounds after the first: the names to re-verify
	filledBefore := map[syncKey]bool{}
	failedAttempts := map[syncKey]int{}
	retryFailed := false
	// latest is each name's most recent verification: its holes, or nil when
	// clean; pending names were not indexed by the target's carbonserver.
	latest := map[string][]syncHole{}
	pending := map[string]bool{}
	skippedLast := map[string]string{} // name -> reason of its latest skip
	var err error
	previousScore := int64(-1)
	for round := 1; ; round++ {
		s.sum.Rounds = round
		names := s.unionSource()
		if round > 1 {
			if names, err = s.setSource(set); err != nil {
				return err
			}
		}
		holes, unindexed, skipped, err := s.verifyRound(ctx, round, names)
		if err != nil {
			return fmt.Errorf("verify round %d: %w", round, err)
		}
		for _, n := range set {
			delete(latest, n)
			delete(pending, n)
			delete(skippedLast, n)
		}
		for _, h := range holes {
			latest[h.Name] = append(latest[h.Name], h)
		}
		for _, n := range unindexed {
			pending[n] = true
		}
		for _, k := range skipped {
			skippedLast[k.Name] = k.Reason
		}
		// Fillable: a reachable source has the points and this pair was not
		// already filled without effect.
		bySource := map[int][]string{}
		seen := map[syncKey]bool{}
		next := map[string]bool{}
		fillable := 0
		for _, h := range holes {
			k := syncKey{h.Name, h.Source}
			if s.forbid[h.Source] || failedAttempts[k] >= 3 {
				continue
			}
			fillable++
			if !seen[k] {
				seen[k] = true
				src := s.sourceIndex(h.Source)
				bySource[src] = append(bySource[src], h.Name)
			}
		}
		for _, n := range unindexed {
			next[n] = true
		}
		for _, k := range skipped { // retried once more in the next round
			next[k.Name] = true
		}
		event("sync_round", map[string]any{"round": round, "verified": len(set), "holes": len(holes), "fillable": fillable, "unindexed": len(unindexed), "skipped": len(skipped)})
		score := int64(len(unindexed) + len(skipped))
		for _, h := range holes {
			score += int64(max(1, h.Points))
		}
		stalled := previousScore >= 0 && score >= previousScore
		previousScore = score
		if s.o.noCopy || (fillable == 0 && len(next) == 0) || (stalled && !retryFailed) || (s.o.maxRounds > 0 && round == s.o.maxRounds) {
			break
		}
		retryFailed = false
		s.phase("fill")
		s.prog.begin("fill", fillable)
		work := 0
		for src := range s.o.from {
			names := bySource[src]
			if len(names) == 0 {
				continue
			}
			var res syncCopyResult
			if rows := s.j.find(func(r syncRow) bool {
				return r.Kind == "copied" && r.Phase == "fill" && r.Round == round && r.Source == s.o.from[src]
			}); len(rows) > 0 {
				res = syncCopyResult{Copied: rows[0].Copied, Failed: rows[0].Failed, Forbidden: rows[0].Forbidden}
			} else {
				res, err = s.copyFrom(ctx, "fill", round, src, names)
				if err != nil {
					return err
				}
				work += len(names)
			}
			if res.Forbidden {
				continue
			}
			bad := map[string]bool{}
			for _, n := range res.Failed {
				bad[n] = true
			}
			for _, n := range names {
				// Failed imports remain in verification and keep their journal
				// evidence. Permit up to three failed fills per name/source,
				// even when the hole count has not improved yet.
				k := syncKey{n, s.o.from[src]}
				next[n] = true
				if bad[n] {
					failedAttempts[k]++
					retryFailed = retryFailed || failedAttempts[k] < 3
				} else {
					delete(failedAttempts, k)
					filledBefore[k] = true
				}
			}
			s.sum.Filled += len(names) - len(res.Failed)
			s.prog.done.Add(int64(len(names)))
		}
		s.endPhase("fill", work)
		set = set[:0]
		for n := range next {
			set = append(set, n)
		}
		sort.Strings(set)
	}
	// Copy failures are provisional: verification may recover the same name
	// from another holder. Keep only failures still evidenced by the final read.
	kept := s.sum.residual[:0]
	for _, r := range s.sum.residual {
		if (r.Reason == "copy_failed" || r.Reason == "missing_only_on_forbidden_source") && len(latest[r.Name]) == 0 && !pending[r.Name] && skippedLast[r.Name] == "" {
			continue
		}
		kept = append(kept, r)
	}
	s.sum.residual = kept
	// Residual: each name's latest holes, and names never indexed.
	names := make([]string, 0, len(latest))
	for n := range latest {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		for _, h := range latest[n] {
			reason := "copy_failed"
			if s.o.maxRounds > 0 && s.sum.Rounds >= s.o.maxRounds {
				reason = "round_limit"
			}
			switch {
			case s.forbid[h.Source]:
				reason = "source_forbidden"
			case filledBefore[syncKey{h.Name, h.Source}]:
				reason = "persisted_after_fill"
			}
			s.addResidual(h, reason)
		}
	}
	unindexed := make([]string, 0, len(pending))
	for n := range pending {
		unindexed = append(unindexed, n)
	}
	sort.Strings(unindexed)
	for _, n := range unindexed {
		s.addResidual(syncHole{Name: n}, "not_indexed_on_target")
	}
	skippedNames := make([]string, 0, len(skippedLast))
	for n := range skippedLast {
		skippedNames = append(skippedNames, n)
	}
	sort.Strings(skippedNames)
	for _, n := range skippedNames {
		reason := skippedLast[n]
		if reason != "schema_step_mismatch" {
			reason = "skipped: " + reason
		}
		s.addResidual(syncHole{Name: n}, reason)
	}
	return nil
}

func (s *syncer) sourceIndex(source string) int {
	for i, f := range s.o.from {
		if f == source {
			return i
		}
	}
	return -1
}

// verifyRound compares the names in batches of --verify-batch with --streams
// workers. Completed batches are journaled with their holes; a resumed round
// replays them instead of reading again (batch boundaries are deterministic).
func (s *syncer) verifyRound(ctx context.Context, round int, set syncNameSource) ([]syncHole, []string, []syncSkip, error) {
	done := map[string]syncRow{}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "verified" && r.Round == round }) {
		done[r.First] = r
	}
	phase := s.phase("verify")
	var mu sync.Mutex
	var holes []syncHole
	var unindexed []string
	var skipped []syncSkip
	holeNames := map[string]bool{}
	s.prog.begin(fmt.Sprintf("verify round %d", round), set.total)
	defer set.close()
	collect := func(r syncRow) {
		mu.Lock()
		defer mu.Unlock()
		s.prog.done.Add(int64(r.Names))
		holes = append(holes, r.Holes...)
		unindexed = append(unindexed, r.Unindexed...)
		skipped = append(skipped, r.Skipped...)
		for _, resolved := range r.Resolved {
			kept := s.sum.residual[:0]
			for _, residual := range s.sum.residual {
				if residual.Name != resolved.Name {
					kept = append(kept, residual)
				}
			}
			s.sum.residual = kept
		}
		for _, h := range r.Holes {
			if !holeNames[h.Name] {
				holeNames[h.Name] = true
			}
		}
	}
	type job struct {
		names []string
		masks []uint64
	}
	jobs := make(chan job)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	verified := 0
	for range s.o.verifyConcurrency() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for jb := range jobs {
				for i, n := range jb.names {
					s.masks.Store(n, jb.masks[i])
				}
				row, err := s.verifyBatch(ctx, round, jb.names)
				for _, n := range jb.names {
					s.masks.Delete(n)
				}
				if err == nil {
					err = s.j.append(row)
				}
				if err != nil {
					cancel(err)
					continue
				}
				collect(row)
				mu.Lock()
				verified += len(jb.names)
				mu.Unlock()
				if len(row.Holes) > 0 {
					event("sync_holes", map[string]any{"round": round, "first": row.First, "last": row.Last, "names": len(jb.names), "holes": len(row.Holes)})
				}
			}
		}()
	}
	resumed, fed := 0, 0
	var feedErr error
feed:
	for {
		var chunk job
		for len(chunk.names) < s.o.batch {
			name, mask, err := set.next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				feedErr = err
				break feed
			}
			chunk.names = append(chunk.names, name)
			chunk.masks = append(chunk.masks, mask)
		}
		if len(chunk.names) == 0 {
			break
		}
		fed += len(chunk.names)
		if r, ok := done[chunk.names[0]]; ok && r.Last == chunk.names[len(chunk.names)-1] {
			collect(r)
			resumed += len(chunk.names)
			continue
		}
		select {
		case jobs <- chunk:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if cause := context.Cause(ctx); cause != nil {
		return nil, nil, nil, cause
	}
	if feedErr != nil {
		return nil, nil, nil, feedErr
	}
	points := 0
	for _, h := range holes {
		points += h.Points
	}
	if round == 1 {
		s.sum.Verified = fed
		s.sum.HoleNames, s.sum.HolePoints = len(holeNames), points
	}
	phase.names += verified
	sort.Slice(holes, func(a, b int) bool {
		if holes[a].Name != holes[b].Name {
			return holes[a].Name < holes[b].Name
		}
		if holes[a].Source != holes[b].Source {
			return holes[a].Source < holes[b].Source
		}
		return holes[a].Step < holes[b].Step
	})
	sort.Strings(unindexed)
	s.endPhase("verify", 0)
	event("sync_verified", map[string]any{"round": round, "names": fed, "resumed": resumed, "hole_names": len(holeNames), "hole_points": points, "unindexed": len(unindexed), "skipped": len(skipped),
		"compared_by_digest": s.prog.digests.Load(), "read_point_by_point": s.prog.reads.Load()})
	return holes, unindexed, skipped, nil
}

// syncWindow is the range only archive a serves: from just inside its
// retention (with a margin for the time between the two reads) to now-settle.
func syncWindow(now int64, a syncArchive, settle time.Duration) (int64, int64) {
	step := a.Step
	from := (now+2400-step*a.Points)/step*step + 2*step
	until := (now - int64(settle.Seconds())) / step * step
	return from, until
}

// syncCompare counts timestamps in [from, until) where the source has a
// finite point and the target has none. Differing values are not holes:
// existing target points win.
func syncCompare(name, source string, src, dst syncSeries, step, from, until int64) (syncHole, bool) {
	h := syncHole{Name: name, Source: source, Step: step}
	for ts := from; ts < until; ts += step {
		if src.finite(ts) && !dst.finite(ts) {
			if h.Points == 0 {
				h.First = ts
			}
			h.Last = ts
			h.Points++
		}
	}
	return h, h.Points > 0
}

func (s *syncer) summary(err error) {
	residualNames := map[string]bool{}
	points := 0
	for _, r := range s.sum.residual {
		residualNames[r.Name] = true
		points += r.Points
	}
	s.sum.Residual = len(s.sum.residual)
	s.sum.ResidualNames, s.sum.ResidualPoints = len(residualNames), points
	forbidden := make([]string, 0, len(s.forbid))
	for f := range s.forbid {
		forbidden = append(forbidden, f)
	}
	sort.Strings(forbidden)
	phases := map[string]any{}
	for name, p := range s.sum.phases {
		f := map[string]any{"names": p.names, "seconds": p.seconds}
		if p.seconds > 0 {
			f["names_per_second"] = float64(p.names) / p.seconds
		}
		phases[name] = f
	}
	residual := s.sum.residual
	const shown = 1000
	if len(residual) > shown {
		residual = residual[:shown]
	}
	fields := map[string]any{
		"to": s.o.to, "discovered": s.sum.Discovered, "on_target_at_start": s.sum.OnTarget, "missing": s.sum.Missing,
		"short_skipped": s.sum.Short, "not_owned": s.sum.NotOwned, "copied": s.sum.Copied, "copy_failed": s.sum.CopyFailed,
		"verified": s.sum.Verified, "hole_names": s.sum.HoleNames, "hole_points": s.sum.HolePoints, "filled": s.sum.Filled,
		"rounds": s.sum.Rounds, "residual": s.sum.Residual, "residual_names": s.sum.ResidualNames, "residual_points": points,
		"residual_detail": residual, "forbidden_sources": forbidden, "phases": phases, "seconds": time.Since(s.started).Seconds(),
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	if err == nil && s.sum.Residual == 0 {
		fields["verdict"] = "COMPLETE"
	} else {
		fields["verdict"] = "INCOMPLETE"
	}
	resolved := map[string]syncSkip{}
	for _, r := range s.j.find(func(r syncRow) bool { return len(r.Resolved) > 0 }) {
		for _, item := range r.Resolved {
			resolved[item.Name] = item
		}
	}
	fields["resolved_empty"] = len(resolved)
	if err := atomicStateJSON(filepath.Join(s.o.state, "resolved.json"), resolved); err != nil {
		event("sync_resolution_error", map[string]any{"error": err.Error()})
	}
	event("sync_done", fields)
	if f, ferr := os.Create(filepath.Join(s.o.state, "residual.json")); ferr == nil {
		_ = json.NewEncoder(f).Encode(s.sum.residual)
		_ = f.Close()
	}
}

func (o syncOptions) verifyConcurrency() int {
	if o.verifyStreams > 0 {
		return o.verifyStreams
	}
	return o.streams
}
