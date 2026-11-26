package downsize

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
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
//     (the importer keeps finite target points), then re-verify only them.
//  5. report remaining holes after the repair verification (two rounds by default).
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
	Copied         int
	Failed         []string
	Forbidden      bool
	ExpectedAbsent []string
}

// syncBackend is everything sync needs from the stores. Info and Fetch omit
// names the store does not know; Copy pulls names from source into target
// with finite target points winning, journaled at journal.
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
	issues           *syncIssues
	noCopy, noVerify bool
	noReleaseProofs  bool
	restartWindow    bool // known restart: prior history is already proven
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
	rescanAll        bool // opt in to full-catalog convergence with a mutation frontier
	settle           time.Duration
	windows          syncRanges
	readyFrontier    bool
	sourceScope      bool // verify only names a source held at discovery (READY off, the default)
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

type syncInvalidation struct {
	At      time.Time  `json:"at,omitempty"`
	Reason  string     `json:"reason"`
	Names   []string   `json:"names,omitempty"`   // empty means every catalog name
	Windows syncRanges `json:"windows,omitempty"` // empty means the entire verified scope
}

type syncRow struct {
	MutationObserved      bool                     `json:"mutation_observed,omitempty"`
	LegacyExpiry          map[string]int64         `json:"legacy_expiration_receipts,omitempty"`
	LegacyDonorProofs     map[string]string        `json:"legacy_donor_proofs,omitempty"`
	LegacyTargetPresent   []string                 `json:"legacy_target_present,omitempty"`
	Invalidation          *syncInvalidation        `json:"invalidation,omitempty"`
	LegacyAdopted         int                      `json:"legacy_observation_adopted,omitempty"`
	LegacyReverified      int                      `json:"legacy_observation_reverified,omitempty"`
	LegacyObservation     bool                     `json:"legacy_observation,omitempty"`
	LegacyObservationTime string                   `json:"legacy_observation_time,omitempty"`
	LegacyInvalidations   []string                 `json:"legacy_invalidations_applied,omitempty"`
	Position              *syncPhasePosition       `json:"phase_position,omitempty"`
	HoleProofs            map[string]string        `json:"hole_proofs_v2,omitempty"`
	LegacyRevalidated     int                      `json:"legacy_revalidated_names,omitempty"`
	ObservationProof      string                   `json:"observation_proof,omitempty"`
	InvalidationReason    string                   `json:"invalidation_reason,omitempty"`
	Work                  []syncRedoWork           `json:"backend_work,omitempty"`
	Reused                int                      `json:"proof_reused,omitempty"`
	Resolved              []syncSkip               `json:"resolved,omitempty"`
	Discovery             *syncDiscoveryCheckpoint `json:"discovery,omitempty"`
	Selected              []string                 `json:"selected,omitempty"`
	OwnershipScoped       bool                     `json:"ownership_scoped,omitempty"`
	SelectionComplete     bool                     `json:"selection_complete,omitempty"`
	Kind                  string                   `json:"kind"`
	Identity              string                   `json:"identity,omitempty"`
	Phase                 string                   `json:"phase,omitempty"`
	Round                 int                      `json:"round,omitempty"`
	Source                string                   `json:"source,omitempty"`
	First                 string                   `json:"first,omitempty"`
	Last                  string                   `json:"last,omitempty"`
	Names                 int                      `json:"names,omitempty"`
	Copied                int                      `json:"copied,omitempty"`
	Failed                []string                 `json:"failed,omitempty"`
	Forbidden             bool                     `json:"forbidden,omitempty"`
	Holes                 []syncHole               `json:"holes,omitempty"`
	Absent                []string                 `json:"absent,omitempty"`
	Unindexed             []string                 `json:"unindexed,omitempty"`
	Short                 []string                 `json:"short,omitempty"`
	Skipped               []syncSkip               `json:"skipped,omitempty"`
	Missing               int                      `json:"missing,omitempty"`
	OnTarget              int                      `json:"on_target,omitempty"`
	Both                  int                      `json:"both,omitempty"`
	NotOwned              int                      `json:"not_owned,omitempty"`
	Time                  string                   `json:"time"`
}

type syncJournal struct {
	phaseGuard    *syncPhasePosition
	phaseGuardRow int
	checkpoint    *syncPhaseCheckpoint
	lock          *os.File
	mu            sync.Mutex
	file          *os.File
	rows          []syncRow
	progress      progressSnapshot
}

func openSyncJournal(path, identity string, checkpointOptions ...string) (*syncJournal, error) {
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
	var good int64
	if cp, cacheErr := readSyncCheckpoint(f, identity, checkpointOptions...); cacheErr != nil {
		event("sync_checkpoint_ignored", map[string]any{"error": cacheErr.Error(), "journal_retained": true})
	} else if cp != nil {
		j.checkpoint, j.rows, j.progress, good = cp, cp.Rows, cp.Progress, cp.Offset
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	reader := bufio.NewReaderSize(f, 1<<20)
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
		if len(j.rows) == 0 && (row.Kind != "header" || row.Identity != identity) {
			f.Close()
			return nil, fmt.Errorf("journal %s: missing or invalid sync header; refusing fresh start", path)
		}
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
	if row.Kind == "verified" && row.Identity == "" {
		// A fresh observation can require a fresh repair even in the same
		// round and with identical holes. Persist its generation before use.
		row.Identity = rand.Text()
	}
	row.Time = time.Now().UTC().Format(time.RFC3339Nano)
	if row.LegacyObservation && row.LegacyObservationTime == "" {
		row.LegacyObservationTime = row.Time
	}
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.guardPhase(row); err != nil {
		return err
	}
	if _, err := j.file.Write(append(data, '\n')); err != nil {
		return err
	}
	j.rows = append(j.rows, row)
	j.progress.apply(row)
	j.observePhase(row, len(j.rows))
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

func (j *syncJournal) close() {
	if err := j.checkpointVerification(); err != nil {
		event("sync_checkpoint_unavailable", map[string]any{"error": err.Error(), "journal_retained": true})
	}
	_ = j.file.Close()
	_ = j.lock.Close()
}

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
	phaseMu              sync.Mutex // phase entry and progress publication are one transition
	resume               syncPhasePosition
	phaseJournal         bool
	newScope             []string
	redo                 *syncRedoGuard
	legacyInvalidations  []syncRow
	frontier             *syncFrontier
	windowRetryCopyError error // selection waits must not hide rejected repairs
	legacyCopyReceipts   bool  // keep the selection and transport of old window campaigns
	verificationBase     int   // append fresh rounds after a terminal residual verdict
	o                    syncOptions
	b                    syncBackend
	j                    *syncJournal
	copied               map[string]bool // missing names copied in this sync
	masks                sync.Map        // name -> source mask, for names being verified
	owner                func(string) bool
	forbid               map[string]bool
	sum                  syncSummary
	started              time.Time
	failedCp             map[string]bool // names whose copy failed
	both                 int             // names on the target and on a source
	down                 sync.Map        // store -> reason: unavailable for --host-wait
	expectedAbsences     sync.Map        // current invocation, never historical exclusions
	digests              sync.Map        // store -> bool: serves /v1/transfer/digest
	prog                 syncProgress
}

func syncIdentity(o syncOptions) (string, error) {
	identityFields := map[string]any{"to": o.to, "from": o.from, "ring": o.ring, "prefix": o.prefix, "windows": o.windows.String(), "sample_every": o.sampleEvery}
	if o.restartWindow {
		identityFields["restart_window"] = true
	}
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

func runSync(ctx context.Context, o syncOptions, b syncBackend) (result error) {
	if err := os.MkdirAll(o.state, 0o755); err != nil {
		return err
	}
	identity, err := syncIdentity(o)
	if err != nil {
		return err
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), identity, syncCheckpointOptions(o))
	if err != nil {
		return err
	}
	defer j.close()
	if o.issues == nil {
		var err error
		o.issues, err = openSyncIssues(o.state)
		if err != nil {
			return err
		}
	}

	defer func() { result = errors.Join(result, o.issues.checkpoint()) }()
	s := &syncer{o: o, b: b, j: j, forbid: map[string]bool{}, failedCp: map[string]bool{}, copied: map[string]bool{}, started: time.Now()}
	if err := s.preserveWindowRetryProgress(); err != nil {
		return err
	}
	if err := s.restorePhase(); err != nil {
		return fmt.Errorf("resume phase: %w", err)
	}
	s.redo = newSyncRedoGuard(j)
	s.sum.phases = map[string]*syncPhase{}
	if err := s.prepareVerificationRetry(); err != nil {
		return err
	}
	if err := s.initOwnership(); err != nil {
		return err
	}
	// Authorization is live configuration. A source rejected by a previous
	// invocation must be probed again after its allowlist has been repaired.
	event("sync_start", map[string]any{"to": o.to, "from": o.from, "ring": o.ring, "prefix": o.prefix, "state": o.state, "streams": o.streams, "verify_batch": o.batch, "max_rounds": o.maxRounds, "round_limit": o.roundLimit(), "rescan_all": o.rescanAll, "windows": o.windows.String(), "sample_every": o.sampleEvery, "resumed_rows": len(j.rows) - 1, "effective": o.policy})
	ctx, cancelFile := context.WithCancelCause(ctx)
	defer cancelFile(nil)
	progressCtx, stopProgress := context.WithCancel(ctx)
	go s.progressLoop(progressCtx, o.progress)
	var progressJournalErr error
	stopFile := startProgressFile(progressCtx, filepath.Join(o.state, "progress.json"), func() progressSnapshot {
		s.phaseMu.Lock()
		defer s.phaseMu.Unlock()
		j.mu.Lock()
		snap := j.progress
		snap.RedoNames = cloneRedoCounts(snap.RedoNames)
		snap.Cursors = cloneCursors(snap.Cursors)
		j.mu.Unlock()
		s.prog.mu.Lock()
		snap.Phase = s.prog.phase
		snap.NextVerifyNames = s.prog.nextVerifyNames
		accounted := s.prog.done.Load() + s.prog.copyActive
		snap.Done = max(accounted, s.prog.durableFloor)
		snap.Total = int64(s.prog.total)
		snap.FreshDoneAvailable = true
		snap.FreshDone = max(0, accounted-s.prog.replayed.Load())
		if s.prog.copyObserved {
			snap.FreshDone = s.prog.copyFresh
		}
		snap.Rate = 0
		if elapsed := time.Since(s.prog.started).Seconds(); elapsed > 0 {
			snap.Rate = float64(max(0, accounted-s.prog.replayed.Load())) / elapsed
			if s.prog.copyObserved {
				snap.Rate = float64(s.prog.copyFresh) / elapsed
			}
		}
		s.prog.mu.Unlock()
		if live, ok := s.b.(*syncLiveBackend); ok {
			snap.WaitingForStore = live.waiting.Load() > 0
			snap.WaitingForTargetHealth, snap.TargetHealthPauseSeconds = live.health.snapshot()
		}
		snap.Quarantined, snap.PolicySkipped = o.issues.counts()
		snap.Effective = o.policy
		phase, round := parseSyncPhase(snap.Phase, snap.Round)
		if phase != "verify" && phase != "fill" && phase != "complete" {
			round = 0
		}
		position := syncPhasePosition{Phase: phase, Round: round, Done: snap.Done, Total: snap.Total}
		if e := j.append(syncRow{Kind: "phase-progress", Position: &position}); e != nil {
			if progressJournalErr == nil {
				progressJournalErr = fmt.Errorf("journal progress before publish: %w", e)
				event("sync_phase_checkpoint_failed", map[string]any{"level": "ERROR", "error": e.Error()})
				cancelFile(progressJournalErr)
			}
			j.mu.Lock()
			if p := j.phaseGuard; p != nil {
				snap.Phase, snap.Round, snap.Done, snap.Total = p.Phase, p.Round, p.Done, p.Total
				if p.Phase == "verify" {
					snap.Phase = fmt.Sprintf("verify round %d", p.Round)
				}
			}
			snap.PhaseRegressions = j.progress.PhaseRegressions
			j.mu.Unlock()
		}
		return snap
	}, cancelFile)
	err = s.run(ctx)
	if cause := context.Cause(ctx); cause != nil && err != nil {
		err = cause
	}
	stopProgress()
	if err == nil && !o.noCopy {
		err = s.quarantineResiduals("verify")
	}
	err = errors.Join(err, o.issues.checkpoint())
	j.mu.Lock()
	j.progress.Quarantined, j.progress.PolicySkipped = o.issues.counts()
	j.mu.Unlock()
	progressErr := err
	if n, _ := o.issues.counts(); progressErr == nil && n > 0 {
		progressErr = ErrSyncQuarantined
	}
	if err == nil && len(s.sum.residual) > 0 {
		failedNames := s.reportRepairFailures()
		j.mu.Lock()
		j.progress.RepairFailedNames = int64(failedNames)
		j.mu.Unlock()
		progressErr = ErrSyncResidual
		var names []string
		for _, r := range s.sum.residual {
			names = append(names, r.Name)
		}
		if jerr := j.append(syncRow{Kind: "verification-incomplete", Round: s.verificationBase + s.sum.Rounds, Selected: names, SelectionComplete: true}); jerr != nil {
			err = errors.Join(err, jerr)
		}
	}
	if perr := stopFile(progressErr); perr != nil {
		err = errors.Join(err, perr)
	}
	err = errors.Join(err, progressJournalErr)
	if err == nil && len(s.sum.residual) == 0 && !o.noVerify {
		err = s.savePhase("complete", max(s.resume.Round, s.sum.Rounds+s.verificationBase), int(max(s.prog.done.Load(), s.prog.durableFloor)), s.prog.total)
	}
	s.summary(err)
	if err != nil {
		return err
	}
	if s.sum.Residual > 0 {
		verdict := ErrSyncResidual
		if n, _ := o.issues.counts(); n > 0 {
			verdict = ErrSyncQuarantined
		}
		return fmt.Errorf("%w: %d residual (names or holes), see sync_done", verdict, s.sum.Residual)
	}
	if n, _ := o.issues.counts(); n > 0 {
		return ErrSyncQuarantined
	}
	return nil
}

// A terminal residual verdict ends a verification attempt. Its receipts must
// remain available for diagnosis, but cannot stand in for a later observation
// of the stores. Interrupted attempts retain their current round namespace.
func (s *syncer) prepareVerificationRetry() error {
	maxRound, pending, hasRetry := 0, false, false
	var terminal syncRow
	for _, r := range s.j.rows {
		if r.Kind == "verified" || r.Kind == "verification-incomplete" || r.Kind == "copied" && r.Phase == "fill" {
			maxRound = max(maxRound, r.Round)
		}
		switch r.Kind {
		case "verification-incomplete":
			pending = true
			terminal = r
		case "verification-retry":
			s.verificationBase, pending, hasRetry = r.Round, false, true
		}
	}
	// Older clients did not journal the terminal verdict. Import their final
	// residual progress snapshot once; subsequent retries are journal driven.
	if !hasRetry && !pending && maxRound > 0 {
		data, err := os.ReadFile(filepath.Join(s.o.state, "progress.json"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			var previous progressSnapshot
			if err := json.Unmarshal(data, &previous); err != nil {
				return fmt.Errorf("read previous sync progress: %w", err)
			}
			pending = previous.Status == "incomplete" && previous.Error == ErrSyncResidual.Error()
		}
	}
	if pending && maxRound > 0 {
		s.verificationBase = maxRound
		if err := s.j.append(syncRow{Kind: "verification-retry", Round: maxRound, Selected: terminal.Selected, SelectionComplete: terminal.SelectionComplete}); err != nil {
			return err
		}
		event("sync_verification_retry", map[string]any{"first_round": maxRound + 1})
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

func (s *syncer) endPhase(name string, names int) error {
	if err := s.o.issues.checkpoint(); err != nil {
		return err
	}
	p := s.phase(name)
	p.names += names
	p.seconds = time.Since(p.started).Seconds()
	fields := map[string]any{"phase": name, "names": p.names, "seconds": p.seconds}
	if p.seconds > 0 {
		fields["names_per_second"] = float64(p.names) / p.seconds
	}
	event("sync_phase", fields)
	return s.finishPhase()
}

func (s *syncer) run(ctx context.Context) error {
	if err := s.savedWindowSelectionFailure(); err != nil {
		return err
	}
	restored := s.restoreVerificationCheckpoint()
	if !restored {
		if err := s.discover(ctx); err != nil {
			return fmt.Errorf("discover: %w", err)
		}
		if !s.o.noCopy && !s.phaseBeforeResume("copy", 0) {
			if err := s.copyMissing(ctx); err != nil {
				return fmt.Errorf("copy missing: %w", err)
			}
		}
	}
	// New journal tail selections are new work even if an older prefix reached
	// verification. They must never be hidden by a completed phase/checkpoint.
	pending := s.hasPendingWindowSelections()
	if pending && s.phaseBeforeResume("window retry", 0) {
		event("sync_scope_changed", map[string]any{"reason": "unresolved window selections appended after completed phase", "prior_phase": s.resume.Phase})
		if err := s.startPhaseScope(syncPhasePosition{Phase: "window retry"}, "new pending selections after completed phase"); err != nil {
			return err
		}
	}
	if s.resume.Phase == "complete" {
		changed, err := s.reopenChangedScope(ctx)
		if err != nil || !changed {
			return err
		}
	}
	if !s.phaseBeforeResume("window retry", 0) || pending {
		if err := s.waitWindowSelections(ctx); err != nil {
			return fmt.Errorf("retry window selections: %w", err)
		}
	}
	if !restored {
		s.startVerificationCheckpoint()
	}

	if s.o.noVerify {
		event("phase_skipped", map[string]any{"phase": "verify", "readiness": false})
		return nil
	}
	err := s.verifyAndFill(ctx)
	if s.o.noCopy && s.sum.Missing > 0 {
		missing, e := s.readMissing()
		if e != nil {
			return errors.Join(err, e)
		}
		remaining := 0
		for _, m := range missing {
			if _, ok := s.expectedAbsences.Load(m.name); !ok {
				remaining++
			}
		}
		if remaining > 0 {
			return errors.Join(err, fmt.Errorf("%w: --no-copy leaves %d names from discovery unimported; use a fresh state for an independent audit", ErrSyncResidual, remaining))
		}
	}
	return err
}

func (s *syncer) namesFile(kind string, round int, source int, names []string) (string, error) {
	base := filepath.Join(s.o.state, fmt.Sprintf("%s-r%d-s%d", kind, round, source))
	sort.Strings(names)
	if !s.windowOnly() || s.legacyCopyReceipts {
		// A changed stream budget or precheck result can assign a different
		// name set to the same wave number. Keep matching partial journals,
		// but never replace their input file with a different copy's names.
		hash := sha256.New()
		for _, name := range names {
			fmt.Fprintln(hash, name)
		}
		file, err := os.Open(base + ".jsonl")
		if err == nil {
			var header copyRow
			err = json.NewDecoder(file).Decode(&header)
			file.Close()
			if err != nil {
				return "", fmt.Errorf("read copy journal header: %w", err)
			}
			var identity map[string]string
			if err := json.Unmarshal([]byte(header.Identity), &identity); err != nil {
				return "", fmt.Errorf("read copy journal identity: %w", err)
			}
			digest := fmt.Sprintf("%x", hash.Sum(nil))
			if identity["names_sha256"] == digest {
				// Existing input may have been overwritten by an old client.
				// Copy selects a separate authenticated input in that case.
				return base + ".jsonl", nil
			}
			base += "-" + digest
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
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
	var repair string
	if phase == "fill" {
		var err error
		repair, err = s.fillIdentity(round, src, names)
		if err != nil {
			return syncCopyResult{}, err
		}
		round += s.verificationBase
	}
	source := s.o.from[src]
	original := append([]string(nil), names...)
	retained := map[string]bool{}
	if s.owner != nil && phase == "fill" && !s.windowOnly() {
		// Keep the historical parent identity. If an old broad transport was
		// interrupted, carry its completed owned imports into the narrowed
		// selection before creating any new transport journal.
		journal, err := s.namesFile("fill-"+repair, round, src, append([]string(nil), original...))
		if err != nil {
			return syncCopyResult{}, err
		}
		pending, err := copyJournalFailures(journal, source, original)
		if err != nil {
			return syncCopyResult{}, err
		}
		for _, n := range original {
			if s.owner(n) {
				retained[n] = true
			}
		}
		for _, n := range pending {
			delete(retained, n)
		}
	}
	eligible := make([]string, 0, len(names))
	for _, n := range names {
		if s.owner != nil && !s.owner(n) || retained[n] {
			continue
		}
		skip, e := s.excludePolicy(n, phase)
		if e != nil {
			return syncCopyResult{}, e
		}
		if !skip {
			eligible = append(eligible, n)
		}
	}
	excluded, err := s.expectedAbsent(ctx, eligible)
	if err != nil {
		return syncCopyResult{}, err
	}
	var resolved []syncSkip
	var selected []string
	for _, n := range names {
		if s.owner != nil && !s.owner(n) {
			resolved = append(resolved, syncSkip{n, "outside_ring_ownership"})
			continue
		}
		if retained[n] {
			continue
		}
		skip, e := s.excludePolicy(n, phase)
		if e != nil {
			return syncCopyResult{}, e
		}
		if skip {
			resolved = append(resolved, syncSkip{n, "quarantined_or_policy_skipped"})
		} else if excluded[n] {
			resolved = append(resolved, syncSkip{n, "expected_absent_target_expiration"})
		} else {
			selected = append(selected, n)
		}
	}
	names = selected
	copyPhase := phase
	if repair != "" {
		// Both the parent receipt and the child transport journal belong to
		// this observation. A new hole must not replay an older import.
		copyPhase += "-" + repair
	}
	if len(excluded) > 0 {
		data, _ := json.Marshal(names)
		hash := sha256.Sum256(data)
		copyPhase = fmt.Sprintf("%s-policy-%x", copyPhase, hash[:8])
	}
	if len(names) == 0 {
		if err := s.j.append(syncRow{Kind: "copied", Identity: repair, Phase: phase, Round: round, Source: source, First: original[0], Last: original[len(original)-1], Names: len(original), Copied: len(retained), Selected: original, Resolved: resolved, SelectionComplete: s.windowOnly()}); err != nil {
			return syncCopyResult{}, err
		}
		res := syncCopyResult{Copied: len(retained)}
		for _, r := range resolved {
			res.ExpectedAbsent = append(res.ExpectedAbsent, r.Name)
		}
		return res, nil
	}
	// The caller owns phase progress; a source batch must not reset its totals
	// or replace a stable phase with an immutable per-batch journal identity.
	journal, err := s.namesFile(copyPhase, round, src, names)
	if err != nil {
		return syncCopyResult{}, err
	}
	if err := s.redoImport(ctx, source, names, phase); err != nil {
		return syncCopyResult{}, err
	}
	event("sync_copy", map[string]any{"phase": phase, "round": round, "source": source, "names": len(names), "journal": journal})
	var res syncCopyResult
	if s.windowOnly() && !s.legacyCopyReceipts {
		backend, ok := s.b.(syncWindowBackend)
		if !ok {
			return res, errors.New("backend does not support window-only copy")
		}
		if phase == "fill" {
			ctx = syncWindowProgressContext(ctx, s.prog.windowCommitted)
			if live, ok := s.b.(interface{ beginWindowCopy() func() }); ok {
				defer live.beginWindowCopy()()
			}
		}
		res, err = backend.CopyWindow(ctx, source, s.o.to, names, journal, s.copyWindows())
	} else {
		if phase == "fill" {
			ctx = context.WithValue(ctx, syncCopyProgressKey{}, s.prog.copyCommitted)
		}
		res, err = s.b.Copy(ctx, source, s.o.to, names, journal)
	}
	if err != nil {
		return res, err
	}
	res.Copied += len(retained)
	if res.Forbidden {
		s.forbid[source] = true
		event("sync_source_forbidden", map[string]any{"level": "error", "source": source, "target": s.o.to, "hint": "the target transfer-sources allowlist does not list this source; continuing with the others"})
	}
	// Exact membership allows replay even when precheck omitted names inside
	// the bounds or a subsequent run uses different batch sizes.
	selected = original
	for _, item := range resolved {
		res.ExpectedAbsent = append(res.ExpectedAbsent, item.Name)
	}
	journalStarted := time.Now()
	invalidationReason := ""
	if phase == "fill" {
		invalidationReason = "verified_holes"
	}
	if err := s.j.append(syncRow{Kind: "copied", Identity: repair, Phase: phase, Round: round, Source: source, First: original[0], Last: original[len(original)-1], Names: len(original), InvalidationReason: invalidationReason, Copied: res.Copied, Failed: res.Failed, Forbidden: res.Forbidden, Short: short, Selected: selected, Resolved: resolved, SelectionComplete: s.windowOnly()}); err != nil {
		return res, err
	}
	s.rememberRedoImport(syncRow{Kind: "copied", Phase: phase, Round: round, Source: source, First: original[0], Last: original[len(original)-1], Names: len(original), Copied: res.Copied, Failed: res.Failed, Forbidden: res.Forbidden, Selected: selected, Resolved: resolved})
	event("sync_copy_journal", map[string]any{"phase": phase, "round": round, "source": source, "seconds": time.Since(journalStarted).Seconds()})
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
	if s.owner != nil && !s.owner(h.Name) {
		return
	}
	s.sum.residual = append(s.sum.residual, syncResidual{h, reason})
}

type syncKey struct{ name, source string }

// verifyAndFill runs verify/fill rounds. Round 1 verifies every name present
// on the target and on at least one source; later rounds only names filled
// or not yet indexed in the previous round, plus skipped names. Full-catalog
// convergence requires rescanAll. Entered legacy rounds >=2 switch to the
// preceding round's repair set without replaying the union.
func (s *syncer) verifyAndFill(ctx context.Context) (err error) {
	if err := s.prepareWindowRepairVerification(); err != nil {
		return err
	}
	set, scopedResume, err := s.windowResumeSet()
	if len(s.newScope) > 0 {
		set, scopedResume = s.newScope, true
	}
	if err != nil {
		return err
	}
	if s.frontier == nil {
		if err = s.openFrontier(ctx); err != nil {
			return err
		}
	}
	defer func() {
		if s.frontier != nil {
			err = errors.Join(err, s.frontier.file.Sync(), s.frontier.file.Close())
		}
	}()
	filledBefore := map[syncKey]bool{}
	failedAttempts := map[syncKey]int{}
	retryFailed := false
	// latest is each name's most recent verification: its holes, or nil when
	// clean; pending names were not indexed by the target's carbonserver.
	latest := map[string][]syncHole{}
	pending := map[string]bool{}
	skippedLast := map[string]string{} // name -> reason of its latest skip
	previousScore := int64(-1)
	firstRound := 1
	currentRound := s.resume.Round - s.verificationBase
	set, switched, err := s.prepareLegacyRepair(set, scopedResume, filledBefore)
	if err != nil {
		return err
	}
	if switched {
		scopedResume = true
		firstRound = currentRound
	}
	if limit := s.o.roundLimit(); !switched && s.resume.Phase == "verify" && limit > 0 && currentRound >= limit && currentRound > 1 {
		fullUnion, err := s.roundUsesUnion(currentRound, scopedResume)
		if err != nil {
			return err
		}
		if fullUnion {
			// This union replaces every earlier observation, and the limit
			// prevents another fill. Earlier fill history cannot affect the
			// verdict, so do not reconstruct ambiguous completed receipts.
			firstRound = currentRound
			event("sync_replay_skipped", map[string]any{"round": s.resume.Round, "round_limit": limit, "completed_rounds": currentRound - 1, "reason": "terminal union verification supersedes completed rounds"})
		}
	}
	for round := firstRound; ; round++ {
		s.sum.Rounds = round
		var names syncNameSource
		journalRound := round + s.verificationBase
		fullUnion, e := s.roundUsesUnion(round, scopedResume)
		if e != nil {
			return e
		}
		if !fullUnion {
			if names, err = s.setSource(set); err != nil {
				return err
			}
		} else {
			names = s.unionSource()
		}
		holes, unindexed, skipped, err := s.verifyRound(ctx, round, names)
		if err != nil {
			return fmt.Errorf("verify round %d: %w", round, err)
		}
		if fullUnion {
			latest = map[string][]syncHole{}
			pending = map[string]bool{}
			skippedLast = map[string]string{}
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
		// This is cumulative proof of this exact scope, including sparse repair
		// rounds. A later window import invalidates only its named members.
		if s.windowOnly() {
			dirty := map[string]bool{}
			for n := range latest {
				dirty[n] = true
			}
			for n := range pending {
				dirty[n] = true
			}
			for n := range skippedLast {
				dirty[n] = true
			}
			if err := s.j.append(syncRow{Kind: "verification-pass", Round: round + s.verificationBase, Selected: sortedKeys(dirty), SelectionComplete: true}); err != nil {
				return err
			}
		}
		// Fillable: a reachable source has the points and this pair was not
		// already filled without effect.
		bySource := map[int][]string{}
		seen := map[syncKey]bool{}
		next := map[string]bool{}
		fillable := 0
		for _, h := range holes {
			next[h.Name] = true
			k := syncKey{h.Name, h.Source}
			if s.forbid[h.Source] || failedAttempts[k] >= 3 || (s.windowOnly() && filledBefore[k]) {
				continue
			}
			if !seen[k] {
				// Transfers select a name once per source, even when several
				// windows or archives report holes for the same pair.
				fillable++
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
		verifiedNames := names.total
		for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "ownership-scope" && r.Round == journalRound }) {
			verifiedNames = r.Names
		}
		event("sync_round", map[string]any{"round": round, "verified": verifiedNames, "catalog_candidates": names.total, "full_union": fullUnion, "holes": len(holes), "fillable": fillable, "unindexed": len(unindexed), "skipped": len(skipped)})
		score := int64(len(unindexed) + len(skipped))
		for _, h := range holes {
			score += int64(max(1, h.Points))
		}
		stalled := previousScore >= 0 && score >= previousScore
		if s.frontier != nil && stalled {
			// A newly dirtied metric can replace the repaired residual at the
			// same score. It has not had a repair attempt and is not stalled.
			for _, h := range holes {
				k := syncKey{h.Name, h.Source}
				if !filledBefore[k] && failedAttempts[k] == 0 {
					stalled = false
					break
				}
			}
		}
		previousScore = score
		// Replay every already-entered fill before applying a new scheduling
		// limit. In particular, upgrading during round N must not stop while
		// replaying round 2 or abandon an interrupted fill.
		mustReplayFill := (s.resume.Phase == "verify" || s.resume.Phase == "fill") && phaseOrder("fill", journalRound) <= phaseOrder(s.resume.Phase, s.resume.Round)
		limit := s.o.roundLimit()
		if !mustReplayFill && (s.o.noCopy || (fillable == 0 && len(unindexed) == 0 && len(skipped) == 0) || (stalled && !retryFailed) || (limit > 0 && round >= limit)) {
			break
		}
		retryFailed = false
		s.phase("fill")
		if err := s.beginPhase("fill", round+s.verificationBase, fillable); err != nil {
			return err
		}
		// Count the planned convergence set without allocating another name map.
		// A later source failure may change it; this is an ETA, not a receipt.
		nextVerifyNames := int64(len(next))
		for name, hs := range latest {
			if next[name] {
				continue
			}
			for _, h := range hs {
				if seen[syncKey{name, h.Source}] {
					nextVerifyNames++
					break
				}
			}
		}
		if !s.phaseBeforeResume("fill", round+s.verificationBase) {
			s.prog.planVerification(nextVerifyNames)
		}
		work := 0
		for src := range s.o.from {
			names := bySource[src]
			if len(names) == 0 {
				continue
			}
			var res syncCopyResult
			replayed := false
			repair, err := s.fillIdentity(round, src, names)
			if err != nil {
				return err
			}
			if rows := s.j.find(func(r syncRow) bool {
				return r.Kind == "copied" && (!r.Forbidden || s.phaseBeforeResume("fill", round+s.verificationBase)) && r.Phase == "fill" && r.Round == round+s.verificationBase && r.Source == s.o.from[src] && r.Identity == repair
			}); len(rows) > 0 {
				replayed = true
				res = syncCopyResult{Copied: rows[0].Copied, Failed: rows[0].Failed, Forbidden: rows[0].Forbidden}
				for _, item := range rows[0].Resolved {
					res.ExpectedAbsent = append(res.ExpectedAbsent, item.Name)
				}
			} else {
				if s.phaseBeforeResume("fill", round+s.verificationBase) {
					return fmt.Errorf("resume: completed fill round %d lacks receipt for %s", round+s.verificationBase, s.o.from[src])
				}
				res, err = s.copyFrom(ctx, "fill", round, src, names)
				if err != nil {
					return err
				}
				work += len(names)
			}
			if res.Forbidden {
				retryFailed = true
				for _, n := range names {
					next[n] = true
				}
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
			s.sum.Filled += len(names) - len(res.Failed) - len(res.ExpectedAbsent)
			if !s.phaseBeforeResume("fill", round+s.verificationBase) {
				if replayed {
					s.prog.replayed.Add(int64(len(names)))
				}
				s.prog.finishCopy(len(names))
			}
		}
		if !s.phaseBeforeResume("fill", round+s.verificationBase) {
			if err := s.endPhase("fill", work); err != nil {
				return err
			}
		}
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
			if limit := s.o.roundLimit(); limit > 0 && s.sum.Rounds >= limit {
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
// workers. Completed batches retain their observations. Entered legacy union
// rounds adopt durable traversal ranges; modern rounds retain proof validation.
func (s *syncer) verifyRound(ctx context.Context, round int, set syncNameSource) ([]syncHole, []string, []syncSkip, error) {
	journalRound := round + s.verificationBase
	if s.phaseBeforeResume("verify", journalRound) && !(journalRound == s.resume.Round && s.adoptingLegacyRound()) {
		if s.owner != nil && !s.o.restartWindow && !s.windowOnly() && !s.o.rescanAll {
			if err := s.loadLegacyInvalidations(); err != nil {
				set.close()
				return nil, nil, nil, err
			}
			if len(s.legacyInvalidations) == 0 {
				return s.replayOwnedRound(ctx, journalRound, set)
			}
		}
		return s.replayVerifyRound(ctx, journalRound, set)
	}
	repairScope := s.legacyRepairScope(journalRound)
	var legacy *legacyRoundProgress
	var err error
	if repairScope != nil && !s.o.rescanAll {
		legacy, err = s.legacyRoundReceipts(journalRound, true, true)
		if err == nil {
			legacy = legacy.restrict(repairScope.Selected)
		}
	} else {
		legacy, err = s.legacyRoundProgress(journalRound)
	}
	if err != nil {
		set.close()
		return nil, nil, nil, err
	}
	if s.frontier == nil {
		if err := s.openFrontier(ctx); err != nil {
			set.close()
			return nil, nil, nil, err
		}
	}
	if err := s.loadLegacyInvalidations(); err != nil {
		set.close()
		return nil, nil, nil, err
	}
	// A ring-scope upgrade retains every completed observation in the current
	// round. Keep original traversal membership for old receipts; new sparse
	// receipts retain ring-filtered range membership. Destructive invalidations use
	// the existing selective revalidation path.
	if s.owner != nil && journalRound == s.resume.Round && !s.windowOnly() && !s.o.restartWindow && !s.o.rescanAll && len(s.legacyInvalidations) == 0 {
		legacy, err = s.legacyRoundReceipts(journalRound, true, false, true)
		if err == nil && repairScope != nil {
			legacy = legacy.restrict(repairScope.Selected)
		}
		if err != nil {
			set.close()
			return nil, nil, nil, err
		}
	}
	if err := s.narrowOwnedRound(ctx, journalRound, &set, legacy); err != nil {
		set.close()
		return nil, nil, nil, err
	}
	done := map[string]syncRow{}
	for _, r := range s.j.find(func(r syncRow) bool { return r.Kind == "verified" && r.Round == journalRound }) {
		done[r.First] = r
	}
	phase := s.phase("verify")
	var mu sync.Mutex
	var holes []syncHole
	var unindexed []string
	var skipped []syncSkip
	frontierReused := 0
	holeNames := map[string]bool{}
	// Receipts can overlap after a resumed run changes batch boundaries. Only
	// matching batches below prove modern progress. Legacy ranges are counted by
	// streaming membership once, so overlapping receipts cannot inflate progress.
	if err := s.beginPhase("verify", journalRound, set.total); err != nil {
		set.close()
		return nil, nil, nil, err
	}
	defer set.close()
	collect := func(r syncRow) {
		mu.Lock()
		defer mu.Unlock()
		s.prog.done.Add(int64(r.Names))
		s.prog.replayed.Add(int64(r.Reused))
		frontierReused += r.Reused
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
	legacy.observations(collect)
	type job struct {
		ids   []int64
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
					mask := jb.masks[i]
					if s.frontier != nil || s.o.restartWindow || len(s.legacyInvalidations) > 0 {
						// Check holders acquired after catalog discovery too.
						mask = (uint64(1) << uint(len(s.o.from))) - 1
					}
					s.masks.Store(n, mask)
				}
				workCtx := redoBatchContext(ctx, jb.names, jb.ids)
				var row syncRow
				var err error
				if s.frontier != nil {
					prior := done[jb.names[0]]
					row, err = s.verifyFrontierBatch(workCtx, journalRound, jb.names, jb.ids, prior)
				} else {
					var adopted bool
					row, adopted, err = s.adoptLegacyObservation(workCtx, journalRound, jb.names, jb.ids, done[jb.names[0]], nil, nil, "")
					if !adopted && err == nil {
						row, err = s.verifyBatch(workCtx, journalRound, jb.names)
					}
				}
				// A read's own budget can expire while the pass remains live.
				// Metadata/proof reads sit outside verifyBatch's split path;
				// retain these uncertain names as residuals instead of aborting
				// the rest of an otherwise finishable round.
				if errors.Is(err, context.DeadlineExceeded) && workCtx.Err() == nil {
					row = syncRow{Kind: "verified", Round: journalRound, First: jb.names[0], Last: jb.names[len(jb.names)-1], Names: len(jb.names)}
					for _, n := range jb.names {
						row.Skipped = append(row.Skipped, syncSkip{n, err.Error()})
					}
					event("sync_verification_read_timeout", map[string]any{"round": journalRound, "first": row.First, "names": row.Names, "error": err.Error()})
					err = nil
				}
				for _, n := range jb.names {
					s.masks.Delete(n)
				}
				if err == nil {
					if repairScope != nil && !s.o.rescanAll {
						row.Selected, row.SelectionComplete = jb.names, true
					}
					row.OwnershipScoped = s.owner != nil && !row.SelectionComplete
					row.Work = s.redo.cleanWork(workCtx, row)
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
			if ctx.Err() != nil {
				break feed
			}
			name, mask, err := set.next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				feedErr = err
				break feed
			}
			if legacy.contains(name, mask) {
				s.prog.done.Add(1)
				s.prog.replayed.Add(1)
				resumed++
				fed++
				// Keep new range receipts from crossing an older observation.
				if len(chunk.names) > 0 {
					break
				}
				continue
			}
			if !s.ownsSyncName(name, mask) {
				continue
			}
			skip, e := s.excludePolicy(name, "verify")
			if e != nil {
				feedErr = e
				break feed
			}
			if skip {
				continue
			}
			chunk.names = append(chunk.names, name)
			if set.id != nil {
				chunk.ids = append(chunk.ids, set.id())
			}
			chunk.masks = append(chunk.masks, mask)
		}
		if len(chunk.names) == 0 {
			break
		}
		fed += len(chunk.names)
		if r, ok := done[chunk.names[0]]; s.frontier == nil && len(s.legacyInvalidations) == 0 && ok && r.Names == len(chunk.names) && r.Last == chunk.names[len(chunk.names)-1] {
			s.prog.replayed.Add(int64(r.Names))
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
	if legacy != nil {
		event("sync_legacy_round_adopted", map[string]any{"round": journalRound, "names": resumed, "receipts": len(legacy.rows), "reason": "retain entered legacy round observations and traversal cursors"})
	}
	points := 0
	for _, h := range holes {
		points += h.Points
	}
	if round == 1 {
		s.sum.Verified = fed - frontierReused
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
	if err := s.endPhase("verify", 0); err != nil {
		return nil, nil, nil, err
	}
	event("sync_verified", map[string]any{"round": round, "names": fed - frontierReused, "candidate_names": fed, "proof_reused": frontierReused, "resumed": resumed, "hole_names": len(holeNames), "hole_points": points, "unindexed": len(unindexed), "skipped": len(skipped),
		"compared_by_digest": s.prog.digests.Load(), "read_point_by_point": s.prog.reads.Load()})
	holes, unindexed, skipped = s.ownedResults(journalRound, holes, unindexed, skipped)
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
	// Render returns an evenly spaced dense array. On the common aligned
	// path, scan it directly: testing every requested timestamp through finite
	// performs two divisions per point, including all the NaN padding.
	if step > 0 && src.Step == step && (from-src.Start)%step == 0 {
		first := max(int64(0), (from-src.Start)/step)
		for i := first; i < int64(len(src.Values)); i++ {
			ts := src.Start + i*step
			if ts >= until {
				break
			}
			v := src.Values[i]
			if math.IsNaN(v) || math.IsInf(v, 0) || dst.finite(ts) {
				continue
			}
			if h.Points == 0 {
				h.First = ts
			}
			h.Last = ts
			h.Points++
		}
		return h, h.Points > 0
	}
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
	points, pendingSelections := 0, 0
	var actual []syncResidual
	var pending []syncResidual
	for _, r := range s.sum.residual {
		if r.Reason == syncWindowSelectionReason {
			pendingSelections++
			pending = append(pending, r)
			continue
		}
		actual = append(actual, r)
		residualNames[r.Name] = true
		points += r.Points
	}
	s.sum.Residual = len(actual)
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
	residual := actual
	const shown = 1000
	if len(residual) > shown {
		residual = residual[:shown]
	}
	fields := map[string]any{
		"to": s.o.to, "discovered": s.sum.Discovered, "on_target_at_start": s.sum.OnTarget, "target_only_skipped": s.targetOnlySkipped(), "missing": s.sum.Missing,
		"short_skipped": s.sum.Short, "not_owned": s.sum.NotOwned, "copied": s.sum.Copied, "copy_failed": s.sum.CopyFailed,
		"verified": s.sum.Verified, "hole_names": s.sum.HoleNames, "hole_points": s.sum.HolePoints, "filled": s.sum.Filled,
		"rounds": s.sum.Rounds, "residual": s.sum.Residual, "residual_names": s.sum.ResidualNames, "residual_points": points,
		"pending_selections": pendingSelections, "residual_detail": residual, "forbidden_sources": forbidden, "phases": phases, "seconds": time.Since(s.started).Seconds(),
	}
	if live, ok := s.b.(*syncLiveBackend); ok {
		_, fields["target_health_pause_seconds"] = live.health.snapshot()
	}
	fields["quarantined"], fields["policy_skipped"] = s.o.issues.counts()
	if err != nil {
		fields["error"] = err.Error()
	}
	if err == nil && s.sum.Residual == 0 && pendingSelections == 0 {
		fields["verdict"] = "COMPLETE"
		if n, _ := s.o.issues.counts(); n > 0 {
			fields["verdict"] = "COMPLETE_EXCEPT"
			fields["level"] = "ERROR"
		}
	} else {
		fields["verdict"] = "INCOMPLETE"
		fields["level"] = "ERROR"
	}
	resolved := map[string]syncSkip{}
	for _, r := range s.j.find(func(r syncRow) bool { return len(r.Resolved) > 0 }) {
		for _, item := range r.Resolved {
			resolved[item.Name] = item
		}
	}
	fields["resolved_empty"] = len(resolved)
	expected := 0
	s.expectedAbsences.Range(func(_, _ any) bool { expected++; return true })
	fields["expected_absent"] = expected
	if err := atomicStateJSON(filepath.Join(s.o.state, "resolved.json"), resolved); err != nil {
		event("sync_resolution_error", map[string]any{"error": err.Error()})
	}
	if err := atomicStateJSON(filepath.Join(s.o.state, "pending-selections.json"), pending); err != nil {
		event("sync_resolution_error", map[string]any{"error": err.Error()})
	}
	event("sync_done", fields)
	if f, ferr := os.Create(filepath.Join(s.o.state, "residual.json")); ferr == nil {
		_ = json.NewEncoder(f).Encode(actual)
		_ = f.Close()
	}
}

func (o syncOptions) verifyConcurrency() int {
	if o.verifyStreams > 0 {
		return o.verifyStreams
	}
	return o.streams
}
