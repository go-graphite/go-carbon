package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
)

var ErrSyncNotReady = errors.New("sync NOT_READY; see ready.json")

type syncReadyOptions struct {
	scope        string
	noReady      bool
	carbonAPI    string
	metricsPort  int
	period       syncRanges
	gapsComplete bool
}
type readyPolicy struct {
	Aggregation  string        `json:"aggregation"`
	XFilesFactor float32       `json:"x_files_factor"`
	Archives     []syncArchive `json:"archives"`
}
type readyProcess struct {
	Start    float64 `json:"process_start_time"`
	Observed int64   `json:"observed"`
}
type syncReadyBackend interface {
	syncBackend
	Process(context.Context, string, int) (float64, error)
	Policies(context.Context, string, []string) (map[string]readyPolicy, error)
	Readback(context.Context, string, string, int64, int64) (syncSeries, error)
}
type readyCount struct {
	Names        int `json:"names_with_data"`
	Points       int `json:"points"`
	Missing      int `json:"missing"`
	StepMismatch int `json:"step_mismatch"`
}
type readyReport struct {
	Repair              *windowRepairCount `json:"repair,omitempty"`
	Scope               string             `json:"scope"`
	UnrecoverablePoints *int64             `json:"unrecoverable_points"`
	UnrecoverableReason string             `json:"unrecoverable_reason"`
	ProofState          string             `json:"proof_state"`
	Ring                string             `json:"ring"`
	NamesSHA256         string             `json:"names_sha256,omitempty"`
	Prefix              string             `json:"prefix,omitempty"`
	Period              syncRanges         `json:"period"`
	GapLedgerComplete   bool               `json:"gap_ledger_complete"`
	Exclusions          []string           `json:"exclusions"`

	Schema           int                     `json:"schema"`
	Verdict          string                  `json:"verdict"`
	Reasons          []string                `json:"reasons"`
	Target           string                  `json:"target"`
	Sources          []string                `json:"sources"`
	Windows          syncRanges              `json:"windows"`
	Processes        map[string]readyProcess `json:"processes"`
	Restarts         syncRanges              `json:"restart_windows"`
	Counts           map[string]readyCount   `json:"counts"`
	Sample           []string                `json:"sample"`
	PolicyNames      int                     `json:"policy_names"`
	PolicyMismatches int                     `json:"policy_mismatches"`
	Finished         int64                   `json:"finished_at"`
}

// runSyncReady owns the final phase and invalidates a prior attestation before
// any work. Recovery always uses fresh journals, including fresh copy filenames:
// pre-restart verification or transfer receipts cannot establish post-restart proof.
func runSyncReady(ctx context.Context, o *syncOptions, r syncReadyOptions, b syncReadyBackend, initial func() error) (err error) {
	if o.now == nil {
		o.now = time.Now
	}
	if err = os.MkdirAll(o.state, 0755); err != nil {
		return err
	}
	lock, err := lockResumeState(filepath.Join(o.state, "ready"))
	if err != nil {
		return err
	}
	defer lock.Close()
	if r.noReady {
		path := filepath.Join(o.state, "ready.json")
		if _, e := os.Stat(path); e == nil {
			if e = atomicStateJSON(path, readyReport{Schema: 1, Verdict: "NOT_READY", Reasons: []string{"READY disabled by --no-ready"}}); e != nil {
				return e
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		return initial()
	}
	report := readyReport{Ring: o.ring, NamesSHA256: o.namesHash, Prefix: o.prefix, Period: r.period, GapLedgerComplete: r.gapsComplete, Exclusions: []string{"archive-expiration boundary plus 2400s margin", "newest unsettled points outside explicit gap windows"}, Schema: 1, Verdict: "NOT_READY", Reasons: []string{"sync in progress"}, Target: o.to, Sources: o.from, Windows: append(syncRanges(nil), o.windows...), Counts: map[string]readyCount{}}
	report.Scope = r.scope
	if report.Scope == "" {
		report.Scope = "full-history"
	}
	report.UnrecoverableReason = "Points absent from every replica cannot be counted without an independent input inventory. Parity certifies available replica coverage only."
	if !o.includeShort {
		report.Exclusions = append(report.Exclusions, "names with all archives retaining at most 3 days")
	}
	path := filepath.Join(o.state, "ready.json")
	if err = atomicStateJSON(path, report); err != nil {
		return err
	}
	defer func() {
		if r.scope == "windows" {
			counts, e := collectWindowPointCounts(o.state)
			if e != nil {
				err = errors.Join(err, e)
			}
			report.Repair = &counts
		}
		if err != nil {
			report.Reasons = append(report.Reasons, err.Error())
		}
		report.Finished = o.now().Unix()
		status := "complete"
		if err != nil || len(report.Reasons) > 0 {
			status = "incomplete"
		}
		if e := readyProgress(o.state, status, err); e != nil {
			err = errors.Join(err, e)
			report.Reasons = append(report.Reasons, e.Error())
		}
		if len(report.Reasons) == 0 {
			report.Verdict = "READY"
		} else {
			report.Verdict = "NOT_READY"
			err = errors.Join(err, ErrSyncNotReady)
		}
		if e := atomicStateJSON(path, report); e != nil {
			err = errors.Join(err, e)
			return
		}
		event("sync_ready", map[string]any{"verdict": report.Verdict, "reasons": report.Reasons, "path": path})
	}()
	report.Reasons = nil
	previous := map[string]readyProcess{}
	snapshotPath := filepath.Join(o.state, "ready-processes.json")
	if data, e := os.ReadFile(snapshotPath); e == nil {
		if e = json.Unmarshal(data, &previous); e != nil {
			return fmt.Errorf("process snapshot: %w", e)
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	capture := func() (map[string]readyProcess, error) {
		out := map[string]readyProcess{}
		for _, store := range append(append([]string(nil), o.from...), o.to) {
			start, e := b.Process(ctx, store, r.metricsPort)
			if e != nil {
				return nil, fmt.Errorf("process identity %s: %w", store, e)
			}
			if start <= 0 || start > float64(o.now().Unix()+1) {
				return nil, fmt.Errorf("invalid process_start_time on %s", store)
			}
			out[store] = readyProcess{start, o.now().Unix()}
		}
		return out, nil
	}
	before, e := capture()
	if e != nil {
		return e
	}
	// Keep an earlier snapshot until recovery and final reads succeed. Interrupted
	// recovery is therefore retried with a new journal on the next invocation.
	_, oldJournalErr := os.Stat(filepath.Join(o.state, "sync.jsonl"))
	unbound := oldJournalErr == nil && len(previous) != len(before)
	for store := range before {
		p := previous[store]
		if p.Start <= 0 || p.Observed <= 0 || p.Observed > o.now().Unix() {
			unbound = oldJournalErr == nil
		}
	}
	if len(previous) == 0 {
		previous = before
		if e = atomicStateJSON(snapshotPath, before); e != nil {
			return e
		}
	}
	if e = initial(); e != nil {
		return e
	}
	validation := readyPrerequisites(*o, r)
	if o.noVerify || (len(o.windows) > 0 && !o.fullHistory && r.scope != "windows") {
		report.Reasons = append(report.Reasons, validation...)
		return nil
	}
	state := o.state
	if e = readyProgress(o.state, "running", nil); e != nil {
		return e
	}
	repairedParity := false
	for generation := 0; ; generation++ {
		after, e := capture()
		if e != nil {
			return e
		}
		gaps := readyRestartWindows(previous, after)
		freshProof := unbound
		unbound = false
		if len(gaps) > 0 || freshProof {
			for _, w := range gaps {
				if wait := w.Until - o.now().Unix(); wait > 0 {
					if e = sleepContext(ctx, time.Duration(wait)*time.Second); e != nil {
						return e
					}
				}
			}
			report.Restarts = append(report.Restarts, gaps...)
			report.Windows = append(report.Windows, gaps...)
			recovery := *o
			recovery.state = filepath.Join(o.state, fmt.Sprintf("ready-recovery-%d-%d", time.Now().UnixNano(), generation))
			recovery.windows = append(append(syncRanges(nil), o.windows...), report.Restarts...)
			recovery.settle = 0
			recovery.fullHistory = r.scope != "windows"
			recovery.sampleEvery = 1
			previous = after
			if e = runSync(ctx, recovery, b); e != nil {
				return fmt.Errorf("restart recovery: %w", e)
			}
			state = recovery.state
			continue
		}
		if len(validation) > 0 {
			report.Reasons = append(report.Reasons, validation...)
			report.Processes = after
			report.ProofState = state
			return nil
		}
		previous = after
		report.Reasons = nil
		report.Counts = map[string]readyCount{}
		report.PolicyNames = 0
		report.PolicyMismatches = 0
		if e = readyChecks(ctx, *o, r, b, state, &report); e != nil {
			return e
		}
		needsFill := false
		for key, count := range report.Counts {
			if !strings.HasPrefix(key, "readback ") && count.Missing > 0 {
				needsFill = true
			}
		}
		if needsFill && !o.noCopy && !repairedParity {
			recovery := *o
			recovery.state = filepath.Join(o.state, fmt.Sprintf("ready-fill-%d", time.Now().UnixNano()))
			recovery.fullHistory = r.scope != "windows"
			recovery.sampleEvery = 1
			recovery.settle = 0
			recovery.windows = append(append(syncRanges(nil), o.windows...), report.Restarts...)
			if e = runSync(ctx, recovery, b); e != nil {
				return fmt.Errorf("final parity fill: %w", e)
			}
			repairedParity = true
			state = recovery.state
			continue
		}
		final, e := capture()
		if e != nil {
			return e
		}
		if len(readyRestartWindows(previous, final)) > 0 {
			continue
		}
		report.Processes = final
		report.ProofState = state
		if len(report.Reasons) == 0 {
			if e = atomicStateJSON(snapshotPath, final); e != nil {
				return e
			}
		}
		return nil
	}
}

func readyPrerequisites(o syncOptions, r syncReadyOptions) []string {
	var out []string
	if o.noVerify {
		out = append(out, "verification disabled")
	}
	if len(o.windows) > 0 && !o.fullHistory && r.scope != "windows" {
		out = append(out, "full-history verification disabled")
	}
	if o.sampleEvery != 1 && len(o.windows) > 0 && r.scope != "windows" {
		out = append(out, "full-history verification sampled; READY requires --sample-every=1")
	}
	if r.carbonAPI == "" {
		out = append(out, "--carbonapi-url is required for read-back")
	} else if u, e := url.Parse(r.carbonAPI); e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		out = append(out, "--carbonapi-url must be an HTTP(S) base URL without credentials, query or fragment")
	}
	if !r.gapsComplete {
		out = append(out, "--gap-ledger-complete is required")
	}
	if r.scope == "windows" {
		if len(o.windows) == 0 || o.fullHistory {
			out = append(out, "window READY requires --window and --no-full-history")
		}
		if o.settle != 0 {
			out = append(out, "window READY requires --settle=0s")
		}
		for _, w := range o.windows {
			if w.Until > o.now().Unix() {
				out = append(out, "gap still open: "+w.Name)
			}
		}
		if len(r.period) != 0 {
			out = append(out, "window READY certifies --window only; omit --period")
		}
		return out
	}
	if len(r.period) != 1 {
		out = append(out, "one --period FROM:UNTIL is required")
		return out
	}
	p := r.period[0]
	if p.Until > o.now().Unix() {
		out = append(out, "block period is still open")
	}
	covered := len(o.windows) == 0
	for _, w := range o.windows {
		if w.Until > o.now().Unix() {
			out = append(out, "gap still open: "+w.Name)
		}
		if w.From <= p.From && w.Until >= p.Until {
			covered = true
		}
		if w.From < p.From || w.Until > p.Until {
			out = append(out, "period does not enclose gap: "+w.Name)
		}
	}
	if !covered {
		out = append(out, "sync windows do not cover block period")
	}
	return out
}

func readyRestartWindows(before, after map[string]readyProcess) syncRanges {
	var out syncRanges
	stores := make([]string, 0, len(after))
	for store := range after {
		stores = append(stores, store)
	}
	sort.Strings(stores)
	for i, store := range stores {
		a, b := before[store], after[store]
		if a.Start != 0 && a.Start != b.Start {
			// The observation interval encloses downtime even when several restarts
			// happened between polls; process start alone would miss the stopped time.
			out = append(out, syncRange{fmt.Sprintf("restart-%d-%d", i, b.Observed), max(0, a.Observed-1), b.Observed + 1})
		}
	}
	return out
}

// Policy parity covers every discovered eligible name, in bounded batches.
// Independent reads take the 100 lowest stable name hashes per source and
// for target read-back, so disjoint donor catalogs retain meaningful minima.
func readyChecks(ctx context.Context, o syncOptions, r syncReadyOptions, b syncReadyBackend, state string, report *readyReport) error {
	f, scan, err := openLines(filepath.Join(state, "union.tsv"))
	if err != nil {
		return err
	}
	defer f.Close()
	type candidate struct {
		name string
		hash uint64
	}
	var cohort []candidate
	cohorts := map[string][]candidate{}
	selectName := func(selected []candidate, name string) []candidate {
		h := fnv.New64a()
		_, _ = io.WriteString(h, name)
		c := candidate{name, h.Sum64()}
		less := func(a, b candidate) bool { return a.hash < b.hash || (a.hash == b.hash && a.name < b.name) }
		if len(selected) == 100 && !less(c, selected[99]) {
			return selected
		}
		selected = append(selected, c)
		if len(selected) >= 100 {
			sort.Slice(selected, func(i, j int) bool { return less(selected[i], selected[j]) })
			selected = selected[:100]
		}
		return selected
	}
	var batch []string
	checkPolicy := func() error {
		target, e := b.Policies(ctx, o.to, batch)
		if e != nil {
			return e
		}
		for _, source := range o.from {
			policies, e := b.Policies(ctx, source, batch)
			if e != nil {
				return e
			}
			for name, p := range policies {
				if !o.includeShort && readyShort(p.Archives) {
					continue
				}
				q, ok := target[name]
				if ok && (o.includeShort || !readyShort(q.Archives)) {
					cohorts[source] = selectName(cohorts[source], name)
				}
				if !ok || !readyPoliciesEqual(p, q) {
					report.PolicyMismatches++
					if len(report.Reasons) < 1000 {
						report.Reasons = append(report.Reasons, fmt.Sprintf("policy mismatch %s %s (aggregation/retention/xFilesFactor)", source, name))
					}
				}
			}
		}
		for _, name := range batch {
			p, ok := target[name]
			if !ok || (!o.includeShort && readyShort(p.Archives)) {
				continue
			}
			cohort = selectName(cohort, name)
		}
		report.PolicyNames += len(batch)
		if report.PolicyNames%10000 < len(batch) {
			event("sync_ready_progress", map[string]any{"phase": "ready policy", "names": report.PolicyNames, "policy_mismatches": report.PolicyMismatches})
		}
		batch = nil
		return nil
	}
	for scan.Scan() {
		name, mask, _, e := parseUnionLine(scan.Text())
		if e != nil {
			return e
		}
		if mask == 0 {
			continue
		}
		batch = append(batch, name)

		if len(batch) >= o.batch {
			if e = checkPolicy(); e != nil {
				return e
			}
		}
	}
	if err = scan.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		if err = checkPolicy(); err != nil {
			return err
		}
	}
	readbackSample := make([]string, 0, len(cohort))
	selected := map[string]bool{}
	for _, c := range cohort {
		readbackSample = append(readbackSample, c.name)
		selected[c.name] = true
	}
	sort.Strings(readbackSample)
	for _, sourceCohort := range cohorts {
		for _, c := range sourceCohort {
			selected[c.name] = true
		}
	}
	report.Sample = nil
	for name := range selected {
		report.Sample = append(report.Sample, name)
	}
	sort.Strings(report.Sample)
	if len(cohort) < 100 {
		report.Reasons = append(report.Reasons, fmt.Sprintf("sample minimum: %d names; require 100", len(cohort)))
	}
	windows := append(syncRanges(nil), r.period...)
	windows = append(windows, report.Windows...)
	// Restarts are closed only after their final observation second has elapsed.
	for _, w := range report.Restarts {
		if o.now().Unix() < w.Until {
			if err = sleepContext(ctx, time.Duration(w.Until-o.now().Unix())*time.Second); err != nil {
				return err
			}
		}
	}
	for _, source := range o.from {
		policies, e := b.Policies(ctx, source, report.Sample)
		if e != nil {
			return e
		}
		for _, name := range report.Sample {
			p, ok := policies[name]
			if !ok || (!o.includeShort && readyShort(p.Archives)) {
				continue
			}
			full := readyCount{}
			archives := p.Archives
			if r.scope == "windows" {
				archives = nil
			}
			for _, a := range archives {
				from, until := syncWindow(o.now().Unix(), a, o.settle)
				if from >= until {
					continue
				}
				row, e := readyCompare(ctx, b, source, o.to, name, from, until, a.Step)
				if e != nil {
					return e
				}
				full.Points += row.Points
				full.Missing += row.Missing
				full.StepMismatch += row.StepMismatch
			}
			if full.Points > 0 {
				full.Names = 1
			}
			if r.scope != "windows" {
				readyAdd(report, "history "+source, full)
			}
			for wi, w := range windows {
				row, e := readyCompare(ctx, b, source, o.to, name, w.From, w.Until, 0)
				if e != nil {
					return e
				}
				key := "period " + source
				if wi >= len(r.period) {
					key = fmt.Sprintf("window %d %s %s", wi, w.Name, source)
				}
				readyAdd(report, key, row)
			}
		}
		if r.scope != "windows" && report.Counts["period "+source].Names < 50 {
			report.Reasons = append(report.Reasons, "period sample minimum: "+source+" requires 50 names with data")
		}
		if r.scope != "windows" && report.Counts["history "+source].Names < 50 {
			report.Reasons = append(report.Reasons, "full-history sample minimum: "+source+" requires 50 names with data")
		}
	}
	for key, row := range report.Counts {
		if row.Missing > 0 || row.StepMismatch > 0 {
			report.Reasons = append(report.Reasons, fmt.Sprintf("parity %s: %+v", key, row))
		}
	}
	readWindows := syncRanges{{"86400", o.now().Unix() - 3600 - 86400, o.now().Unix() - 3600}, {"604800", o.now().Unix() - 3600 - 7*86400, o.now().Unix() - 3600}}
	if r.scope == "windows" {
		readWindows = report.Windows
	}
	for wi, w := range readWindows {
		key := "readback " + w.Name
		if r.scope == "windows" {
			key = fmt.Sprintf("readback window %d %s", wi, w.Name)
		}
		row := readyCount{}
		from, until := w.From, w.Until
		for _, name := range readbackSample {
			target, e := b.Fetch(ctx, o.to, []string{name}, from, until)
			if e != nil {
				return e
			}
			api, e := b.Readback(ctx, r.carbonAPI, name, from, until)
			if e != nil {
				return e
			}
			t := target[name]
			has := false
			for i, v := range t.Values {
				ts := t.Start + int64(i)*t.Step
				if !t.finite(ts) {
					continue
				}
				has = true
				row.Points++
				if !api.finite(ts) {
					row.Missing++
				} else if !readyValueEqual(v, api.Values[(ts-api.Start)/api.Step]) {
					row.StepMismatch++
				}
			}
			if has {
				row.Names++
				if t.Step != api.Step {
					row.StepMismatch++
				}
			}
		}
		report.Counts[key] = row
		if (r.scope != "windows" && row.Names < 50) || row.Missing > 0 || row.StepMismatch > 0 {
			report.Reasons = append(report.Reasons, fmt.Sprintf("%s minimum/parity failure: %+v", key, row))
		}
	}
	return nil
}
func readyShort(a []syncArchive) bool {
	if len(a) == 0 {
		return true
	}
	for _, v := range a {
		if v.Step*v.Points > syncShortRetention {
			return false
		}
	}
	return true
}
func readyPoliciesEqual(a, b readyPolicy) bool {
	sort.Slice(a.Archives, func(i, j int) bool { return a.Archives[i].Step < a.Archives[j].Step })
	sort.Slice(b.Archives, func(i, j int) bool { return b.Archives[i].Step < b.Archives[j].Step })
	return reflect.DeepEqual(a, b)
}
func readyAdd(r *readyReport, key string, a readyCount) {
	v := r.Counts[key]
	v.Names += a.Names
	v.Points += a.Points
	v.Missing += a.Missing
	v.StepMismatch += a.StepMismatch
	r.Counts[key] = v
}
func readyCompare(ctx context.Context, b syncBackend, source, target, name string, from, until, step int64) (readyCount, error) {
	s, e := b.Fetch(ctx, source, []string{name}, from, until)
	if e != nil {
		return readyCount{}, e
	}
	t, e := b.Fetch(ctx, target, []string{name}, from, until)
	if e != nil {
		return readyCount{}, e
	}
	a, z := s[name], t[name]
	row := readyCount{}
	for i := range a.Values {
		ts := a.Start + int64(i)*a.Step
		if ts < from || ts >= until || !a.finite(ts) {
			continue
		}
		row.Points++
		if !z.finite(ts) {
			row.Missing++
		}
	}
	if row.Points > 0 {
		row.Names = 1
		if a.Step != z.Step || (step > 0 && a.Step != step) {
			row.StepMismatch = 1
		}
	}
	return row, nil
}

func readyProgress(state, status string, checkErr error) error {
	path := filepath.Join(state, "progress.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var progress progressSnapshot
	if err = json.Unmarshal(data, &progress); err != nil {
		return err
	}
	progress.Phase = "ready"
	progress.Status = status
	progress.Updated = time.Now()
	if checkErr != nil {
		progress.Error = checkErr.Error()
	} else {
		progress.Error = ""
	}
	return atomicStateJSON(path, progress)
}
