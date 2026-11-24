package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// This is scheduling evidence, never a replacement for the mutation frontier.
// A terminal report is reusable only after the original scope, every holder's
// process identity and every non-excluded name's schema/mutation seal agree.
type readyResume struct {
	report              readyReport
	state               string
	completed, finished bool
	changed             []string
	work                []syncOptions
	windows             syncRanges
}

func readyScopeKey(o syncOptions, r syncReadyOptions) (string, error) {
	identity, err := syncIdentity(o)
	if err != nil {
		return "", err
	}
	var ring []byte
	if o.ring != "" {
		ring, err = os.ReadFile(o.ring)
		if err != nil {
			return "", err
		}
	}
	return gapfillKey([]any{identity, ring, r.scope, r.period, r.carbonAPI, r.gapsComplete, o.settle, o.noCopy, o.noVerify})
}

func inspectReadyResume(ctx context.Context, o syncOptions, r syncReadyOptions, b syncReadyBackend) (out readyResume, err error) {
	path := filepath.Join(o.state, "ready.json")
	if err = readReadyObservationJSON(path, &out.report); err != nil && !errors.Is(err, os.ErrNotExist) {
		return out, err
	}
	terminal := out.report.Verdict == "READY" || out.report.Verdict == "READY_EXCEPT"
	out.state, err = completedReadyObservation(o)
	if err != nil {
		return out, fmt.Errorf("%s: %w", filepath.Join(o.state, "ready-completed.json"), err)
	}
	if terminal && out.state == "" {
		out.state = out.report.ProofState
	}
	if out.state == "" {
		return out, nil
	}
	if out.state != o.state && filepath.Dir(out.state) != o.state {
		return out, fmt.Errorf("%s: invalid proof state %q", path, out.state)
	}
	// The root header remains the authority even if a later recovery completed.
	var header syncRow
	if err = readReadyObservationJSON(filepath.Join(o.state, "sync.jsonl"), &header); err != nil {
		return out, err
	}
	identity, err := syncIdentity(o)
	if err != nil {
		return out, err
	}
	if header.Kind != "header" || header.Identity != identity {
		return out, fmt.Errorf("%s record 1: donor/target/ring/window/catalog scope changed; prior work retained, use a separate state for the new scope", filepath.Join(o.state, "sync.jsonl"))
	}
	if terminal && out.report.DataScopeKey != "" {
		dataKey, e := readyDataScopeKey(o)
		if e != nil {
			return out, e
		}
		if dataKey != out.report.DataScopeKey {
			return out, fmt.Errorf("%s data_scope_key: ownership scope changed; prior receipts retained", path)
		}
	}
	if terminal {
		// Validate the process fence before querying old per-name proofs: a new
		// process opens only its restart interval through the existing recovery path.
		for _, store := range append(append([]string(nil), o.from...), o.to) {
			start, e := b.Process(ctx, store, r.metricsPort)
			if e != nil {
				return out, fmt.Errorf("%s processes[%s]: %w", path, store, e)
			}
			prior := out.report.Processes[store]
			if start <= 0 || prior.Start <= 0 {
				return out, fmt.Errorf("%s processes[%s]: invalid process fence", path, store)
			}
			if start != prior.Start {
				out.completed = true
				event("sync_ready_invalidated", map[string]any{"record": path + " processes[" + store + "]", "reason": "process fence changed; recover only new restart interval", "prior": prior.Start, "current": start})
				return out, nil
			}
		}
	}
	// Validate the original history frontier. Bounded restart journals do not
	// supersede the original history proof or broaden it to the current time.
	known, changed, err := readyFrontierChanges(ctx, o, b)
	if err != nil {
		return out, err
	}
	if terminal && !known {
		return out, fmt.Errorf("%s: terminal attestation lacks an authenticated verification frontier; prior verdict retained, refusing data replay", filepath.Join(o.state, "verification-scope.json"))
	}
	out.completed, out.changed = known && len(changed) == 0, changed
	// Every completed recovery contributes bounded proof. A later restart must
	// not let the root history frontier stand in for those additional windows.
	if terminal && known {
		entries, e := os.ReadDir(o.state)
		if e != nil {
			return out, e
		}
		for _, entry := range entries {
			if !entry.IsDir() || (!strings.HasPrefix(entry.Name(), "ready-recovery-") && !strings.HasPrefix(entry.Name(), "ready-fill-")) {
				continue
			}
			state := filepath.Join(o.state, entry.Name())
			var progress progressSnapshot
			if e = readReadyObservationJSON(filepath.Join(state, "progress.json"), &progress); errors.Is(e, os.ErrNotExist) {
				continue
			} else if e != nil {
				return out, e
			}
			if (progress.Status != "complete" || progress.Error != "") && out.report.ProofState != state {
				continue
			}
			scoped, e := readyStateOptions(o, state)
			if e != nil {
				return out, e
			}
			valid, names, e := readyFrontierChanges(ctx, scoped, b)
			if e != nil {
				return out, e
			}
			if !valid {
				out.completed = false
				continue
			}
			if len(names) > 0 {
				out.work = append(out.work, scoped)
				out.changed = append(out.changed, names...)
				out.windows = append(out.windows, scoped.windows...)
			}
		}
	}
	if !terminal || !out.completed || len(out.work) > 0 || len(readyPrerequisites(o, r)) > 0 {
		return out, nil
	}
	key, err := readyScopeKey(o, r)
	if err != nil {
		return out, err
	}
	compatible := out.report.ScopeKey == key
	if out.report.ScopeKey == "" {
		// Automatically adopt reports written by older executables. Their journal
		// and frontier authenticate the data scope; these fields bind READY inputs.
		scope := r.scope
		if scope == "" {
			scope = "full-history"
		}
		compatible = !r.inputsChanged && out.report.Target == o.to && reflect.DeepEqual(out.report.Sources, o.from) && out.report.Scope == scope && reflect.DeepEqual(out.report.Period, r.period) && out.report.GapLedgerComplete == r.gapsComplete && out.report.NamesSHA256 == o.namesHash && out.report.Prefix == o.prefix
	}
	q, p := o.issues.counts()
	if q != out.report.Quarantined || p != out.report.PolicySkipped {
		compatible = false
	}
	if !compatible {
		event("sync_ready_invalidated", map[string]any{"record": path, "reason": "READY inputs or exclusion ledger changed; retain completed verification"})
		return out, nil
	}
	// Close the process fence around metadata validation too.
	for store, prior := range out.report.Processes {
		start, e := b.Process(ctx, store, r.metricsPort)
		if e != nil {
			return out, e
		}
		if start != prior.Start {
			return out, nil
		}
	}
	out.finished = true
	return out, nil
}

func readyFrontierChanges(ctx context.Context, o syncOptions, b syncBackend) (bool, []string, error) {
	return readyFrontier(ctx, o, b, false)
}

func readyFrontier(ctx context.Context, o syncOptions, b syncBackend, seal bool) (bool, []string, error) {
	if _, ok := b.(syncProofBackend); !ok {
		return false, nil, nil
	}
	s := &syncer{o: o, b: b}
	if err := s.initOwnership(); err != nil {
		return false, nil, err
	}
	f := &syncFrontier{}
	path := filepath.Join(o.state, "verification-scope.json")
	if err := readReadyObservationJSON(path, &f.scope); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil, nil
		}
		return false, nil, err
	}
	identity, err := syncIdentity(o)
	if err != nil {
		return false, nil, err
	}
	if f.scope.Identity != identity || f.scope.Settle != o.settle {
		return false, nil, fmt.Errorf("%s: verification scope changed; prior receipts retained", path)
	}
	data, _ := json.Marshal(f.scope)
	f.key = string(data)
	frontierPath := filepath.Join(o.state, "verification-frontier.bin")
	if _, e := os.Stat(filepath.Join(o.state, "ready-frontier.bin")); e == nil {
		frontierPath = filepath.Join(o.state, "ready-frontier.bin")
	} else if !errors.Is(e, os.ErrNotExist) {
		return false, nil, e
	}
	if seal {
		f.file, err = os.CreateTemp(o.state, ".ready-frontier-*")
	} else {
		f.file, err = os.Open(frontierPath)
	}
	if err != nil {
		return false, nil, err
	}
	defer f.file.Close()
	if seal {
		defer os.Remove(f.file.Name())
	}
	s.frontier = f
	source := s.unionSource()
	defer source.close()
	var changed []string
	for {
		var names []string
		var ids []int64
		for len(names) < o.batch {
			name, mask, e := source.next()
			if errors.Is(e, io.EOF) {
				break
			}
			if e != nil {
				return false, nil, e
			}
			if !s.ownsSyncName(name, mask) || o.issues.excluded(name) {
				continue
			}
			names = append(names, name)
			ids = append(ids, source.id())
		}
		if len(names) == 0 {
			break
		}
		stamps, e := s.proofStamps(ctx, names)
		if e != nil {
			return false, nil, e
		}
		for i, name := range names {
			if stamps[i] == (syncProof{}) {
				return false, nil, fmt.Errorf("%s: unknown mutation fence for %s; refusing data replay", path, name)
			}
			if seal {
				record := f.record(name, stamps[i])
				if _, e = f.file.WriteAt(record[:], ids[i]*32); e != nil {
					return false, nil, e
				}
				continue
			}
			var saved [32]byte
			if _, e = f.file.ReadAt(saved[:], ids[i]*32); e != nil && !errors.Is(e, io.EOF) {
				return false, nil, e
			}
			if saved != f.record(name, stamps[i]) {
				changed = append(changed, name)
				event("sync_ready_invalidated", map[string]any{"record": f.file.Name(), "offset": ids[i] * 32, "metric": name, "reason": "schema or mutation fence changed"})
			}
		}
	}
	if seal {
		if err = f.file.Sync(); err != nil {
			return false, nil, err
		}
		if err = f.file.Close(); err != nil {
			return false, nil, err
		}
		if err = os.Rename(f.file.Name(), filepath.Join(o.state, "ready-frontier.bin")); err != nil {
			return false, nil, err
		}
	}
	return true, changed, nil
}

func publishReadyNoop(o syncOptions, report readyReport) error {
	status := "complete"
	if report.Verdict == "READY_EXCEPT" {
		status = "ready_except"
	}
	if err := readyProgress(o.state, status, nil); err != nil {
		return err
	}
	event("sync_ready", map[string]any{"verdict": report.Verdict, "path": filepath.Join(o.state, "ready.json"), "noop": true, "data_work": false, "quarantined": report.Quarantined})
	return nil
}

// Recovery journals intentionally differ only in their fixed window contract.
func readyStateOptions(root syncOptions, state string) (syncOptions, error) {
	var header syncRow
	if err := readReadyObservationJSON(filepath.Join(state, "sync.jsonl"), &header); err != nil {
		return root, err
	}
	var saved struct {
		Windows       string `json:"windows"`
		FullHistory   bool   `json:"full_history"`
		RestartWindow bool   `json:"restart_window"`
		SampleEvery   int    `json:"sample_every"`
	}
	if err := json.Unmarshal([]byte(header.Identity), &saved); err != nil {
		return root, err
	}
	o := root
	o.state = state
	o.windows = nil
	o.fullHistory = saved.FullHistory
	o.restartWindow = saved.RestartWindow
	o.sampleEvery = saved.SampleEvery
	o.settle = 0
	if err := o.windows.Set(saved.Windows); err != nil {
		return root, err
	}
	identity, err := syncIdentity(o)
	if err != nil {
		return root, err
	}
	if header.Kind != "header" || header.Identity != identity {
		return root, fmt.Errorf("%s record 1: recovery scope changed", filepath.Join(state, "sync.jsonl"))
	}
	return o, nil
}

func retainReadyAttestation(o syncOptions, report readyReport) (string, error) {
	if report.Verdict != "READY" && report.Verdict != "READY_EXCEPT" {
		return "", nil
	}
	key, err := gapfillKey(report)
	if err != nil {
		return "", err
	}
	path := filepath.Join(o.state, "ready-attestation-"+key+".json")
	return path, atomicStateJSON(path, report)
}

// A completed bounded recovery can change a holder's process-wide revision.
// Bind the finished attestation to current metadata without replaying historical
// data. Keep the original verification frontier as provenance; future selective
// repairs adopt this attested baseline instead of treating a new process as a
// mutation of every old name.
func sealReadyFrontiers(ctx context.Context, o syncOptions, b syncBackend, selected string, restarted bool) error {
	states := []syncOptions{o}
	entries, err := os.ReadDir(o.state)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || (!strings.HasPrefix(entry.Name(), "ready-recovery-") && !strings.HasPrefix(entry.Name(), "ready-fill-")) {
			continue
		}
		state := filepath.Join(o.state, entry.Name())
		if _, e := os.Stat(filepath.Join(state, "verification-scope.json")); errors.Is(e, os.ErrNotExist) {
			continue
		} else if e != nil {
			return e
		}
		var progress progressSnapshot
		if e := readReadyObservationJSON(filepath.Join(state, "progress.json"), &progress); e != nil {
			return e
		}
		if state != selected && progress.Status != "complete" && !strings.Contains(progress.Error, ErrSyncQuarantined.Error()) {
			continue
		}
		scoped, e := readyStateOptions(o, state)
		if e != nil {
			return e
		}
		states = append(states, scoped)
	}
	for _, scoped := range states {
		if !restarted {
			_, changed, e := readyFrontierChanges(ctx, scoped, b)
			if e != nil {
				return e
			}
			if len(changed) > 0 {
				return fmt.Errorf("%s: mutation fence changed during READY; retain verified work and resume the named scope", filepath.Join(scoped.state, "verification-frontier.bin"))
			}
		}
		if _, _, err := readyFrontier(ctx, scoped, b, true); err != nil {
			return err
		}
	}
	return nil
}
