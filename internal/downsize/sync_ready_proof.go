package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// A READY block is independent of the original copy identity. Its content and
// ownership inputs bind a separate exhaustive journal; changing them can never
// promote receipts from a narrower proof into a wider attestation.
func loadReadyProof(copy syncOptions, r syncReadyOptions) (syncOptions, syncReadyOptions, error) {
	data, err := os.ReadFile(r.proofBlock)
	if err != nil {
		return copy, r, err
	}
	var block syncBlock
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&block); err != nil {
		return copy, r, fmt.Errorf("READY block: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return copy, r, errors.New("READY block has trailing JSON")
	}
	if block.Target == "" || len(block.Donors) == 0 || block.CarbonAPIURL == "" {
		return copy, r, errors.New("READY block requires target, donors and carbonapi_url")
	}
	target, err := origin(block.Target)
	if err != nil || target != copy.to {
		return copy, r, errors.New("READY block target must equal the copy target")
	}
	proof := copy
	proof.from = append(append([]string(nil), block.Donors...), block.Peers...)
	proof.ring, proof.ringIdentity, proof.prefix = block.Ring, "", ""
	proof.namesFile, proof.namesHash = block.Names, ""
	proof.fullHistory, proof.sampleEvery, proof.includeShort = true, 1, true
	proof.restartWindow = false
	proof.windows = nil
	if err := proof.windows.Set(fmt.Sprintf("period:%d:%d", block.Period.From, block.Period.Until)); err != nil {
		return copy, r, err
	}
	seen := map[string]bool{"period": true}
	for _, gap := range block.Gaps {
		if gap.Name == "" || strings.ContainsAny(gap.Name, ": ,") {
			return copy, r, errors.New("invalid READY gap name")
		}
		if seen[gap.Name] {
			return copy, r, fmt.Errorf("duplicate READY gap: %s", gap.Name)
		}
		seen[gap.Name] = true
		if gap.From < 0 {
			return copy, r, fmt.Errorf("invalid READY gap start: %s", gap.Name)
		}
		if gap.Open {
			r.openGaps = append(r.openGaps, gap.Name)
			continue
		}
		if err := proof.windows.Set(fmt.Sprintf("%s:%d:%d", gap.Name, gap.From, gap.Until)); err != nil {
			return copy, r, err
		}
	}
	for _, id := range r.gapSelection {
		if !seen[id] || id == "period" {
			return copy, r, fmt.Errorf("unknown gapfill window: %s", id)
		}
	}
	if err := proof.normalize(); err != nil {
		return copy, r, err
	}
	if proof.namesFile != "" {
		proof.namesHash, err = readSyncNames(proof.namesFile)
		if err != nil {
			return copy, r, err
		}
	}
	// Include ownership bytes as well as paths in the durable scope key.
	hash := sha256.New()
	_, _ = hash.Write(data)
	_, _ = hash.Write([]byte(proof.namesHash))
	if proof.ring != "" {
		ring, err := os.ReadFile(proof.ring)
		if err != nil {
			return copy, r, err
		}
		_, _ = hash.Write(ring)
	}
	proof.state = filepath.Join(copy.state, "ready-proof-"+hex.EncodeToString(hash.Sum(nil)))
	r.proofBlock = ""
	r.proofInput = append(json.RawMessage(nil), data...)
	r.scope = "full-history"
	r.carbonAPI = block.CarbonAPIURL
	r.period = syncRanges{{"period", block.Period.From, block.Period.Until}}
	r.gapsComplete = block.GapLedgerComplete
	return proof, r, nil
}

func runSeparateReadyProof(ctx context.Context, copy *syncOptions, r syncReadyOptions, b syncReadyBackend, prepare, initial func() error) (err error) {
	if err = os.MkdirAll(copy.state, 0700); err != nil {
		return err
	}
	lock, err := lockResumeState(filepath.Join(copy.state, "ready"))
	if err != nil {
		return err
	}
	defer lock.Close()
	proofOptions, readyOptions, e := loadReadyProof(*copy, r)
	if e != nil {
		return e
	}
	if copy.issues == nil {
		copy.issues, e = openSyncIssues(copy.state)
		if e != nil {
			return e
		}
	}
	proofOptions.issues = copy.issues
	if _, e = os.Stat(filepath.Join(proofOptions.state, "ready.json")); e == nil {
		proofLock, e := lockResumeState(filepath.Join(proofOptions.state, "ready"))
		if e != nil {
			return e
		}
		completed, e := inspectReadyResume(ctx, proofOptions, readyOptions, b)
		if e != nil {
			proofLock.Close()
			return e
		}
		if completed.finished {
			defer proofLock.Close()
			data, e := os.ReadFile(filepath.Join(proofOptions.state, "ready.json"))
			if e != nil {
				return e
			}
			root, e := os.ReadFile(filepath.Join(copy.state, "ready.json"))
			if e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
			if !bytes.Equal(root, data) {
				if e = atomicSyncConfigFile(filepath.Join(copy.state, "ready.json"), data); e != nil {
					return e
				}
			}
			return publishReadyNoop(*copy, completed.report)
		}
		proofLock.Close()
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	root := filepath.Join(copy.state, "ready.json")
	report := readyReport{Schema: 1, Verdict: "NOT_READY", Reasons: []string{"independent READY proof in progress"}, Target: copy.to}
	if err = atomicStateJSON(root, report); err != nil {
		return err
	}
	defer func() {
		if err != nil && report.Verdict != "READY_EXCEPT" && report.Verdict != "READY" {
			report.Reasons = append(report.Reasons, err.Error())
		}
		if e := atomicStateJSON(root, report); e != nil {
			err = errors.Join(err, e)
		}
	}()
	if copy.issues == nil {
		copy.issues, err = openSyncIssues(copy.state)
		if err != nil {
			return err
		}
	}
	if prepare != nil {
		if err = prepare(); err != nil {
			return err
		}
	}
	proof, ready, err := loadReadyProof(*copy, r)
	if err != nil {
		return err
	}
	report.Sources, report.Windows = proof.from, proof.windows
	report.GapLedgerComplete = ready.gapsComplete
	report.Quarantined, report.PolicySkipped = copy.issues.counts()
	report.QuarantinePath = filepath.Join(copy.issues.state, "quarantine.json")
	report.PolicySkippedPath = filepath.Join(copy.issues.state, "policy-skipped.json")
	report.Reasons = nil
	for _, name := range ready.openGaps {
		report.Reasons = append(report.Reasons, "READY gap still open: "+name)
	}
	report.GapReceipts, err = fillReadyGaps(ctx, *copy, proof, ready.gapSelection, b)
	if !ready.gapsComplete {
		report.Reasons = append(report.Reasons, "--gap-ledger-complete is required")
	}
	if err != nil || len(ready.openGaps) > 0 || !ready.gapsComplete {
		return errors.Join(err, ErrSyncNotReady)
	}
	// Closed-window work never invokes the old copy or exhaustive proof while
	// other windows are open. Final READY retains both existing proof gates.
	if err = initial(); err != nil && !errors.Is(err, ErrSyncQuarantined) {
		return err
	}
	if err = os.MkdirAll(proof.state, 0700); err != nil {
		return err
	}
	if copy.policy != nil {
		policy := *copy.policy
		policy.Engine, policy.Scope, policy.SampleEvery = "metric", "windows+full-history", 1
		policy.Windows = proof.windows.String()
		proof.policy = &policy
	}
	if err = atomicSyncConfigFile(filepath.Join(proof.state, "ready-scope.json"), ready.proofInput); err != nil {
		return err
	}
	// Keep an explicit durable scope receipt beside the independently bound journal.
	scope := struct {
		Block      string `json:"block"`
		CopyState  string `json:"copy_state"`
		ProofState string `json:"proof_state"`
	}{r.proofBlock, copy.state, proof.state}
	if err = atomicStateJSON(filepath.Join(copy.state, "ready-proof.json"), scope); err != nil {
		return err
	}
	err = runSyncReadyPrepared(ctx, &proof, ready, b, nil, func() error { return runSync(ctx, proof, b) })
	data, readErr := os.ReadFile(filepath.Join(proof.state, "ready.json"))
	if readErr != nil {
		return errors.Join(err, readErr)
	}
	if readErr = json.Unmarshal(data, &report); readErr != nil {
		return errors.Join(err, readErr)
	}
	if report.ProofState == "" {
		report.ProofState = proof.state
	}
	return err
}
