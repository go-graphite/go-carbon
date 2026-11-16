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
		if gap.Open {
			return copy, r, fmt.Errorf("READY gap still open: %s", gap.Name)
		}
		if seen[gap.Name] {
			return copy, r, fmt.Errorf("duplicate READY gap: %s", gap.Name)
		}
		seen[gap.Name] = true
		if err := proof.windows.Set(fmt.Sprintf("%s:%d:%d", gap.Name, gap.From, gap.Until)); err != nil {
			return copy, r, err
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
	// Preserve the copy's own discovery, repairs and exception decisions first.
	if err = initial(); err != nil && !errors.Is(err, ErrSyncQuarantined) {
		return err
	}
	proof, ready, err := loadReadyProof(*copy, r)
	if err != nil {
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
