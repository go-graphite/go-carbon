package downsize

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// Data scope excludes operational budgets and READY-only inputs. Ring bytes,
// rather than its filename alone, bind ownership to the saved configuration.
func readyDataScopeKey(o syncOptions) (string, error) {
	id, err := syncIdentity(o)
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
	return gapfillKey([]any{id, ring})
}

// Changed immutable inputs never overwrite or replay the original journal.
// An appended window opens only its uncovered interval; other holder/ownership
// changes get an explicitly named independent scope beneath the original state.
func (c *syncCommand) selectReadyScope() (bool, error) {
	path := filepath.Join(c.sync.state, "sync.jsonl")
	var header syncRow
	if err := readReadyObservationJSON(path, &header); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	identity, err := syncIdentity(c.sync)
	if err != nil {
		return false, err
	}
	if header.Kind != "header" {
		return false, fmt.Errorf("%s record 1: missing header", path)
	}
	var saved savedSyncConfig
	if err := readReadyObservationJSON(filepath.Join(c.sync.state, "config.json"), &saved); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	key, err := readyDataScopeKey(c.sync)
	if err != nil {
		return false, err
	}
	c.ready.inputsChanged = saved.Flags["carbonapi-url"] != c.ready.carbonAPI
	ringMatches := saved.DataScopeKey == key
	if saved.DataScopeKey == "" {
		priorRing, e := os.ReadFile(saved.Flags["ring"])
		currentRing, f := os.ReadFile(c.sync.ring)
		ringMatches = (saved.Flags["ring"] == "" && c.sync.ring == "") || (e == nil && f == nil && bytes.Equal(priorRing, currentRing))
	}
	same := header.Identity == identity && ringMatches
	if same {
		// A repeated block command has its complete scope in the descriptor. It
		// adopts the journal without requiring an operator to rewrite the launcher.
		if !c.resuming && c.explicit["block"] {
			c.resuming = true
			return true, nil
		}
		return false, nil
	}
	// An ordinary existing --state remains protected against accidental reuse.
	if !c.resuming && !c.explicit["block"] {
		return false, nil
	}
	parent := c.sync.state
	var old, requested map[string]any
	if err = json.Unmarshal([]byte(header.Identity), &old); err != nil {
		return false, err
	}
	if err = json.Unmarshal([]byte(identity), &requested); err != nil {
		return false, err
	}
	oldWindows, _ := old["windows"].(string)
	delete(old, "windows")
	delete(requested, "windows")
	var prior syncRanges
	if err = prior.Set(oldWindows); err != nil {
		return false, err
	}
	priorReport := readyReport{}
	if err := readReadyObservationJSON(filepath.Join(parent, "ready.json"), &priorReport); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	terminal := priorReport.Verdict == "READY" || priorReport.Verdict == "READY_EXCEPT"
	reason := "donor, target, catalog or ownership scope changed"
	oldOptions := c.sync
	oldOptions.windows = prior
	oldKey, err := readyDataScopeKey(oldOptions)
	if err != nil {
		return false, err
	}
	ringUnchanged := saved.DataScopeKey == oldKey
	if saved.DataScopeKey == "" {
		priorRing, e := os.ReadFile(saved.Flags["ring"])
		currentRing, f := os.ReadFile(c.sync.ring)
		ringUnchanged = (c.sync.ring == "" && saved.Flags["ring"] == "") || (e == nil && f == nil && bytes.Equal(priorRing, currentRing))
	}
	if terminal && reflect.DeepEqual(old, requested) && ringUnchanged {
		delta := uncoveredReadyWindows(c.sync.windows, prior)
		if len(delta) == 0 {
			original, e := parseSyncCommand([]string{"--resume", parent})
			if e != nil {
				return false, e
			}
			*c = original
			event("sync_scope_retained", map[string]any{"record": path + " record 1", "reason": "requested windows already covered; validate prior scope", "state": parent})
			return true, nil
		}
		if len(delta) > 0 {
			c.sync.windows, c.sync.fullHistory, c.sync.settle = delta, false, 0
			c.sync.sampleEvery = 1
			c.config.Flags["sample-every"] = "1"
			c.config.Explicit["sample-every"] = true
			c.ready.scope, c.ready.period = "windows", nil
			c.config.Flags["window"] = delta.String()
			c.config.Flags["full-history"] = "false"
			c.config.Flags["no-full-history"] = "true"
			c.config.Flags["settle"] = "0s"
			c.config.Flags["ready-scope"] = "windows"
			c.config.Flags["period"] = ""
			reason = "new window intervals; prior completed windows retained"
		}
	}
	next := filepath.Join(parent, "scope-"+key)
	attestation, err := retainReadyAttestation(c.sync, priorReport)
	if err != nil {
		return false, err
	}
	if err = os.MkdirAll(next, 0700); err != nil {
		return false, err
	}
	scope := struct{ Record, Reason, Parent, Requested string }{path + " record 1", reason, attestation, identity}
	if err = atomicStateJSON(filepath.Join(next, "scope-parent.json"), scope); err != nil {
		return false, err
	}
	c.sync.state, c.copy.state = next, next
	c.legacyJournal = false
	c.config.CopyState = ""
	c.config.Flags["state"] = next
	_, err = os.Stat(filepath.Join(next, "config.json"))
	c.resuming = err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	event("sync_scope_changed", map[string]any{"record": path + " record 1", "reason": reason, "state": next, "windows": c.sync.windows, "prior_state_preserved": true})
	return true, nil
}

func uncoveredReadyWindows(windows, covered syncRanges) syncRanges {
	var out syncRanges
	for _, w := range windows {
		remaining := syncRanges{w}
		for _, old := range covered {
			var next syncRanges
			for _, part := range remaining {
				if old.Until <= part.From || old.From >= part.Until {
					next = append(next, part)
					continue
				}
				if part.From < old.From {
					next = append(next, syncRange{Name: part.Name, From: part.From, Until: old.From})
				}
				if part.Until > old.Until {
					next = append(next, syncRange{Name: part.Name, From: old.Until, Until: part.Until})
				}
			}
			remaining = next
		}
		out = append(out, remaining...)
	}
	return out
}
