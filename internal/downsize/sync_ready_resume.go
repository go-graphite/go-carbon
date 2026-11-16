package downsize

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

// completedReadyObservation finds historical completion evidence for a blocked
// NOT_READY result. It deliberately does not authorize READY or reuse data proofs:
// deletes, expiration, donor changes and circular-slot overwrites still go through
// the normal mutation fences once the missing inputs have been supplied.
// Only small progress records and journal headers are read, never the catalog or
// the potentially multi-gigabyte journal. Partial recoveries remain untouched.
func completedReadyObservation(o syncOptions) (string, error) {
	identity, err := syncIdentity(o)
	if err != nil {
		return "", err
	}
	var expected map[string]any
	if err := json.Unmarshal([]byte(identity), &expected); err != nil {
		return "", err
	}
	entries, err := os.ReadDir(o.state)
	if err != nil {
		return "", err
	}
	states := []string{o.state}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "ready-recovery-") {
			states = append(states, filepath.Join(o.state, entry.Name()))
		}
	}
	var latest time.Time
	var completed string
	var saved readyObservation
	if err := readReadyObservationJSON(filepath.Join(o.state, "ready-completed.json"), &saved); err == nil {
		if saved.Identity != identity || filepath.Dir(saved.State) != o.state && saved.State != o.state {
			return "", errors.New("completed READY observation scope changed")
		}
		info, err := os.Stat(filepath.Join(saved.State, "sync.jsonl"))
		if err != nil {
			return "", err
		}
		if info.Size() == saved.Bytes && info.ModTime().Equal(saved.Modified) {
			completed, latest = saved.State, saved.Finished
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	for _, state := range states {
		var progress progressSnapshot
		if err := readReadyObservationJSON(filepath.Join(state, "progress.json"), &progress); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return "", err
		}
		if progress.Status != "complete" || !strings.HasPrefix(progress.Phase, "verify") || progress.Error != "" || progress.Updated.IsZero() || progress.Updated.Before(latest) {
			continue
		}
		var header syncRow
		if err := readReadyObservationJSON(filepath.Join(state, "sync.jsonl"), &header); err != nil {
			return "", err
		}
		if header.Kind != "header" {
			return "", fmt.Errorf("completed observation %s has no journal header", state)
		}
		var got map[string]any
		if err := json.Unmarshal([]byte(header.Identity), &got); err != nil {
			return "", err
		}
		if state == o.state {
			if header.Identity != identity {
				continue
			}
		} else {
			// Recovery scopes intentionally use fixed restart windows and sample1.
			// All immutable owner/donor/catalog/policy fields must still agree.
			if got["sample_every"] != float64(1) {
				continue
			}
			base := make(map[string]any, len(expected))
			for k, v := range expected {
				base[k] = v
			}
			for _, k := range []string{"windows", "sample_every", "full_history", "restart_window"} {
				delete(base, k)
				delete(got, k)
			}
			if !reflect.DeepEqual(got, base) {
				continue
			}
		}
		completed, latest = state, progress.Updated
	}
	return completed, nil
}

type readyObservation struct {
	Identity string    `json:"identity"`
	State    string    `json:"state"`
	Bytes    int64     `json:"journal_bytes"`
	Modified time.Time `json:"journal_modified"`
	Finished time.Time `json:"finished"`
}

func saveReadyObservation(o syncOptions, state string) error {
	var p progressSnapshot
	if err := readReadyObservationJSON(filepath.Join(state, "progress.json"), &p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if p.Status != "complete" || !strings.HasPrefix(p.Phase, "verify") || p.Error != "" {
		return nil
	}
	id, err := syncIdentity(o)
	if err != nil {
		return err
	}
	info, err := os.Stat(filepath.Join(state, "sync.jsonl"))
	if err != nil {
		return err
	}
	return atomicStateJSON(filepath.Join(o.state, "ready-completed.json"), readyObservation{id, state, info.Size(), info.ModTime(), p.Updated})
}

func readReadyObservationJSON(path string, value any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(io.LimitReader(f, 64<<10)).Decode(value)
}
