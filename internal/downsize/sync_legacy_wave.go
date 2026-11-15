package downsize

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

func syncNamesBytes(names []string) []byte {
	return []byte(strings.Join(names, "\n") + "\n")
}

// legacyWaveNames authenticates membership against the ORIGINAL header. Old
// clients reused round numbers and overwrote .names when their budget changed.
// Neither a receipt's bounds nor a current file alone proves the original set.
func (s *syncer) legacyWaveNames(header copyRow, path string, src, group int, rows []syncRow, missing []syncMissing) ([]string, error) {
	var identity map[string]string
	if header.Kind != "header" {
		return nil, errors.New("missing header")
	}
	if err := json.Unmarshal([]byte(header.Identity), &identity); err != nil {
		return nil, err
	}
	for key := range identity {
		switch key {
		case "from", "to", "prefix", "ring_sha256", "names_sha256":
		default:
			return nil, errors.New("unknown identity field")
		}
	}
	if len(identity) != 5 || identity["from"] != s.o.from[src] || identity["to"] != s.o.to || identity["prefix"] != "" || identity["ring_sha256"] != "" || identity["names_sha256"] == "" {
		return nil, errors.New("identity mismatch")
	}
	matches := func(names []string) bool {
		return len(names) > 0 && fmt.Sprintf("%x", sha256.Sum256(syncNamesBytes(names))) == identity["names_sha256"]
	}
	f, scan, err := openLines(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var current []string
	if err == nil {
		for scan.Scan() {
			if n := strings.TrimSpace(scan.Text()); n != "" {
				current = append(current, n)
			}
		}
		err = scan.Err()
		f.Close()
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(current)
	if matches(current) {
		return current, nil
	}
	recovered := func(names []string) []string {
		event("sync_legacy_wave_recovered", map[string]any{"source": s.o.from[src], "round": group, "names": len(names), "names_sha256": identity["names_sha256"]})
		return names
	}
	// Only historical prechecks that precede this child header describe its
	// input. Later metadata changes must not remove names from an old wave.
	omitted := map[string]bool{}
	started, startErr := time.Parse(time.RFC3339Nano, header.Time)
	for _, row := range rows {
		checked, err := time.Parse(time.RFC3339Nano, row.Time)
		if row.Kind != "prechecked" || row.Source != s.o.from[src] || startErr != nil || err != nil || checked.After(started) {
			continue
		}
		for _, n := range row.Short {
			omitted[n] = true
		}
		for _, n := range row.Absent {
			omitted[n] = true
		}
	}
	for _, r := range rows {
		if r.Kind != "copied" || r.Source != s.o.from[src] || r.Phase != "missing-batch" || r.Round != group || r.Forbidden {
			continue
		}
		if matches(r.Selected) {
			return recovered(r.Selected), nil
		}
		// An enlarged replacement may still contain the exact older wave.
		// Precheck can have omitted absent names that remain in discovery.
		lo := sort.SearchStrings(current, r.First)
		hi := sort.Search(len(current), func(i int) bool { return current[i] > r.Last })
		if matches(current[lo:hi]) {
			return recovered(current[lo:hi]), nil
		}
		// The durable discovery inventory still contains completed names even
		// when a later retry removed them from its new wave. Bound it with the
		// parent receipt, then demand a full cryptographic membership match.
		first := sort.Search(len(missing), func(i int) bool { return missing[i].name >= r.First })
		var candidate []string
		short := make(map[string]bool, len(r.Short))
		for _, n := range r.Short {
			short[n] = true
		}
		for i := first; i < len(missing) && missing[i].name <= r.Last; i++ {
			m := missing[i]
			if m.mask&(1<<uint(src)) != 0 && !short[m.name] && !omitted[m.name] {
				candidate = append(candidate, m.name)
			}
		}
		if matches(candidate) {
			return recovered(candidate), nil
		}
	}
	return nil, errors.New("names/header SHA mismatch: cannot authenticate original membership from retained input or receipts and discovery inventory")
}

// Use a distinct immutable input if an old client replaced the sibling .names.
// The original journal and overwritten file both remain intact. Its original
// identity is still checked by the copy engine, and all done ranges replay.
func syncCopyInput(journal string, names []string) (string, error) {
	path := strings.TrimSuffix(journal, ".jsonl") + ".names"
	data := syncNamesBytes(names)
	current, err := os.ReadFile(path)
	if err == nil && bytes.Equal(current, data) {
		return path, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	path = fmt.Sprintf("%s.input-%x.names", journal, sha256.Sum256(data))
	if current, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(current, data) {
			return "", fmt.Errorf("copy input %s is corrupt", path)
		}
		return path, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := atomicSyncConfigFile(path, data); err != nil {
		return "", err
	}
	return path, nil
}
