package downsize

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCopyStateLegacyCost is an opt-in offline measurement. Input is read only;
// the real journal code writes only a private copy under the test's temp root.
func TestCopyStateLegacyCost(t *testing.T) {
	state := os.Getenv("STORESTATE_LEGACY_FIXTURE")
	if state == "" {
		t.Skip("set STORESTATE_LEGACY_FIXTURE to an offline captured state")
	}
	input, err := os.Open(filepath.Join(state, "sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	line, err := bufio.NewReader(input).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var header syncRow
	if err = json.Unmarshal(line, &header); err != nil {
		t.Fatal(err)
	}
	if header.Kind != "header" || header.Identity == "" {
		t.Fatal("invalid fixture header")
	}
	if _, err = input.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "sync.jsonl")
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(output, input); err != nil {
		t.Fatal(err)
	}
	if err = output.Close(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	j, err := openSyncJournal(path, header.Identity)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	t.Logf("MEASURE journal_replay_seconds=%.6f rows=%d", time.Since(start).Seconds(), len(j.rows))
	s := &syncer{j: j}
	start = time.Now()
	resolved := s.resolvedWindowSelections()
	t.Logf("MEASURE rebuild_resolution_index_seconds=%.6f groups=%d", time.Since(start).Seconds(), len(resolved))
	start = time.Now()
	b, err := os.ReadFile(filepath.Join(state, "progress.json"))
	if err != nil {
		t.Fatal(err)
	}
	var progress progressSnapshot
	if err = json.Unmarshal(b, &progress); err != nil {
		t.Fatal(err)
	}
	type status struct{ Selected, Skipped, Resolved, Visited int }
	statuses := map[syncWindowSelectionKey]status{}
	for _, r := range j.rows {
		k := syncWindowSelectionKey{r.Round, r.Source}
		v := statuses[k]
		switch r.Kind {
		case "window-selected":
			v.Selected += len(r.Selected)
			v.Skipped += len(r.Skipped)
		case "window-resolved":
			v.Resolved += len(r.Selected)
		case "window-retry-visited":
			v.Visited += r.Names
		default:
			continue
		}
		statuses[k] = v
	}
	t.Logf("MEASURE full_status_query_after_replay_seconds=%.6f phase=%q done=%d holes=%d quarantined=%d groups=%d", time.Since(start).Seconds(), progress.Phase, progress.Done, progress.Holes, progress.Quarantined, len(statuses))
	var selected []string
	var source string
	var group int
	for _, r := range j.rows {
		if r.Kind == "window-selected" && len(r.Skipped) > 0 {
			source = r.Source
			group = r.Round
			for _, skip := range r.Skipped {
				selected = append(selected, skip.Name)
				if len(selected) == 8192 {
					break
				}
			}
			if len(selected) == 8192 {
				break
			}
		}
	}
	if len(selected) == 0 {
		t.Fatal("fixture has no pending selection")
	}
	start = time.Now()
	err = j.append(syncRow{Kind: "window-resolved", Round: group, Source: source, Selected: selected})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MEASURE durable_retry_batch_seconds=%.6f names=%d", time.Since(start).Seconds(), len(selected))
	start = time.Now()
	_ = s.resolvedWindowSelections()
	t.Logf("MEASURE rebuild_after_retry_seconds=%.6f", time.Since(start).Seconds())
}
