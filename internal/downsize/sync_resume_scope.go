package downsize

import (
	"errors"
	"io"
	"sort"
)

// A terminal window retry invalidates residuals and later repairs, not every
// clean closed window. Recover the latest complete pass against the immutable
// catalog, then replay exact sparse membership. Never treat First/Last alone as
// proof that an omitted name participated in a later sparse verification batch.
func (s *syncer) windowResumeSet() ([]string, bool, error) {
	if !s.windowOnly() || s.verificationBase == 0 {
		return nil, false, nil
	}
	dirty := map[string]bool{}
	planned := false
	proofBoundary := -1
	for i, row := range s.j.rows {
		if row.Kind == "verification-retry" && row.Round == s.verificationBase && row.SelectionComplete {
			dirty = map[string]bool{}
			proofBoundary = i
			for _, n := range row.Selected {
				dirty[n] = true
			}
			planned = true
		}
	}
	if !planned {
		var rows []syncRow
		bounds := map[string]int{}
		for _, row := range s.j.rows {
			if row.Kind != "verified" || row.Round > s.verificationBase || row.Names <= 0 {
				continue
			}
			rows = append(rows, row)
			bounds[row.First], bounds[row.Last] = -1, -1
		}
		if len(rows) == 0 {
			return nil, false, errors.New("window verification checkpoint has no verified batches")
		}
		source := s.unionSource()
		defer source.close()
		total := 0
		for {
			name, _, err := source.next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, false, err
			}
			if _, ok := bounds[name]; ok {
				bounds[name] = total
			}
			total++
		}
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].Round != rows[j].Round {
				return rows[i].Round < rows[j].Round
			}
			return rows[i].First < rows[j].First
		})
		complete := 0
		covered := 0
		round := 0
		for i, row := range rows {
			if row.Round != round {
				round, covered = row.Round, 0
			}
			first, last := bounds[row.First], bounds[row.Last]
			if first >= 0 && last-first+1 == row.Names && first <= covered {
				covered = max(covered, last+1)
			}
			if (i+1 == len(rows) || rows[i+1].Round != round) && covered == total {
				complete = round
			}
		}
		if complete == 0 {
			return nil, false, errors.New("window verification checkpoint has no complete catalog pass")
		}
		for i, row := range s.j.rows {
			if row.Kind == "verified" && row.Round == complete {
				proofBoundary = i
			}
		}
		inputs := map[int]map[string]bool{}
		addInput := func(round int, names []string) {
			if inputs[round] == nil {
				inputs[round] = map[string]bool{}
			}
			for _, n := range names {
				inputs[round][n] = true
			}
		}
		for _, row := range s.j.rows {
			if row.Kind == "copied" && row.Phase == "fill" && len(row.Selected) == row.Names {
				addInput(row.Round+1, row.Selected)
			}
			if row.Kind == "verified" {
				addInput(row.Round+1, row.Unindexed)
				for _, k := range row.Skipped {
					addInput(row.Round+1, []string{k.Name})
				}
			}
		}
		round = -1
		var input []string
		for _, row := range rows {
			if row.Round < complete {
				continue
			}
			if round != row.Round {
				round = row.Round
				input = nil
				for n := range inputs[round] {
					input = append(input, n)
				}
				sort.Strings(input)
			}
			if row.Round > complete {
				start := sort.SearchStrings(input, row.First)
				if start+row.Names <= len(input) && input[start] == row.First && input[start+row.Names-1] == row.Last {
					for _, n := range input[start : start+row.Names] {
						delete(dirty, n)
					}
				}
			}
			for _, h := range row.Holes {
				dirty[h.Name] = true
			}
			for _, n := range row.Unindexed {
				dirty[n] = true
			}
			for _, k := range row.Skipped {
				dirty[k.Name] = true
			}
		}
	}
	// Window resolutions are outside verify/fill; a crash may follow the import
	// but precede its recheck. Keep these touched names even after selection is 0.
	for i, row := range s.j.rows {
		if i <= proofBoundary {
			continue
		}
		if row.Kind == "verified" && row.Round > s.verificationBase {
			// Older clients may already have begun a broad retry. Its new
			// hole evidence remains dirty even though that pass never finished.
			for _, h := range row.Holes {
				dirty[h.Name] = true
			}
			for _, n := range row.Unindexed {
				dirty[n] = true
			}
			for _, k := range row.Skipped {
				dirty[k.Name] = true
			}
		}
		if row.Kind == "window-resolved" {
			for _, n := range row.Selected {
				dirty[n] = true
			}
		}
	}
	for _, r := range s.sum.residual {
		dirty[r.Name] = true
	}
	names := make([]string, 0, len(dirty))
	for n := range dirty {
		names = append(names, n)
	}
	sort.Strings(names)
	if !planned {
		if err := s.j.append(syncRow{Kind: "verification-retry", Round: s.verificationBase, Selected: names, SelectionComplete: true}); err != nil {
			return nil, false, err
		}
	}
	return names, true, nil
}
