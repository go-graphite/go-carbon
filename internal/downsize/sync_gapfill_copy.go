package downsize

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

// Copy all holders' committed native points in this window. This intentionally
// does not enter sync's full-catalog verify/fill loop: retained-archive samples
// certify a window receipt; exhaustive all-donor proof is a later READY gate.
// Native range receipts also protect an interrupted, partially imported batch.
func runGapfillWindow(ctx context.Context, o syncOptions, b syncBackend) error {
	native, ok := b.(syncWindowBackend)
	if !ok {
		return errors.New("gap fill requires native window copying")
	}
	j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), mustGapfillSyncIdentity(o), syncCheckpointOptions(o))
	if err != nil {
		return err
	}
	defer j.close()
	if live, ok := b.(interface{ beginWindowCopy() func() }); ok {
		defer live.beginWindowCopy()()
	}
	completed := map[syncWindowSelectionKey]syncRow{}
	var total int64
	for _, row := range j.rows {
		if row.Kind == "discovered" {
			total = int64(row.Names)
		}
		if row.Kind == "copied" && row.Phase == "gapfill" {
			completed[syncWindowSelectionKey{row.Round, row.Source}] = row
		}
	}
	f, scan, err := openLines(filepath.Join(o.state, "union.tsv"))
	if err != nil {
		return err
	}
	defer f.Close()
	group, scanned, fresh := 0, int64(0), int64(0)
	var names []string
	var masks []uint64
	flush := func() error {
		if len(names) == 0 {
			return nil
		}
		group++
		changed := false
		for src, source := range o.from {
			var selected []string
			for i, name := range names {
				if masks[i]&(uint64(1)<<uint(src)) != 0 {
					selected = append(selected, name)
				}
			}
			if len(selected) == 0 {
				continue
			}
			identity, err := gapfillKey(selected)
			if err != nil {
				return err
			}
			key := syncWindowSelectionKey{group, source}
			if row, ok := completed[key]; ok {
				if row.Identity != identity || row.Copied != len(selected) || len(row.Failed) > 0 || row.Forbidden {
					return errors.New("gap batch receipt does not match inventory")
				}
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			path := filepath.Join(o.state, fmt.Sprintf("batch-%d-source-%d-%s", group, src, identity))
			result, err := native.CopyWindow(ctx, source, o.to, selected, path, o.windows)
			if err != nil {
				return fmt.Errorf("native gap fill %s: %w", source, err)
			}
			if result.Forbidden || len(result.Failed) > 0 || result.Copied != len(selected) {
				failed := result.Failed
				if len(failed) > 20 {
					failed = failed[:20]
				}
				return fmt.Errorf("native gap fill %s copied=%d expected=%d forbidden=%v failed=%q", source, result.Copied, len(selected), result.Forbidden, failed)
			}
			row := syncRow{Kind: "copied", Phase: "gapfill", Identity: identity, Source: source, Round: group, First: selected[0], Last: selected[len(selected)-1], Names: len(selected), Copied: result.Copied}
			if err := j.append(row); err != nil {
				return err
			}
			completed[key] = row
			changed = true
		}
		scanned += int64(len(names))
		if changed {
			fresh += int64(len(names))
		}
		event("sync_gapfill_progress", map[string]any{"window": o.windows[0], "names": scanned, "total": total, "fresh_names": fresh})
		if err := atomicStateJSON(filepath.Join(o.state, "progress.json"), progressSnapshot{Phase: "gap fill", Done: scanned, Total: total, Status: "running", FreshDoneAvailable: true, FreshDone: fresh}); err != nil {
			return err
		}
		names, masks = nil, nil
		return nil
	}
	for scan.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		name, mask, _, err := parseUnionLine(scan.Text())
		if err != nil {
			return err
		}
		names = append(names, name)
		masks = append(masks, mask)
		if len(names) >= o.batch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	return flush()
}
