//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"golang.org/x/sys/unix"
)

type copyOptions struct {
	RangeOrder        bool
	OriginalAllocated uint64
	Checkpoint        func(string) error
}

type rangeState struct {
	Version             int
	Source, Destination string
	Done                bool
	Sequence            uint64
	Active              *partsconvert.NameReceipt
}

type rangeMeasurement struct {
	Sequence                                                   uint64
	LegacyBefore, LegacyAfter, LegacySSTBefore, LegacySSTAfter uint64
	ConvertedAllocated                                         uint64
	Metrics, Points                                            uint64
}

// convertIDRanges leaves the lexical name cursor untouched. Its independent
// published receipts describe arbitrary verified coverage of this offline copy.
// No running mixed reader is installed by this tool.
func convertIDRanges(ctx context.Context, source func() *chunkstore.TransferSource, db *pebble.DB, reclaimer partsconvert.LegacyReclaimer, sourcePath, targetPath, sourceID, targetID string, maxMetrics int, maxBytes uint64, opts copyOptions) (copyConversionResult, error) {
	var out copyConversionResult
	directory := filepath.Join(targetPath, "ranges")
	if err := os.MkdirAll(directory, 0750); err != nil {
		return out, err
	}
	lock, err := os.OpenFile(filepath.Join(directory, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return out, err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return out, err
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	path := filepath.Join(directory, "state.json")
	state := rangeState{Version: 1, Source: sourceID, Destination: targetID}
	if b, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(b, &state); err != nil {
			return out, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return out, err
	}
	if state.Version != 1 || state.Source != sourceID || state.Destination != targetID || state.Done && state.Active != nil {
		return out, errors.New("ID-range resume identity mismatch")
	}
	// Catalog capture happens once per process. A crashed active plan retains its
	// own catalog/revision/bucket authority even if deletion already committed.
	var inventory partsconvert.LegacyPlan
	err = source().ConversionView(ctx, func(view pebble.Reader) error {
		var err error
		inventory, err = partsconvert.PlanLegacy(ctx, view, sourceID, []byte("legacy"), nil, nil, true, 1_000_000)
		return err
	})
	if err != nil {
		return out, err
	}
	if state.Done {
		if len(inventory.Metrics) != 0 {
			return out, errors.New("completed ID conversion still has legacy names")
		}
		return out, nil
	}
	slices.SortFunc(inventory.Metrics, func(a, b partsconvert.LegacyMetric) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	for i := 1; i < len(inventory.Metrics); i++ {
		if inventory.Metrics[i-1].ID == inventory.Metrics[i].ID {
			return out, errors.New("duplicate legacy ID in range inventory")
		}
	}
	position := 0
	for {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if state.Active == nil {
			if position == len(inventory.Metrics) {
				state.Done = true
				return out, saveRangeJSON(path, state)
			}
			plan := partsconvert.LegacyPlan{Source: sourceID, Group: []byte("legacy"), Final: true, Buckets: inventory.Buckets}
			var estimated uint64
			for position < len(inventory.Metrics) && len(plan.Metrics) < maxMetrics {
				metric := inventory.Metrics[position]
				snapshot, err := source().StoredSnapshot(ctx, string(metric.Name))
				if err != nil {
					return out, err
				}
				bound := storedOutputBound(metric.Name, snapshot)
				if bound > maxBytes-estimated {
					if len(plan.Metrics) == 0 {
						return out, errors.New("ID-range metric exceeds output budget")
					}
					break
				}
				plan.Metrics = append(plan.Metrics, metric)
				estimated += bound
				position++
			}
			slices.SortFunc(plan.Metrics, func(a, b partsconvert.LegacyMetric) int { return bytes.Compare(a.Name, b.Name) })
			state.Sequence++
			state.Active = &partsconvert.NameReceipt{Version: 1, Plan: plan, State: "building", Cursor: partsconvert.NameCursor{Version: 1, Source: sourceID, Destination: targetID}}
			if err := saveRangeJSON(path, state); err != nil {
				return out, err
			}
		}
		receipt := state.Active
		if receipt.Plan.Source != sourceID || receipt.Cursor.Source != sourceID || receipt.Cursor.Destination != targetID || receipt.Plan.Final != true || len(receipt.Plan.Lower) != 0 || len(receipt.Plan.Upper) != 0 {
			return out, errors.New("invalid active ID-range plan")
		}
		switch receipt.State {
		case "building", "published", "reclaimed":
		default:
			return out, errors.New("invalid ID-range phase")
		}
		// Never infer free bytes from a tombstone or an estimate of the original DB.
		if opts.OriginalAllocated > 0 {
			if err := admitCopyBatch(sourcePath, targetPath, opts.OriginalAllocated, maxBytes); err != nil {
				return out, err
			}
		}
		before, err := allocatedBytes(sourcePath)
		if err != nil {
			return out, err
		}
		sstBefore, err := sstBytes(sourcePath)
		if err != nil {
			return out, err
		}
		converter := partsconvert.Converter{Directory: filepath.Join(directory, fmt.Sprintf("batch-%08d", state.Sequence)), Source: copyArchive(source, receipt.Plan, maxBytes), Target: partsconvert.PebbleTarget{DB: db, DestinationID: targetID}, Space: &partsconvert.SpaceGuard{Path: targetPath}, ChunkBytes: 32 << 20, MaxTables: 100000}
		units := receipt.Plan.Units()
		if receipt.State == "building" {
			receipt.Units = nil
			for _, unit := range units {
				r, err := converter.Run(ctx, unit)
				if err != nil {
					return out, err
				}
				receipt.Units = append(receipt.Units, r)
			}
			receipt.State = "published"
			if err := saveRangeJSON(path, state); err != nil {
				return out, err
			}
		}
		if opts.Checkpoint != nil {
			if err := opts.Checkpoint("id-published"); err != nil {
				return out, err
			}
		}
		if len(receipt.Units) != len(units) {
			return out, errors.New("ID-range coverage is incomplete")
		}
		for i, r := range receipt.Units {
			if r.Unit != units[i] || r.State != "ready" || r.Source != sourceID || r.Destination != targetID {
				return out, errors.New("invalid ID-range unit receipt")
			}
			if err := converter.Target.Verify(ctx, r.Tables); err != nil {
				return out, err
			}
		}
		if receipt.State == "published" {
			if err := reclaimer.Reclaim(ctx, *receipt); err != nil {
				return out, err
			}
			receipt.State = "reclaimed"
			if err := saveRangeJSON(path, state); err != nil {
				return out, err
			}
		}
		if opts.Checkpoint != nil {
			if err := opts.Checkpoint("id-reclaimed"); err != nil {
				return out, err
			}
		}
		// The durable target owns its links after full readback and reclamation.
		// Drop replay staging links, preserving every journal and receipt.
		for _, unit := range receipt.Units {
			for _, table := range unit.Tables {
				if filepath.Dir(filepath.Dir(table.Path)) != converter.Directory {
					return out, errors.New("invalid stage path in range receipt")
				}
				for _, suffix := range []string{"", ".ingest"} {
					if err := os.Remove(table.Path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
						return out, err
					}
				}
			}
		}
		after, err := allocatedBytes(sourcePath)
		if err != nil {
			return out, err
		}
		sstAfter, err := sstBytes(sourcePath)
		if err != nil {
			return out, err
		}
		target, err := allocatedBytes(targetPath)
		if err != nil {
			return out, err
		}
		measure := rangeMeasurement{Sequence: state.Sequence, LegacyBefore: before, LegacyAfter: after, LegacySSTBefore: sstBefore, LegacySSTAfter: sstAfter, ConvertedAllocated: target, Metrics: uint64(len(receipt.Plan.Metrics))}
		for _, u := range receipt.Units {
			out.Points += u.Points
			out.SSTBytes += u.OutputBytes
			measure.Points += u.Points
		}
		out.Metrics += measure.Metrics
		out.Batches++
		if err := saveRangeJSON(filepath.Join(directory, fmt.Sprintf("reclaimed-%08d.json", state.Sequence)), receipt); err != nil {
			return out, err
		}
		if err := saveRangeJSON(filepath.Join(directory, fmt.Sprintf("measurement-%08d.json", state.Sequence)), measure); err != nil {
			return out, err
		}
		data, _ := json.Marshal(measure)
		fmt.Fprintln(os.Stderr, "ID_RANGE_RECLAIM", string(data))
		// A recovered active batch can overlap the fresh inventory. Skip its exact
		// IDs, never a name prefix or an unconverted ID hole.
		completed := make(map[uint64]bool, len(receipt.Plan.Metrics))
		for _, m := range receipt.Plan.Metrics {
			completed[m.ID] = true
		}
		for position < len(inventory.Metrics) && completed[inventory.Metrics[position].ID] {
			position++
		}
		state.Active = nil
		if err := saveRangeJSON(path, state); err != nil {
			return out, err
		}
	}
}

func saveRangeJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func allocatedBytes(root string) (uint64, error) {
	var total uint64
	seen := make(map[[2]uint64]bool)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)
		key := [2]uint64{uint64(st.Dev), st.Ino}
		if !seen[key] {
			total += uint64(st.Blocks) * 512
			seen[key] = true
		}
		return nil
	})
	return total, err
}

func admitCopyBatch(source, target string, original, batch uint64) error {
	legacy, err := allocatedBytes(source)
	if err != nil {
		return err
	}
	converted, err := allocatedBytes(target)
	if err != nil {
		return err
	}
	ceiling := original + original/20
	if legacy > ceiling || converted > ceiling-legacy || batch > ceiling-legacy-converted {
		return fmt.Errorf("conversion disk admission: legacy=%d target=%d next_output_bound=%d ceiling=%d; original=%d", legacy, converted, batch, ceiling, original)
	}
	return nil
}
