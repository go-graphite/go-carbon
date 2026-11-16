package partsconvert

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// NameCursor's first three JSON fields match mixedread.Cursor. Extra fields
// fence the source/destination incarnation and bind the verified batch.
type NameCursor struct {
	Version     int    `json:"version"`
	Before      []byte `json:"before_base64,omitempty"`
	Done        bool   `json:"done,omitempty"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Batch       string `json:"batch"`
}

type NameReceipt struct {
	Version int
	Plan    LegacyPlan
	Cursor  NameCursor
	State   string
	Units   []Receipt
	// Batches preserve the original plan and unit identities during a bounded resume.
	Batches []NameReceipt `json:",omitempty"`
}

// NameConverter verifies all shards of a global name batch before publishing a
// single cursor. Its Converter must not publish or reclaim individual shards.
// Publish installs the frozen cursor/catalog/part generation idempotently.
// Reclaim must finish physical recovery, not merely submit tombstones.
type NameConverter struct {
	Directory string
	Converter *Converter
	Publish   func(context.Context, NameReceipt) error
	Reclaim   func(context.Context, NameReceipt) error
	// MaxOutputBytes bounds aggregate output of ALL participating shards.
	MaxOutputBytes uint64
	// Checkpoint optionally injects a failure for offline crash qualification.
	Checkpoint func(string) error
}

func (c *NameConverter) Run(ctx context.Context, plan LegacyPlan) (NameReceipt, error) {
	var r NameReceipt
	if err := plan.validate(); err != nil {
		return r, err
	}
	if c.Directory == "" || c.Converter == nil || c.Converter.Source == nil || c.Converter.Target == nil || c.Converter.Publisher != nil || c.Converter.Reclaimer != nil || c.Publish == nil || c.Reclaim == nil || c.MaxOutputBytes == 0 || plan.Source != c.Converter.Source.Identity() {
		return r, errors.New("invalid global name conversion configuration")
	}
	if err := makeDurableDirectory(c.Directory); err != nil {
		return r, err
	}
	unlock, err := lockDirectory(c.Directory)
	if err != nil {
		return r, err
	}
	defer unlock()
	// Only boundaries identify the journal. Altering a persisted plan then
	// fails identity validation rather than creating a new deletion authority.
	key, _ := json.Marshal(struct {
		Lower, Upper []byte
		Final        bool
	}{plan.Lower, plan.Upper, plan.Final})
	batchID := fmt.Sprintf("%x", sha256.Sum256(key))
	path := filepath.Join(c.Directory, "names-"+batchID+".json")
	err = readJSON(path, &r)
	newJournal := false
	if errors.Is(err, os.ErrNotExist) {
		r = NameReceipt{Version: 1, Plan: plan, State: "building", Cursor: NameCursor{Version: 1, Before: bytes.Clone(plan.Upper), Done: plan.Final, Source: plan.Source, Destination: c.Converter.Target.Identity(), Batch: batchID}}
		newJournal, err = true, nil
	}
	if err != nil {
		return r, err
	}
	wantPlan, _ := json.Marshal(plan)
	gotPlan, _ := json.Marshal(r.Plan)
	if r.Version != 1 || !bytes.Equal(wantPlan, gotPlan) || r.Cursor.Source != plan.Source || r.Cursor.Destination != c.Converter.Target.Identity() || r.Cursor.Batch != batchID || r.Cursor.Version != 1 || !bytes.Equal(r.Cursor.Before, plan.Upper) || r.Cursor.Done != plan.Final {
		return r, errors.New("name batch journal identity mismatch")
	}
	switch r.State {
	case "building", "splitting", "ready", "published", "reclaimed":
	default:
		return r, errors.New("invalid global name cursor state")
	}
	if r.State == "reclaimed" {
		for _, unit := range r.Units {
			if err := cleanStage(filepath.Join(c.Converter.Directory, unitName(unit.Unit))); err != nil {
				return r, err
			}
		}
		return r, clearActivePlan(c.Directory, plan)
	}
	if r.State == "splitting" {
		return c.runSplit(ctx, path, r)
	}
	var current NameCursor
	err = readJSON(filepath.Join(c.Directory, "name-cursor.json"), &current)
	if errors.Is(err, os.ErrNotExist) {
		if len(plan.Lower) != 0 {
			return r, errors.New("first global batch must begin at the first name")
		}
	} else if err != nil {
		return r, err
	} else if current.Source != plan.Source || current.Destination != r.Cursor.Destination || current.Version != 1 || current.Batch != batchID && (current.Done || !bytes.Equal(current.Before, plan.Lower)) {
		return r, errors.New("name cursor would skip or regress a batch")
	} else if current.Batch != batchID {
		// No new batch while the preceding one still owns unreclaimed bytes.
		var previous NameReceipt
		if err := readJSON(filepath.Join(c.Directory, "names-"+current.Batch+".json"), &previous); err != nil {
			return r, err
		}
		if previous.State != "reclaimed" {
			return r, errors.New("previous name batch still needs physical reclamation")
		}
	}
	if newJournal {
		var active LegacyPlan
		if err := readJSON(filepath.Join(c.Directory, "name-active.json"), &active); err == nil {
			a, _ := json.Marshal(active)
			if !bytes.Equal(a, wantPlan) {
				return r, errors.New("another name batch still owns the active conversion plan")
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return r, err
		}
		if err := writeJSON(filepath.Join(c.Directory, "name-active.json"), plan); err != nil {
			return r, err
		}
		if err := writeJSON(path, r); err != nil {
			return r, err
		}
	}
	checkpoint := func(stage string) error {
		if c.Checkpoint != nil {
			return c.Checkpoint(stage)
		}
		return nil
	}
	units := plan.Units()
	if r.State == "building" {
		var total uint64
		bounds := make(map[uint16]uint64)
		for _, u := range units {
			n, err := c.Converter.Source.Estimate(ctx, u)
			if err != nil {
				return r, err
			}
			if n > c.MaxOutputBytes {
				return r, errors.New("global name shard exceeds output budget")
			}
			bounds[u.VShard] = n
			if total > ^uint64(0)-n {
				return r, errors.New("global name estimate overflows")
			}
			total += n
		}
		if total > c.MaxOutputBytes {
			// Persist the complete frozen authority before doing any child work.
			// Charge full original shards: a shard shared by children is built
			// once, with its original token, before any of its names are deleted.
			for start := 0; start < len(plan.Metrics); {
				end, used := start, uint64(0)
				seen := make(map[uint16]bool)
				for end < len(plan.Metrics) {
					shard := partsformat.Route(partsformat.ID(plan.Metrics[end].Name))
					if !seen[shard] {
						if bounds[shard] > c.MaxOutputBytes-used {
							break
						}
						used += bounds[shard]
						seen[shard] = true
					}
					end++
				}
				child := plan
				child.Metrics = plan.Metrics[start:end]
				if start > 0 {
					child.Lower = bytes.Clone(plan.Metrics[start].Name)
				}
				if end < len(plan.Metrics) {
					child.Upper, child.Final = bytes.Clone(plan.Metrics[end].Name), false
				}
				cursor := r.Cursor
				cursor.Before, cursor.Done = bytes.Clone(child.Upper), child.Final
				r.Batches = append(r.Batches, NameReceipt{Version: 1, Plan: child, Cursor: cursor, State: "building"})
				start = end
			}
			r.State = "splitting"
			if err := writeJSON(path, r); err != nil {
				return r, err
			}
			return c.runSplit(ctx, path, r)
		}
		r.Units = nil
		// Do not mutate the caller's converter while installing the coverage
		// guard. Each unit retains the same journal/target/source identities.
		base := c.Converter
		worker := &Converter{Directory: base.Directory, Source: plannedSource{Source: base.Source, plan: plan}, Target: base.Target, Space: base.Space, ChunkBytes: base.ChunkBytes, MaxTables: base.MaxTables}
		for _, u := range units {
			receipt, err := worker.Run(ctx, u)
			if err != nil {
				return r, err
			}
			r.Units = append(r.Units, receipt)
		}
		r.State = "ready"
		if err = writeJSON(path, r); err != nil {
			return r, err
		}
	}
	if err = checkpoint("names-ready"); err != nil {
		return r, err
	}
	if len(r.Units) != len(units) {
		return r, errors.New("global name batch has incomplete shard receipts")
	}
	for i, u := range units {
		unit := r.Units[i]
		if unit.Unit != u || unit.Source != plan.Source || unit.Destination != r.Cursor.Destination || unit.State != "ready" {
			return r, errors.New("global name batch contains an invalid shard receipt")
		}
		// Recovery never trusts stale ready receipts over actual target data.
		if err = c.Converter.Target.Verify(ctx, unit.Tables); err != nil {
			return r, err
		}
	}
	if r.State == "ready" {
		if err = writeJSON(filepath.Join(c.Directory, "name-cursor.json"), r.Cursor); err != nil {
			return r, err
		}
		if err = checkpoint("cursor-durable"); err != nil {
			return r, err
		}
		if err = c.Publish(ctx, r); err != nil {
			return r, err
		}
		r.State = "published"
		if err = writeJSON(path, r); err != nil {
			return r, err
		}
	} else {
		// An owner process may have restarted since the durable publication.
		if err = c.Publish(ctx, r); err != nil {
			return r, err
		}
	}
	if err = checkpoint("names-published"); err != nil {
		return r, err
	}
	if err = c.Reclaim(ctx, r); err != nil {
		return r, err
	}
	if err = checkpoint("names-reclaimed"); err != nil {
		return r, err
	}
	r.State = "reclaimed"
	if err = writeJSON(path, r); err != nil {
		return r, err
	}
	for _, unit := range r.Units {
		if err = cleanStage(filepath.Join(c.Converter.Directory, unitName(unit.Unit))); err != nil {
			return r, err
		}
	}
	return r, clearActivePlan(c.Directory, plan)
}

// NameState returns the durable cursor and, if present, the only unfinished
// batch plan. Call before opening the next source batch after process restart;
// a published plan may already have lost its legacy catalog to a synced delete.
func NameState(directory string) (NameCursor, *LegacyPlan, error) {
	var cursor NameCursor
	if err := readJSON(filepath.Join(directory, "name-cursor.json"), &cursor); err != nil && !errors.Is(err, os.ErrNotExist) {
		return cursor, nil, err
	}
	var plan LegacyPlan
	if err := readJSON(filepath.Join(directory, "name-active.json"), &plan); errors.Is(err, os.ErrNotExist) {
		return cursor, nil, nil
	} else if err != nil {
		return cursor, nil, err
	}
	if err := plan.validate(); err != nil {
		return cursor, nil, err
	}
	return cursor, &plan, nil
}

func clearActivePlan(directory string, plan LegacyPlan) error {
	var active LegacyPlan
	err := readJSON(filepath.Join(directory, "name-active.json"), &active)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	a, _ := json.Marshal(active)
	b, _ := json.Marshal(plan)
	if !bytes.Equal(a, b) {
		// Retrying a completed old batch must not remove a newer active plan.
		return nil
	}
	if err := os.Remove(filepath.Join(directory, "name-active.json")); err != nil {
		return err
	}
	d, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

type plannedSource struct {
	Source
	plan LegacyPlan
}

func (s plannedSource) Scan(ctx context.Context, u Unit, yield func(Record) error) error {
	wanted := make(map[string]bool)
	for _, m := range s.plan.Metrics {
		if partsformat.Route(partsformat.ID(m.Name)) == u.VShard {
			wanted[string(m.Name)] = false
		}
	}
	if err := s.Source.Scan(ctx, u, func(r Record) error {
		if _, ok := wanted[string(r.Key.Metric)]; !ok {
			return errors.New("legacy scanner emitted a name outside the frozen plan")
		}
		wanted[string(r.Key.Metric)] = true
		return yield(r)
	}); err != nil {
		return err
	}
	for _, found := range wanted {
		if !found {
			return errors.New("legacy scanner omitted a planned metric")
		}
	}
	return nil
}

func readJSON(path string, value any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 64<<20))
	d.DisallowUnknownFields()
	if err = d.Decode(value); err != nil {
		return err
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing global cursor data")
	}
	return nil
}

func writeJSON(path string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(b) >= 64<<20 {
		return errors.New("global name journal exceeds 64MiB")
	}
	f, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
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

// LegacyReclaimer is the concrete exclusive-close/reopen adapter. Open must use
// the existing store's comparator/merger/WAL options and DeleteCleaner. Identity
// must read the durable source incarnation from the opened COPY, not its path.
// No unpinned source snapshots/iterators, checkpoint hardlinks or external copy
// hardlinks may retain its old SST files. Close waits obsolete-file cleanup.
type LegacyReclaimer struct {
	Gate                        *ReaderGate
	CloseReaders, ReopenReaders func() error
	Open                        func() (*pebble.DB, error)
	Identity                    func(*pebble.DB) (string, error)
	Checkpoint                  func(string) error
}

func (r LegacyReclaimer) Reclaim(ctx context.Context, receipt NameReceipt) error {
	if r.Gate == nil || r.Open == nil || r.Identity == nil || receipt.State != "published" {
		return errors.New("legacy reclaim needs a published name batch and exclusive lifecycle")
	}
	return r.Gate.Exclusive(ctx, r.CloseReaders, func() error {
		db, err := r.Open()
		if err != nil {
			return err
		}
		identity, err := r.Identity(db)
		if err == nil && identity != receipt.Plan.Source {
			err = errors.New("reclaim source incarnation differs from frozen plan")
		}
		if err == nil {
			err = deleteLegacy(ctx, db, receipt.Plan, r.Checkpoint)
		}
		return errors.Join(err, db.Close())
	}, r.ReopenReaders)
}

// runSplit keeps the parent active plan, all original unit journals and all
// pending deletion authority. Only verified lexical prefixes advance the cursor.
func (c *NameConverter) runSplit(ctx context.Context, path string, r NameReceipt) (NameReceipt, error) {
	if len(r.Batches) < 2 {
		return r, errors.New("invalid split name journal")
	}
	position := 0
	for i, child := range r.Batches {
		if len(child.Plan.Metrics) == 0 || position+len(child.Plan.Metrics) > len(r.Plan.Metrics) {
			return r, errors.New("invalid split coverage")
		}
		want := r.Plan
		want.Metrics = r.Plan.Metrics[position : position+len(child.Plan.Metrics)]
		if position > 0 {
			want.Lower = bytes.Clone(r.Plan.Metrics[position].Name)
		}
		position += len(child.Plan.Metrics)
		if position < len(r.Plan.Metrics) {
			want.Upper = bytes.Clone(r.Plan.Metrics[position].Name)
			want.Final = false
		}
		a, _ := json.Marshal(want)
		b, _ := json.Marshal(child.Plan)
		cursor := r.Cursor
		cursor.Before = want.Upper
		cursor.Done = want.Final
		x, _ := json.Marshal(cursor)
		y, _ := json.Marshal(child.Cursor)
		if !bytes.Equal(a, b) || !bytes.Equal(x, y) || child.Version != 1 {
			return r, errors.New("split authority changed")
		}
		switch child.State {
		case "building", "ready", "published", "reclaimed":
		default:
			return r, errors.New("invalid split state")
		}
		if i > 0 && r.Batches[i-1].State != "reclaimed" && child.State != "building" {
			return r, errors.New("split cursor skipped reclamation")
		}
	}
	if position != len(r.Plan.Metrics) {
		return r, errors.New("split coverage is incomplete")
	}
	var current NameCursor
	if err := readJSON(filepath.Join(c.Directory, "name-cursor.json"), &current); err == nil {
		if current.Version != 1 || current.Source != r.Plan.Source || current.Destination != r.Cursor.Destination {
			return r, errors.New("split cursor identity changed")
		}
		valid := current.Batch != r.Cursor.Batch && !current.Done && bytes.Equal(current.Before, r.Plan.Lower)
		for _, child := range r.Batches {
			if current.Batch == child.Cursor.Batch && current.Done == child.Cursor.Done && bytes.Equal(current.Before, child.Cursor.Before) && child.State != "building" {
				valid = true
			}
		}
		if !valid {
			return r, errors.New("split cursor would skip or regress coverage")
		}
	} else if !errors.Is(err, os.ErrNotExist) || len(r.Plan.Lower) != 0 {
		return r, errors.New("split cursor is missing or unreadable")
	}
	base := c.Converter
	worker := &Converter{Directory: base.Directory, Source: plannedSource{Source: base.Source, plan: r.Plan}, Target: base.Target, Space: base.Space, ChunkBytes: base.ChunkBytes, MaxTables: base.MaxTables}
	original := make(map[uint16]Unit)
	for _, u := range r.Plan.Units() {
		original[u.VShard] = u
	}
	for i := range r.Batches {
		child := &r.Batches[i]
		if child.State == "reclaimed" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return r, err
		}
		if child.State == "building" {
			child.Units = nil
			for _, u := range child.Plan.Units() {
				receipt, err := worker.Run(ctx, original[u.VShard])
				if err != nil {
					return r, err
				}
				child.Units = append(child.Units, receipt)
			}
			child.State = "ready"
			if err := writeJSON(path, r); err != nil {
				return r, err
			}
		}
		if len(child.Units) != len(child.Plan.Units()) {
			return r, errors.New("split has incomplete receipts")
		}
		for j, unit := range child.Units {
			u := original[child.Plan.Units()[j].VShard]
			if unit.Unit != u || unit.Source != r.Plan.Source || unit.Destination != r.Cursor.Destination || unit.State != "ready" {
				return r, errors.New("invalid split unit receipt")
			}
			if err := base.Target.Verify(ctx, unit.Tables); err != nil {
				return r, err
			}
		}
		if child.State == "ready" {
			if err := writeJSON(filepath.Join(c.Directory, "name-cursor.json"), child.Cursor); err != nil {
				return r, err
			}
		}
		if err := c.Publish(ctx, *child); err != nil {
			return r, err
		}
		child.State = "published"
		if err := writeJSON(path, r); err != nil {
			return r, err
		}
		if c.Checkpoint != nil {
			if err := c.Checkpoint("split-published"); err != nil {
				return r, err
			}
		}
		if err := c.Reclaim(ctx, *child); err != nil {
			return r, err
		}
		child.State = "reclaimed"
		if err := writeJSON(path, r); err != nil {
			return r, err
		}
		if c.Checkpoint != nil {
			if err := c.Checkpoint("split-reclaimed"); err != nil {
				return r, err
			}
		}
	}
	// Keep one receipt per original unit for completed retries and stage GC.
	r.Units = nil
	seen := make(map[uint16]bool)
	for _, child := range r.Batches {
		for _, unit := range child.Units {
			if !seen[unit.Unit.VShard] {
				r.Units = append(r.Units, unit)
				seen[unit.Unit.VShard] = true
			}
		}
	}
	r.State = "reclaimed"
	if err := writeJSON(path, r); err != nil {
		return r, err
	}
	for _, unit := range r.Units {
		if err := cleanStage(filepath.Join(base.Directory, unitName(unit.Unit))); err != nil {
			return r, err
		}
	}
	return r, clearActivePlan(c.Directory, r.Plan)
}
