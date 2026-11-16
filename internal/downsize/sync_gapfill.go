package downsize

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Receipts are scoped to one immutable window and holder/ownership inventory,
// not to the evolving gap ledger or cutover period. Closing another gap cannot
// invalidate completed work. These receipts alone never authorize READY: the
// separate all-donor proof also covers names created after this catalog snapshot.
type gapfillReceipt struct {
	FilledAt    int64             `json:"filled_at"`
	Schema      int               `json:"schema"`
	Identity    string            `json:"identity"`
	Window      syncRange         `json:"window"`
	Target      string            `json:"target"`
	Sources     []string          `json:"sources"`
	Catalog     string            `json:"catalog"`
	Names       int               `json:"names_processed"`
	FilledNames int               `json:"copied_source_name_transfers"`
	Points      windowRepairCount `json:"points"`
	Samples     []gapfillSample   `json:"samples"`
	Complete    bool              `json:"complete"`
}

type gapfillSample struct {
	Source  string     `json:"source"`
	Name    string     `json:"name"`
	Step    int64      `json:"step"`
	From    int64      `json:"from"`
	Until   int64      `json:"until"`
	Outcome readyCount `json:"outcome"`
}

func gapfillKey(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func fillReadyGaps(ctx context.Context, original, proof syncOptions, selection []string, b syncReadyBackend) ([]string, error) {
	// loadReadyProof puts the cutover period first. Only explicit closed gaps
	// belong to this phase; open bounds must never be replaced by the current time.
	windows := proof.windows[1:]
	if len(windows) == 0 {
		return nil, nil
	}
	if proof.noCopy || proof.noVerify {
		return nil, errors.New("gap fill requires copying and verification")
	}
	ring := []byte(nil)
	var err error
	if proof.ring != "" {
		ring, err = os.ReadFile(proof.ring)
		if err != nil {
			return nil, err
		}
	}
	key, err := gapfillKey([]any{proof.to, proof.from, ring, proof.namesHash})
	if err != nil {
		return nil, err
	}
	catalog := proof
	catalog.state = filepath.Join(original.state, "gapfill-catalog-"+key)
	catalog.windows, catalog.restartWindow = nil, false
	catalog.fullHistory, catalog.sampleEvery = true, 1
	catalog.noCopy, catalog.noVerify, catalog.issues, catalog.policy = true, true, nil, nil
	selected := map[string]bool{}
	for _, id := range selection {
		selected[id] = true
	}
	var receipts []string
	var failures []error
	catalogReady := false
	for _, w := range windows {
		if err := ctx.Err(); err != nil {
			return receipts, errors.Join(append(failures, err)...)
		}
		identity, err := gapfillKey([]any{key, w.From, w.Until})
		if err != nil {
			return receipts, err
		}
		o := proof
		o.state = filepath.Join(original.state, "gapfill-window-"+identity)
		o.windows, o.fullHistory, o.sampleEvery, o.settle = syncRanges{{Name: "gap", From: w.From, Until: w.Until}}, false, 0, 0
		o.restartWindow, o.issues, o.policy = true, nil, nil
		label, err := gapfillKey(w.Name)
		if err != nil {
			return receipts, err
		}
		path := filepath.Join(o.state, "receipt-"+label+".json")
		var receipt gapfillReceipt
		if data, err := os.ReadFile(path); err == nil {
			if err := json.Unmarshal(data, &receipt); err != nil {
				return receipts, err
			}
			if receipt.Schema != 1 || receipt.Identity != identity || receipt.Window != w || !receipt.Complete {
				return receipts, fmt.Errorf("invalid gap receipt %s", path)
			}
			receipts = append(receipts, path)
			event("sync_gapfill_resumed", map[string]any{"window": w.Name, "receipt": path})
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return receipts, err
		}
		if len(selected) > 0 && !selected[w.Name] {
			failures = append(failures, fmt.Errorf("gap receipt pending: %s", w.Name))
			continue
		}
		err = func() error {
			if w.Until > o.now().Unix() {
				return fmt.Errorf("gap end is in the future: %s", w.Name)
			}
			if err := os.MkdirAll(o.state, 0755); err != nil {
				return err
			}
			filled := filepath.Join(o.state, "filled.json")
			if data, err := os.ReadFile(filled); err == nil {
				if err := json.Unmarshal(data, &receipt); err != nil {
					return err
				}
				if receipt.Identity != identity || receipt.Schema != 1 {
					return errors.New("gap fill checkpoint scope changed")
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			} else {
				if !catalogReady {
					if err := runSync(ctx, catalog, b); err != nil {
						return fmt.Errorf("gap inventory: %w", err)
					}
					catalogReady = true
				}
				if err := seedGapfillCatalog(catalog, o); err != nil {
					return err
				}
				event("sync_gapfill_start", map[string]any{"window": w, "sources": o.from, "state": o.state})
				// Copy committed native windows directly, including live target names.
				// Retention samples below verify this phase; the all-donor full proof
				// remains a separate final READY requirement.
				if err := runGapfillWindow(ctx, o, b); err != nil {
					return err
				}
				receipt = gapfillReceipt{FilledAt: o.now().Unix(), Schema: 1, Identity: identity, Window: w, Target: o.to, Sources: o.from, Catalog: catalog.state}
				j, err := openSyncJournal(filepath.Join(o.state, "sync.jsonl"), mustGapfillSyncIdentity(o), syncCheckpointOptions(o))
				if err != nil {
					return err
				}
				if j.checkpoint != nil {
					receipt.Names = j.checkpoint.Summary.Discovered
				}
				for _, row := range j.rows {
					if row.Kind == "discovered" {
						receipt.Names = row.Names
					}
					if row.Kind == "copied" {
						receipt.FilledNames += row.Copied
					}
				}
				j.close()
				receipt.Points, err = collectWindowPointCountsContext(ctx, o.state)
				if err != nil {
					return err
				}
				// This marker commits the finished native fill before any
				// sampling. A restart during sampling must not walk that work again.
				if err := atomicStateJSON(filled, receipt); err != nil {
					return err
				}
			}
			if err := sampleGapfill(ctx, o, b, &receipt); err != nil {
				return err
			}
			receipt.Window, receipt.Complete = w, true
			if err := atomicStateJSON(path, receipt); err != nil {
				return err
			}
			event("sync_gapfill_complete", map[string]any{"window": w.Name, "receipt": path, "names": receipt.Names, "points": receipt.Points})
			return nil
		}()
		if err != nil {
			failures = append(failures, fmt.Errorf("gap %s: %w", w.Name, err))
			continue
		}
		receipts = append(receipts, path)
	}
	return receipts, errors.Join(failures...)
}

func mustGapfillSyncIdentity(o syncOptions) string {
	// syncIdentity encodes only JSON primitive values.
	identity, _ := syncIdentity(o)
	return identity
}

// Share only the completed discovery snapshot, never another window's copy or
// verification rows. The final all-donor proof remains independent and fresh.
func seedGapfillCatalog(catalog, window syncOptions) error {
	path := filepath.Join(window.state, "sync.jsonl")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	j, err := openSyncJournal(filepath.Join(catalog.state, "sync.jsonl"), mustGapfillSyncIdentity(catalog), syncCheckpointOptions(catalog))
	if err != nil {
		return err
	}
	var discovered *syncRow
	for _, row := range j.rows {
		if row.Kind == "discovered" {
			r := row
			discovered = &r
			break
		}
	}
	j.close()
	if discovered == nil {
		return errors.New("gap inventory has no committed discovery")
	}
	from, to := filepath.Join(catalog.state, "union.tsv"), filepath.Join(window.state, "union.tsv")
	if err := os.Link(from, to); err != nil {
		a, ea := os.Stat(from)
		z, ez := os.Stat(to)
		if ea != nil || ez != nil || !os.SameFile(a, z) {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(window.state, "missing.tsv"), nil, 0644); err != nil {
		return err
	}
	discovered.Missing = 0
	// Publish the journal only after its catalog files are durable. A crash
	// before rename leaves a resumable seed, never a header-only discovery that
	// could truncate the shared catalog.
	temp, err := os.CreateTemp(window.state, ".gapfill-seed-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	enc := json.NewEncoder(temp)
	err = enc.Encode(syncRow{Kind: "header", Identity: mustGapfillSyncIdentity(window)})
	if err == nil {
		err = enc.Encode(discovered)
	}
	err = errors.Join(err, temp.Sync(), temp.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		return err
	}
	return syncDirectory(window.state)
}

// Select a distributed, bounded cohort without walking a multi-gigabyte TSV
// again. Names at evenly spaced byte offsets are deterministic on this fixed
// inventory; each selected name is sampled in every retained archive below.
func gapfillSampleNames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for i := int64(0); i < 100; i++ {
		off := st.Size() * i / 100
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			return nil, err
		}
		r := bufio.NewReader(f)
		if off > 0 {
			if _, err := r.ReadString('\n'); err != nil && !errors.Is(err, io.EOF) {
				return nil, err
			}
		}
		line, err := r.ReadString('\n')
		if errors.Is(err, io.EOF) && line == "" {
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		name, _, _, err := parseUnionLine(line)
		if err != nil {
			return nil, err
		}
		selected[name] = true
	}
	// Uneven line lengths can make offsets select the same name. Top up from
	// the beginning with a bounded read, retaining 100 distinct candidates when
	// the inventory has that many names, and all names for smaller inventories.
	if len(selected) < 100 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		scan := bufio.NewScanner(f)
		for len(selected) < 100 && scan.Scan() {
			name, _, _, err := parseUnionLine(scan.Text())
			if err != nil {
				return nil, err
			}
			selected[name] = true
		}
		if err := scan.Err(); err != nil {
			return nil, err
		}
	}
	names := make([]string, 0, len(selected))
	for name := range selected {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func sampleGapfill(ctx context.Context, o syncOptions, b syncReadyBackend, receipt *gapfillReceipt) error {
	now := receipt.FilledAt
	if now <= 0 {
		return errors.New("gap fill checkpoint has no completion time")
	}
	o.now = func() time.Time { return time.Unix(now, 0) }
	path := filepath.Join(o.state, "samples.json")
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &receipt.Samples); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	done := map[string]bool{}
	for _, s := range receipt.Samples {
		if s.Outcome.Missing > 0 || s.Outcome.StepMismatch > 0 {
			return errors.New("invalid completed gap sample")
		}
		key, _ := gapfillKey([]any{s.Source, s.Name, s.Step, s.From, s.Until})
		done[key] = true
	}
	names, err := gapfillSampleNames(filepath.Join(o.state, "union.tsv"))
	if err != nil {
		return err
	}
	for _, source := range o.from {
		info, err := b.Info(ctx, source, names)
		if err != nil {
			return err
		}
		for _, name := range names {
			archives := append([]syncArchive(nil), info[name]...)
			sort.Slice(archives, func(i, j int) bool { return archives[i].Step < archives[j].Step })
			var previous int64
			for _, a := range archives {
				from, until := syncWindow(o.now().Unix(), a, 0)
				if previous > 0 {
					until = min(until, o.now().Unix()-previous)
				}
				previous = max(previous, a.Step*a.Points)
				from, until = max(from, o.windows[0].From), min(until, o.windows[0].Until)
				if from >= until {
					continue
				}
				sample := gapfillSample{Source: source, Name: name, Step: a.Step, From: from, Until: until}
				key, _ := gapfillKey([]any{source, name, a.Step, from, until})
				if done[key] {
					continue
				}
				sample.Outcome, err = readyCompare(ctx, b, source, o.to, name, from, until, a.Step)
				if err != nil {
					return err
				}
				if sample.Outcome.Missing > 0 || sample.Outcome.StepMismatch > 0 {
					return fmt.Errorf("gap sample failed %s %s step=%d: %+v", source, name, a.Step, sample.Outcome)
				}
				receipt.Samples = append(receipt.Samples, sample)
				if err := atomicStateJSON(path, receipt.Samples); err != nil {
					return err
				}
			}
		}
	}
	return ctx.Err()
}
