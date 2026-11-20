package downsize

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// A fixed --names scope already has a complete, durable inventory. A restart
// changes the data to compare, not that inventory. Reuse only the catalog, never
// copy or verification receipts: every configured donor still participates in
// the new gap comparison, including donors that acquired a name after discovery.
// Unbounded scopes need a catalog-change feed before they can do the same.
func seedReadyRecoveryCatalog(ctx context.Context, original, recovery syncOptions) error {
	if !recovery.restartWindow || original.namesFile == "" || original.namesHash == "" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(recovery.state, 0755); err != nil {
		return err
	}
	path := filepath.Join(recovery.state, "sync.jsonl")
	lock, err := lockResumeState(path)
	if err != nil {
		return err
	}
	defer lock.Close()
	// Existing recoveries retain their own catalog/cursors. In particular, never
	// replace an in-progress discovery or any verification prefix on adoption.
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	sourcePath := filepath.Join(original.state, "sync.jsonl")
	sourceLock, err := lockResumeState(sourcePath)
	if err != nil {
		return err
	}
	defer sourceLock.Close()
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	identity, err := syncIdentity(original)
	if err != nil {
		return err
	}
	reader := bufio.NewReaderSize(source, 64<<10)
	var discovered syncRow
	var catalogBytes int64
	var catalogNames int
	fixedInventory, header := false, false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return fmt.Errorf("restart catalog has no committed discovery: %w", err)
		}
		var row syncRow
		if err := json.Unmarshal(line, &row); err != nil {
			return fmt.Errorf("restart catalog journal: %w", err)
		}
		if !header && row.Kind != "header" {
			return errors.New("restart catalog journal has no header")
		}
		switch {
		case row.Kind == "header":
			header = true
			if row.Identity != identity {
				return errors.New("restart catalog scope changed")
			}
		case row.Kind == "discover_checkpoint":
			fixedInventory = row.Discovery != nil && row.Discovery.Cursors != nil && (row.Discovery.Cursors["@names"] != "" || row.Discovery.Names == 0)
			if row.Discovery != nil {
				catalogBytes, catalogNames = row.Discovery.UnionBytes, row.Discovery.Names
			}
		case row.Kind == "discovered":
			discovered = row
		case row.Kind != "phase-position" && row.Kind != "phase-progress":
			return errors.New("restart catalog journal has no discovery boundary")
		}
		if row.Kind == "discovered" {
			break
		}
	}
	if !fixedInventory {
		return errors.New("restart catalog lacks a complete explicit-name inventory")
	}
	if discovered.Names < 0 || discovered.Names != catalogNames {
		return errors.New("restart catalog has invalid name count")
	}
	// Hard links avoid reading or copying a multi-gigabyte inventory. Both paths
	// are immutable after the durable discovery row. There is intentionally no
	// fallback to re-listing if the source or filesystem cannot prove this reuse.
	from, to := filepath.Join(original.state, "union.tsv"), filepath.Join(recovery.state, "union.tsv")
	info, err := os.Stat(from)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != catalogBytes {
		return errors.New("restart catalog extent changed")
	}
	if err := os.Link(from, to); err != nil {
		a, ea := os.Stat(from)
		b, eb := os.Stat(to)
		if ea != nil || eb != nil || !os.SameFile(a, b) {
			return fmt.Errorf("reuse restart catalog: %w", err)
		}
	}
	missing, err := os.OpenFile(filepath.Join(recovery.state, "missing.tsv"), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	st, err := missing.Stat()
	if err == nil && st.Size() != 0 {
		err = errors.New("restart catalog has unexpected missing-name work")
	}
	err = errors.Join(err, missing.Sync(), missing.Close())
	if err != nil {
		return err
	}
	recoveryIdentity, err := syncIdentity(recovery)
	if err != nil {
		return err
	}
	// Gap verification/fill owns absence now. The old missing-name copy phase
	// would pull unrelated history and must not be imported into this generation.
	discovered.Missing = 0
	discovered.Time = time.Now().UTC().Format(time.RFC3339Nano)
	tmp, err := os.CreateTemp(recovery.state, ".restart-catalog-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	encode := json.NewEncoder(tmp)
	err = encode.Encode(syncRow{Kind: "header", Identity: recoveryIdentity, Time: discovered.Time})
	if err == nil {
		err = encode.Encode(discovered)
	}
	err = errors.Join(err, tmp.Sync(), tmp.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	if err := syncDirectory(recovery.state); err != nil {
		return err
	}
	event("sync_restart_catalog_reused", map[string]any{"names": discovered.Names, "from": original.state, "state": recovery.state, "windows": recovery.windows.String()})
	return nil
}
