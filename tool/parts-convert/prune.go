//go:build linux || darwin

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"golang.org/x/sys/unix"
)

type pruneResult struct{ Files, LogicalBytes, AllocatedBytes uint64 }

// pruneReclaimedReceipts removes only exact duplicate per-unit JSON receipts.
// The durable global reclaimed receipt retains the complete receipt and every
// digest. Active/building/published units and all data/staging files are kept.
// A repeated reclaimed NameConverter.Run needs the directory for cleanStage,
// but does not read its per-unit cursor. Keep that directory and global journal.
func pruneReclaimedReceipts(target string, remove bool) (pruneResult, error) {
	var out pruneResult
	absolute, err := filepath.Abs(target)
	if err != nil {
		return out, err
	}
	identity, err := os.ReadFile(filepath.Join(absolute, "CONVERSION-TARGET"))
	if err != nil {
		return out, err
	}
	lock, err := os.OpenFile(filepath.Join(absolute, "names", "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return out, err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return out, err
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	journals, err := filepath.Glob(filepath.Join(absolute, "names", "names-*.json"))
	if err != nil {
		return out, err
	}
	wanted := map[string][]byte{}
	for _, path := range journals {
		b, err := os.ReadFile(path)
		if err != nil {
			return out, err
		}
		var receipt partsconvert.NameReceipt
		if err := json.Unmarshal(b, &receipt); err != nil {
			return out, err
		}
		if receipt.State != "reclaimed" {
			continue
		}
		if receipt.Version != 1 || receipt.Cursor.Destination != strings.TrimSpace(string(identity)) || receipt.Cursor.Source != receipt.Plan.Source {
			return out, errors.New("reclaimed journal identity mismatch")
		}
		for _, unit := range receipt.Units {
			if unit.Version != 1 || unit.Source != receipt.Plan.Source || unit.Destination != receipt.Cursor.Destination || unit.State != "ready" {
				return out, errors.New("invalid reclaimed unit")
			}
			// Derive exactly the public unit journal convention, including opaque
			// byte names, from an existing table path rather than another hash codec.
			if len(unit.Tables) == 0 {
				continue
			}
			directory := filepath.Dir(unit.Tables[0].Path)
			if filepath.Dir(directory) != filepath.Join(absolute, "units") {
				return out, errors.New("reclaimed stage path escapes target units")
			}
			for _, table := range unit.Tables {
				if filepath.Dir(table.Path) != directory {
					return out, errors.New("reclaimed unit spans stage directories")
				}
			}
			cursorPath := filepath.Join(directory, "cursor.json")
			if _, duplicate := wanted[cursorPath]; duplicate {
				return out, errors.New("duplicate reclaimed unit ownership")
			}
			want, err := json.Marshal(unit)
			if err != nil {
				return out, err
			}
			wanted[cursorPath] = want
		}
	}
	// Validate every candidate before unlinking the first. An interrupted prune
	// simply skips its already-absent redundant files on the next invocation.
	var candidates []string
	for path, want := range wanted {
		b, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return out, err
		}
		var unit partsconvert.Receipt
		if err := json.Unmarshal(b, &unit); err != nil {
			return out, err
		}
		got, err := json.Marshal(unit)
		if err != nil {
			return out, err
		}
		if !bytes.Equal(got, want) {
			return out, fmt.Errorf("reclaimed receipt differs from retained authority: %s", path)
		}
		info, err := os.Lstat(path)
		if err != nil {
			return out, err
		}
		if !info.Mode().IsRegular() {
			return out, errors.New("redundant receipt is not regular file")
		}
		out.Files++
		out.LogicalBytes += uint64(info.Size())
		// Each receipt is a distinct JSON file; inode links are checked during
		// initial accounting by allocatedBytes. This is potential release, not a
		// substitute for observing post-prune allocation of the complete store.
		n, err := allocatedBytes(path)
		if err != nil {
			return out, err
		}
		out.AllocatedBytes += n
		candidates = append(candidates, path)
	}
	if remove {
		for _, path := range candidates {
			if err := os.Remove(path); err != nil {
				return out, err
			}
			d, err := os.Open(filepath.Dir(path))
			if err != nil {
				return out, err
			}
			if err := errors.Join(d.Sync(), d.Close()); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}
