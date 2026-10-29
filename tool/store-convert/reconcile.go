package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// A live baseline and its final quiesced reconciliation share the original
// identity. Nothing permits adopting a different source, copy tree or dataset.
func reconciliationIdentity(dir string, requested identity) (identity, error) {
	data, err := os.ReadFile(filepath.Join(dir, "CONVERSION.json"))
	if err != nil {
		return identity{}, fmt.Errorf("reconcile requires an existing conversion identity: %w", err)
	}
	var previous identity
	if err := json.Unmarshal(data, &previous); err != nil {
		return identity{}, err
	}
	requested.Live = previous.Live
	if previous != requested {
		return identity{}, errors.New("reconciliation identity differs from the baseline")
	}
	return previous, nil
}

// Canonical paths are deliberately required for the complete-tree finalizer.
// An arbitrary or partial manifest cannot authorize deletion of unseen metrics.
func canonicalEntry(e entry) bool {
	return e.Metric != "" && !strings.ContainsAny(e.Metric, ";/\\") && !strings.Contains(e.Metric, "..") && !strings.HasPrefix(e.Metric, ".") && !strings.HasSuffix(e.Metric, ".") && filepath.IsLocal(e.Path) && filepath.Clean(e.Path) == e.Path &&
		filepath.ToSlash(e.Path) == strings.ReplaceAll(e.Metric, ".", "/")+".wsp"
}

func (c *converter) reconcileDeletions(ctx context.Context) (uint64, error) {
	var deleted uint64
	err := chunkstore.EachPage(ctx, c.db.ListPage, "", 1000, func(page []chunkstore.Metadata) error {
		for _, metadata := range page {
			if err := ctx.Err(); err != nil {
				return err
			}
			e := entry{Metric: metadata.Name, Path: filepath.FromSlash(strings.ReplaceAll(metadata.Name, ".", "/") + ".wsp")}
			if !canonicalEntry(e) {
				return errors.New("final reconciliation cannot map a tagged or noncanonical catalog metric")
			}
			if c.visited.Contains(metadata.ID) {
				c.deletionChecksSkipped.Add(1)
				continue
			}
			path := filepath.Join(c.source, e.Path)
			resolved, err := filepath.EvalSymlinks(path)
			if err == nil {
				if !within(c.source, resolved) {
					return fmt.Errorf("source alias escapes tree: %s", e.Path)
				}
				info, err := os.Stat(path)
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() {
					return fmt.Errorf("source metric is not a regular file: %s", e.Path)
				}
				continue
			}
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := c.db.Delete(ctx, e.Metric); err != nil {
				return err
			}
			deleted++
		}
		return nil
	})
	return deleted, err
}
