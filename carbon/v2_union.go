package carbon

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-whisper"
)

// openV2Classic reuses the writable dual-write handle, or opens history read-only.
func (app *App) openV2Classic() ([]string, error) {
	c := app.Config
	var names []string
	if c.Whisper.StorageBackend == "pebble-chunk" {
		path := sharedStorePath(c)
		entries, err := os.ReadDir(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if len(entries) > 0 {
			if app.MetricStore == nil {
				opts := store.Options{ReadOnly: true, ExactBlocks: c.Whisper.StoreEncoding == "exact", ReadConcurrency: c.Whisper.StoreReadConcurrency, BlockLoadConcurrency: c.Whisper.StoreBlockLoadConcurrency, CacheSize: c.Whisper.StoreCacheSize, WALDir: c.Whisper.StoreWALDir}
				if c.Whisper.StoreIncrementalMigration {
					opts.IncrementalSource = c.Whisper.DataDir
				}
				app.MetricStore, err = store.Open(path, opts)
				if errors.Is(err, store.ErrFormat) && opts.ExactBlocks {
					// Read-only format validation happens before opening Pebble. The
					// continuation codec also reads native formats v6 and later.
					opts.Continuation = &whisperio.ContinuationCodec{ReuseBlockKeys: true, DirectLateWrites: true}
					opts.TimeBuckets = true
					app.MetricStore, err = store.Open(path, opts)
				}
				if err != nil {
					return nil, fmt.Errorf("open historical classic store: %w", err)
				}
			}
			if err := store.EachPage(context.Background(), app.MetricStore.ListCatalogPage, "", 4096, func(page []store.Metadata) error {
				for _, m := range page {
					names = append(names, m.Name)
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}
	// Native shared stores use only their catalog. An incremental store may still
	// refer to .wsp files, whose names must also be visible from initial readiness.
	if app.MetricStore == nil || c.Whisper.StoreIncrementalMigration {
		root := c.Whisper.DataDir
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if errors.Is(err, os.ErrNotExist) && path == root {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("historical classic symlink %s", path)
			}
			if !strings.HasSuffix(path, ".wsp") {
				return nil
			}
			w, err := whisper.Open(path)
			if err != nil {
				return fmt.Errorf("open historical classic metric: %w", err)
			}
			if err := w.Close(); err != nil {
				return err
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			names = append(names, strings.ReplaceAll(strings.TrimSuffix(relative, ".wsp"), string(filepath.Separator), "."))
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	slices.Sort(names)
	return slices.Compact(names), nil
}
