package whisperio

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-graphite/go-carbon/internal/whisperpath"
)

// MetricPath identifies an untagged metric relative to its original tree.
type MetricPath struct{ Metric, Path string }

// WalkMetrics visits original metrics without constructing a second name catalog.
// Live walks tolerate a vanished subtree; their catalog audit handles deletions.
func WalkMetrics(ctx context.Context, root string, live bool, fn func(MetricPath) error) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if stopped := ctx.Err(); stopped != nil {
			return stopped
		}
		if err != nil {
			if live && path != root && errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in source tree: %s", path)
		}
		if d.IsDir() && d.Name() == whisperpath.Quarantine {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".wsp") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		metric := strings.ReplaceAll(strings.TrimSuffix(filepath.ToSlash(rel), ".wsp"), "/", ".")
		if strings.HasPrefix(filepath.ToSlash(rel), "_tagged/") {
			return errors.New("tagged filenames require an explicit metric/path manifest to preserve original names")
		}
		return fn(MetricPath{Metric: metric, Path: rel})
	})
}

// WalkMetricsParallel is WalkMetrics with directories read by workers
// goroutines, in no particular order; fn must be safe for concurrent calls.
// Directory reads dominate a walk of a large tree, and on hosts whose security
// agents hook every open and getdents, one reader leaves conversion workers
// idle for most of a restart's re-walk.
func WalkMetricsParallel(ctx context.Context, root string, live bool, workers int, fn func(MetricPath) error) error {
	if workers <= 1 {
		return WalkMetrics(ctx, root, live, fn)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := &parallelWalk{root: root, live: live, fn: fn, stack: []string{root}, pending: 1}
	w.cond = sync.NewCond(&w.mu)
	stop := context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	defer stop()
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				dir, ok := w.next(ctx)
				if !ok {
					return
				}
				subdirs, err := w.visit(ctx, dir)
				w.done(subdirs, err, cancel)
			}
		})
	}
	wg.Wait()
	if w.err != nil {
		return w.err
	}
	return ctx.Err()
}

type parallelWalk struct {
	root    string
	live    bool
	fn      func(MetricPath) error
	mu      sync.Mutex
	cond    *sync.Cond
	stack   []string // depth first keeps the backlog near the tree's depth
	pending int      // directories queued or being read
	err     error
}

func (w *parallelWalk) next(ctx context.Context) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for len(w.stack) == 0 && w.pending > 0 && w.err == nil && ctx.Err() == nil {
		w.cond.Wait()
	}
	if len(w.stack) == 0 || w.err != nil || ctx.Err() != nil {
		return "", false
	}
	dir := w.stack[len(w.stack)-1]
	w.stack = w.stack[:len(w.stack)-1]
	return dir, true
}

func (w *parallelWalk) done(subdirs []string, err error, cancel func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil && w.err == nil {
		w.err = err
		cancel()
	}
	w.stack = append(w.stack, subdirs...)
	w.pending += len(subdirs) - 1
	w.cond.Broadcast()
}

// visit reads one directory unsorted, calls fn for its metrics and returns its
// subdirectories, with WalkMetrics' rules for errors, symlinks and tags.
func (w *parallelWalk) visit(ctx context.Context, dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err == nil {
		var entries []fs.DirEntry
		entries, err = f.ReadDir(-1)
		f.Close()
		if err == nil {
			return w.entries(ctx, dir, entries)
		}
	}
	if w.live && dir != w.root && errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return nil, err
}

func (w *parallelWalk) entries(ctx context.Context, dir string, entries []fs.DirEntry) ([]string, error) {
	var subdirs []string
	for _, d := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		path := filepath.Join(dir, d.Name())
		if d.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("symlink in source tree: %s", path)
		}
		if d.IsDir() {
			if d.Name() == whisperpath.Quarantine {
				continue
			}
			subdirs = append(subdirs, path)
			continue
		}
		if !strings.HasSuffix(path, ".wsp") {
			continue
		}
		rel, err := filepath.Rel(w.root, path)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(filepath.ToSlash(rel), "_tagged/") {
			return nil, errors.New("tagged filenames require an explicit metric/path manifest to preserve original names")
		}
		metric := strings.ReplaceAll(strings.TrimSuffix(filepath.ToSlash(rel), ".wsp"), "/", ".")
		if err := w.fn(MetricPath{Metric: metric, Path: rel}); err != nil {
			return nil, err
		}
	}
	return subdirs, nil
}
