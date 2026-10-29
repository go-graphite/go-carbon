package persister

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/helper"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/internal/whisperpath"
	whisper "github.com/go-graphite/go-whisper"
)

// whisperPath already removes these batches using the stock name-length policy
// and records a debug event. Do not report that rejection as a storage failure.
var errOriginalMetricPathRejected = errors.New("invalid original metric path")

const incrementalCaptureShards = 256

// IncrementalMetricStore publishes ownership only after exact verification and
// a durable commit. The same store serves reads and ordinary shared-only writes.
type IncrementalMetricStore interface {
	MetricStore
	IncrementalSource() string
	StageFileSnapshot(context.Context, store.Snapshot) (store.Metadata, error)
	PublishFileOwnership(context.Context, store.Snapshot, store.Metadata) (store.Metadata, error)
	PendingFileCleanup(context.Context, int) ([]store.Metadata, error)
	CompleteFileCleanup(context.Context, store.Metadata) error
	DiscardStagedFile(context.Context, string) error
}

// SetIncrementalMigrationPriority installs a best-effort hint after a successful
// original-file write. The callback must not wait for conversion or admission.
// Removing it is safe while writers are running; an already loaded callback may
// finish afterwards, so consumers must not close their notification channel.
func (p *Whisper) SetIncrementalMigrationPriority(hint func(string)) {
	if hint == nil {
		p.incrementalPriority.Store(nil)
		return
	}
	p.incrementalPriority.Store(&hint)
}

// SetIncrementalMigrationComplete marks every original converted: a finished
// migration that skipped nothing. Writes then go to the store without looking
// up ownership first, which on a converted host only repeated the store's own
// lookup. A stale staged import of a removed original is still discarded
// before a metric of its name is created (see createSharedMetric).
func (p *Whisper) SetIncrementalMigrationComplete() { p.incrementalComplete.Store(true) }

func (p *Whisper) SetIncrementalMigration(db IncrementalMetricStore, scratch string) error {
	if db == nil || db.IncrementalSource() != p.rootPath || p.mirrorFiles || p.tagsEnabled || scratch == "" {
		return errors.New("incremental migration requires its original directory, private scratch and no rollback mirror or tagged paths")
	}
	scratch = filepath.Join(scratch, ".incremental-capture")
	// OOO snapshots create and remove a private directory per conversion.
	// Give them independent parents instead of serializing on one directory.
	// Store startup recursively removes this disposable tree.
	for i := range incrementalCaptureShards {
		if err := os.MkdirAll(filepath.Join(scratch, fmt.Sprintf("%02x", i)), 0750); err != nil {
			return err
		}
	}
	p.incrementalStore, p.incrementalScratch = db, scratch
	return nil
}

// Called under the ordinary per-metric writer lock, before popping cache data.
// A staged import is intentionally invisible until ownership becomes durable.
// The returned path accompanies an opened original so the caller does not
// rebuild and revalidate it for every unconverted write.
func (p *Whisper) incrementalWriteRoute(metric string) (bool, *whisper.Whisper, string, error) {
	if p.incrementalStore == nil || p.incrementalComplete.Load() {
		return true, nil, "", nil
	}
	ctx := context.Background()
	if _, err := p.incrementalStore.Metadata(ctx, metric); err == nil {
		return true, nil, "", nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return false, nil, "", err
	}
	path, err := p.incrementalPath(metric)
	if err != nil {
		return false, nil, "", err
	}
	// Reuse the fenced source open for the ordinary write. A separate stat
	// repeats an expensive filesystem operation for every unconverted metric.
	w, err := p.openWhisper(path, true)
	if err == nil {
		return false, w, path, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, nil, "", err
	}
	// A source removed before publication is not allowed to resurrect its
	// abandoned import when a new metric with the same name arrives.
	if err := p.incrementalStore.DiscardStagedFile(ctx, metric); err != nil {
		return false, nil, "", err
	}
	return true, nil, "", nil
}

func (p *Whisper) incrementalPath(metric string) (string, error) {
	if metric == "" || strings.ContainsAny(metric, "/\\;") {
		return "", errors.New("incremental migration requires canonical untagged metric names")
	}
	path, ok := p.whisperPath(metric)
	if !ok {
		return "", errOriginalMetricPathRejected
	}
	if !canonicalMetricPath(p.rootPath, path, metric) {
		return "", errors.New("noncanonical original metric path")
	}
	return path, nil
}

// canonicalMetricPath reports whether path is exactly root/<metric with dots
// as separators>.wsp. It replaces filepath.Rel plus a rebuilt relative name,
// which allocated twice on every unconverted write. Dots always become
// separators, so a cleaned path of the same length cannot hide "." or ".."
// segments.
func canonicalMetricPath(root, path, metric string) bool {
	const sep = filepath.Separator
	// Clean does not allocate for already-clean input, which whisperPath's
	// Join always produces; it keeps parity with filepath.Rel otherwise.
	root, path = filepath.Clean(root), filepath.Clean(path)
	// Rel also handles current-directory roots and platform-specific volume
	// and case rules. Keep its behavior for those uncommon configurations.
	if sep != '/' || root == "." {
		relative, err := filepath.Rel(root, path)
		return err == nil && filepath.IsLocal(relative) && relative == strings.ReplaceAll(metric, ".", string(sep))+".wsp"
	}
	if !strings.HasPrefix(path, root) {
		return false
	}
	n := len(root)
	if root[n-1] != sep {
		if len(path) <= n || path[n] != sep {
			return false
		}
		n++
	}
	if len(path) != n+len(metric)+len(".wsp") || !strings.HasSuffix(path, ".wsp") {
		return false
	}
	for i := 0; i < len(metric); i++ {
		c := metric[i]
		if c == '.' {
			c = sep
		}
		if path[n+i] != c {
			return false
		}
	}
	return true
}

func (p *Whisper) lockMetricContext(ctx context.Context, name string) (func(), error) {
	mutex := &p.storeMutex[storeMutexIndex(name)]
	for !mutex.TryLock() {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		mutex.Unlock()
		return nil, err
	}
	return mutex.Unlock, nil
}

// FileConversion reports physical bytes unlinked, not a promised reduction in
// filesystem allocation: another open descriptor or reflink may retain extents.
// ErrConversionInProgress reports that another worker is converting the metric,
// as when a write hint and the cold scan pick the same name. That worker
// publishes it, so callers skip the duplicate.
var ErrConversionInProgress = errors.New("metric conversion already in progress")

type FileConversion struct {
	Metadata        store.Metadata
	Points          int
	UnlinkedBytes   int64
	ArchivedBytes   int64
	PreviouslyOwned bool
}

func originalPaths(path string) []string {
	sidecar := whisper.OutOfOrderSidecarPath(path)
	return []string{sidecar, whisper.PathLockPath(sidecar), path, whisper.PathLockPath(path)}
}

// ConversionWorkingBytes reserves capture, decode, verification and encoding
// workspace without reading the source history twice.
func (p *Whisper) ConversionWorkingBytes(ctx context.Context, metric string) (int64, error) {
	path, err := p.incrementalPath(metric)
	if err != nil {
		return 0, err
	}
	return whisperio.SnapshotWorkingBytes(ctx, path)
}

func (p *Whisper) ConvertFile(ctx context.Context, metric string) (FileConversion, error) {
	return p.convertFile(ctx, metric, nil)
}

// fault is used by crash-boundary tests; production always passes nil. A failed
// boundary leaves its real durable state for a subsequent process to recover.
func (p *Whisper) convertFile(ctx context.Context, metric string, fault func(string) error) (FileConversion, error) {
	var result FileConversion
	if p.incrementalStore == nil {
		return result, errors.New("incremental migration is disabled")
	}
	if p.conversionIO != nil {
		slot := conversionSlot(p.conversionIO)
		select {
		case slot <- struct{}{}:
		case <-ctx.Done():
			return result, ctx.Err()
		}
		defer slot.Release()
		ctx = store.WithIngestYield(ctx, slot)
	}
	unlock, err := p.lockMetricContext(ctx, metric)
	if err != nil {
		return result, err
	}
	// The stripe excludes a writer already inside store(); the marker then
	// defers this metric's writeouts until conversion ends, while the stripe's
	// other metrics keep persisting during a long import.
	if _, busy := p.converting.LoadOrStore(metric, struct{}{}); busy {
		unlock()
		return result, ErrConversionInProgress
	}
	unlock()
	defer p.converting.Delete(metric)
	path, err := p.incrementalPath(metric)
	if err != nil {
		return result, err
	}
	metadata, err := p.incrementalStore.Metadata(ctx, metric)
	switch {
	case err == nil:
		result.Metadata, result.PreviouslyOwned = metadata, true
	case !errors.Is(err, store.ErrNotFound):
		return result, err
	default:
		var sourceActivity time.Time
		if info, err := os.Lstat(path); err != nil {
			return result, err
		} else if !info.Mode().IsRegular() {
			return result, errors.New("conversion source is not a regular file")
		} else {
			sourceActivity = info.ModTime()
		}
		scratch := filepath.Join(p.incrementalScratch, fmt.Sprintf("%02x", helper.HashString(metric)%incrementalCaptureShards))
		ctx = whisperio.WithConversionSource(ctx)
		snapshot, err := whisperio.ReadSnapshotIn(ctx, metric, path, scratch)
		if err != nil {
			return result, err
		}
		for _, archive := range snapshot.Archives {
			result.Points += len(archive.Points)
		}
		// Conversion is not ingestion. Preserve the original main-file activity
		// used by legacy mtime expiration; ordinary shared writes refresh it.
		snapshot.Metadata.LastUpdate = sourceActivity
		staged, err := p.incrementalStore.StageFileSnapshot(ctx, snapshot)
		if err != nil {
			return result, err
		}
		if fault != nil {
			if err := fault("staged"); err != nil {
				return result, err
			}
		}
		metadata, err = p.incrementalStore.PublishFileOwnership(ctx, snapshot, staged)
		if err != nil {
			return result, err
		}
		result.Metadata = metadata
	}
	if fault != nil {
		if err := fault("owned"); err != nil {
			return result, err
		}
	}
	if r := p.reclaim; r != nil && fault == nil && p.incrementalArchive == "" && !result.PreviouslyOwned {
		if r.push(reclaimJob{name: metadata.Name, id: metadata.ID, generation: metadata.Generation, path: path}) {
			if info, err := os.Lstat(path); err == nil {
				if stat, ok := info.Sys().(*syscall.Stat_t); ok {
					result.UnlinkedBytes = stat.Blocks * 512
				}
			}
			return result, nil
		}
		r.inlineFallback.Add(1)
	}
	for i, original := range originalPaths(path) {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		info, err := os.Lstat(original)
		if err == nil {
			if !info.Mode().IsRegular() {
				return result, fmt.Errorf("refuse to reclaim a nonregular original: %s", original)
			}
			if p.incrementalArchive != "" {
				if err := p.archiveOriginal(original, info); err != nil {
					return result, err
				}
				if fault != nil {
					if err := fault(fmt.Sprintf("archived-%d", i)); err != nil {
						return result, err
					}
				}
			}
			if err := os.Remove(original); err != nil {
				return result, err
			}
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				if p.incrementalArchive != "" {
					result.ArchivedBytes += stat.Blocks * 512
				} else {
					result.UnlinkedBytes += stat.Blocks * 512
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return result, err
		}
		if fault != nil {
			if err := fault(fmt.Sprintf("unlink-%d", i)); err != nil {
				return result, err
			}
		}
	}
	if err := p.reclaimedDirectories.sync(filepath.Dir(path), p.rootPath); err != nil {
		return result, err
	}
	if fault != nil {
		if err := fault("directory-synced"); err != nil {
			return result, err
		}
	}
	if err := p.pruneOriginalDirectory(ctx, filepath.Dir(path)); err != nil {
		return result, err
	}
	return result, p.incrementalStore.CompleteFileCleanup(ctx, metadata)
}

// An external empty-directory reaper may remove a parent after a crash. Sync
// the closest remaining ancestor so that its missing child is durable too.
func syncReclaimedDirectory(path, root string) error {
	for {
		directory, err := os.Open(path)
		if err == nil {
			return errors.Join(directory.Sync(), directory.Close())
		}
		if !errors.Is(err, os.ErrNotExist) || path == root {
			return err
		}
		path = filepath.Dir(path)
	}
}

func (p *Whisper) pruneOriginalDirectory(ctx context.Context, dir string) error {
	if r := p.reclaim; r != nil {
		r.rootMu.RLock()
		if r.root != nil {
			err := whisperpath.PruneAt(int(r.root.Fd()), p.rootPath, dir, ctx.Err)
			r.rootMu.RUnlock()
			return err
		}
		r.rootMu.RUnlock()
	}
	return whisperpath.Prune(p.rootPath, dir, ctx.Err)
}

// directorySyncs coalesces reclamation syncs of one directory. Thousands of
// concurrent conversions walk the same leaf directories, and every directory
// sync forces the filesystem log. A sync covers each request registered before
// it started, so every caller's unlinks are durable before it returns.
type directorySyncs struct {
	mu   sync.Mutex
	dirs map[string]*directorySync
}

type directorySync struct {
	cond                 *sync.Cond
	requested, completed uint64
	running              bool
	waiters              int
	errs                 map[uint64]error
}

func (d *directorySyncs) sync(path, root string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.dirs == nil {
		d.dirs = make(map[string]*directorySync)
	}
	s := d.dirs[path]
	if s == nil {
		s = &directorySync{cond: sync.NewCond(&d.mu), errs: make(map[uint64]error)}
		d.dirs[path] = s
	}
	s.waiters++
	s.requested++
	ticket := s.requested
	for s.completed < ticket {
		if s.running {
			s.cond.Wait()
			continue
		}
		s.running = true
		target := s.requested
		d.mu.Unlock()
		err := syncReclaimedDirectory(path, root)
		d.mu.Lock()
		s.running = false
		s.completed = target
		if err != nil {
			s.errs[target] = err
		}
		s.cond.Broadcast()
	}
	// The earliest sync whose generation reached this ticket covered it.
	var err error
	covering := uint64(0)
	for generation, failure := range s.errs {
		if generation >= ticket && (covering == 0 || generation < covering) {
			covering, err = generation, failure
		}
	}
	s.waiters--
	if s.waiters == 0 {
		delete(d.dirs, path)
	}
	return err
}

// conversionSlot bounds conversions inside file I/O. Each blocking file
// syscall pins an OS thread, and Go aborts at its thread limit; thousands of
// conversions released together by one ingestion group exceeded it. Waiting
// for a group holds no slot.
type conversionSlot chan struct{}

func (s conversionSlot) Release() { <-s }
func (s conversionSlot) Acquire() { s <- struct{}{} }

// ConversionIOSlots reports conversions inside file I/O and the slot count.
func (p *Whisper) ConversionIOSlots() (used, slots int) {
	return len(p.conversionIO), cap(p.conversionIO)
}

// SetConversionIOSlots bounds concurrent conversion file I/O; zero disables it.
// Configure it before conversions start.
func (p *Whisper) SetConversionIOSlots(n int) {
	if n > 0 {
		p.conversionIO = make(chan struct{}, n)
	}
}
