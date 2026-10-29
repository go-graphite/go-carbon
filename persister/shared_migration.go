package persister

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// SetMirrorImportMissing permits verified import of an existing original during
// live migration. Configure it before workers start; ordinary mirrors refuse it.
func (p *Whisper) SetMirrorImportMissing(enabled bool) { p.mirrorImportMissing = enabled }

// ReconcileMirrorMetric serializes repair with the exact lock used by normal
// writes. A nonzero verifiedID can skip repeated repair only after verification
// of that ID in this writer session; current source existence is still checked.
// A missing original is confirmed before its obsolete shared entry is
// removed. The caller must keep this persister alive until the operation ends.
func (p *Whisper) ReconcileMirrorMetric(ctx context.Context, name string, verifiedID uint64) (store.Metadata, int, error) {
	if err := ctx.Err(); err != nil {
		return store.Metadata{}, 0, err
	}
	if !p.mirrorFiles || p.mirrorState == nil {
		return store.Metadata{}, 0, errors.New("live reconciliation requires rollback mirroring")
	}
	mutex := &p.storeMutex[storeMutexIndex(name)]
	for !mutex.TryLock() {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return store.Metadata{}, 0, ctx.Err()
		case <-timer.C:
		}
	}
	defer mutex.Unlock()
	if err := ctx.Err(); err != nil {
		return store.Metadata{}, 0, err
	}
	path, ok := p.whisperPath(name)
	if !ok {
		return store.Metadata{}, 0, errors.New("invalid original metric path")
	}
	if verifiedID != 0 {
		if _, err := os.Stat(path); err == nil {
			metadata, err := p.metricStore.Metadata(ctx, name)
			_, pending := p.mirrorState.pending.Load(name)
			if err == nil && metadata.ID == verifiedID && !pending {
				return metadata, 0, nil
			}
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return store.Metadata{}, 0, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return store.Metadata{}, 0, err
		}
	}
	if p.mirrorState.BaselineCopy != "" {
		metadata, err := p.metricStore.Metadata(ctx, name)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return store.Metadata{}, 0, err
		}
		_, pending := p.mirrorState.pending.Load(name)
		if err == nil && !pending && metadata.ID <= p.mirrorState.BaselineIDLimit && metadata.Generation == 1 && metadata.Revision == 1 {
			relative, err := filepath.Rel(p.rootPath, path)
			if err != nil || !filepath.IsLocal(relative) {
				return store.Metadata{}, 0, errors.New("invalid baseline metric path")
			}
			same, err := whisperio.SameCapture(ctx, path, filepath.Join(p.mirrorState.BaselineCopy, relative))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return store.Metadata{}, 0, err
			}
			if same {
				p.mirrorState.BaselineSkipped.Add(1)
				return metadata, 0, nil
			}
		}
	}
	metadata, count, err := p.replaceMirror(ctx, name, path, true)
	if errors.Is(err, os.ErrNotExist) {
		if _, sourceErr := os.Stat(path); errors.Is(sourceErr, os.ErrNotExist) {
			remover, ok := p.metricStore.(interface {
				Delete(context.Context, string) error
			})
			if !ok {
				return store.Metadata{}, 0, errors.New("live reconciliation requires catalog deletion")
			}
			err = remover.Delete(ctx, name)
			if errors.Is(err, store.ErrNotFound) {
				err = nil
			}
			if err == nil {
				p.mirrorState.pending.Delete(name)
			}
			return store.Metadata{}, 0, err
		}
	}
	if err == nil {
		p.mirrorState.pending.Delete(name)
	}
	return metadata, count, err
}

// prepareSharedMetric is called under the metric store mutex before acquiring
// the original file handle: taking a second inode lock while holding the first
// would deadlock. No cache batch has been popped at this point.
func (p *Whisper) prepareSharedMetric(ctx context.Context, metric string) (store.Metadata, error) {
	metadata, err := p.metricStore.Metadata(ctx, metric)
	if !p.mirrorImportMissing || !errors.Is(err, store.ErrNotFound) {
		return metadata, err
	}
	path, ok := p.whisperPath(metric)
	if !ok {
		return store.Metadata{}, errors.New("invalid original metric path")
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return metadata, store.ErrNotFound
		}
		return store.Metadata{}, err
	}
	if err := p.repairMirror(ctx, metric, path); err != nil {
		return store.Metadata{}, err
	}
	return p.metricStore.Metadata(ctx, metric)
}

// HasPendingMirrors is checked after draining writers before read publication.
func (p *Whisper) HasPendingMirrors() bool {
	if p.mirrorState == nil {
		return false
	}
	pending := false
	p.mirrorState.pending.Range(func(_, _ any) bool { pending = true; return false })
	return pending
}
