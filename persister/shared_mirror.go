package persister

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type mirrorSnapshotReplacer interface {
	Replace(context.Context, store.Snapshot) (store.Metadata, error)
}

// MirrorState survives persister replacement on config reload. Entries exist
// only after a partial/failed mirrored batch; normal writes retain no per-metric
// state. A process restart is protected by the durable application guard.
type MirrorState struct {
	ScratchDir      string
	BaselineCopy    string
	BaselineIDLimit uint64
	BaselineSkipped atomic.Uint64
	pending         sync.Map
}

func (p *Whisper) SetMirrorState(state *MirrorState) { p.mirrorState = state }

func (p *Whisper) mirrorFailed(metric string) {
	if p.mirrorFiles {
		p.mirrorState.pending.Store(metric, struct{}{})
	}
}

// The caller still holds the persister's metric lock. Capture obtains the
// original's cross-process path/inode locks only for the copy, then decodes the
// private copy. Normal mirrored writes do not scan the historical archives.
func (p *Whisper) repairMirror(ctx context.Context, name, path string) error {
	_, _, err := p.replaceMirror(ctx, name, path, false)
	return err
}

// replaceMirror is shared by exceptional batch repair and verified migration.
// The caller holds exactly one persister metric mutex throughout the operation.
func (p *Whisper) replaceMirror(ctx context.Context, name, path string, verify bool) (store.Metadata, int, error) {
	db, ok := p.metricStore.(mirrorSnapshotReplacer)
	if !ok {
		return store.Metadata{}, 0, errors.New("rollback mirror requires archive-preserving replacement")
	}
	base := p.mirrorState.ScratchDir
	if base == "" {
		base = filepath.Dir(filepath.Clean(p.rootPath))
	}
	// Keep private .wsp copies outside the original tree: a crash must not
	// expose a scratch metric to the stock file scanner during rollback.
	dir, err := os.MkdirTemp(base, ".mirror-repair-")
	if err != nil {
		return store.Metadata{}, 0, err
	}
	defer os.RemoveAll(dir)
	copyPath := filepath.Join(dir, "metric.wsp")
	if err := whisperio.Capture(ctx, path, copyPath, false); err != nil {
		return store.Metadata{}, 0, err
	}
	snapshot, err := whisperio.ReadSnapshotIn(ctx, name, copyPath, dir)
	if err != nil {
		return store.Metadata{}, 0, err
	}
	return p.reconcileMirrorSnapshot(ctx, db, snapshot, verify)
}

func (p *Whisper) reconcileMirrorSnapshot(ctx context.Context, db mirrorSnapshotReplacer, snapshot store.Snapshot, verify bool) (store.Metadata, int, error) {
	var err error
	var metadata store.Metadata
	var unchanged bool
	if checked, ok := p.metricStore.(interface {
		ReconcileSnapshotStatus(context.Context, store.Snapshot) (store.Metadata, bool, error)
	}); verify && ok {
		metadata, unchanged, err = checked.ReconcileSnapshotStatus(ctx, snapshot)
	} else if differential, ok := p.metricStore.(interface {
		ReconcileSnapshot(context.Context, store.Snapshot) (store.Metadata, error)
	}); verify && ok {
		metadata, err = differential.ReconcileSnapshot(ctx, snapshot)
	} else {
		metadata, err = db.Replace(ctx, snapshot)
	}
	if err != nil {
		return store.Metadata{}, 0, err
	}
	if verify && !unchanged {
		reader, ok := p.metricStore.(interface {
			Snapshot(context.Context, string) (store.Snapshot, error)
		})
		if !ok {
			return store.Metadata{}, 0, errors.New("rollback migration requires snapshot verification")
		}
		actual, err := reader.Snapshot(ctx, snapshot.Metadata.Name)
		if err != nil {
			return store.Metadata{}, 0, err
		}
		if err = store.EqualSnapshots(snapshot, actual); err != nil {
			return store.Metadata{}, 0, err
		}
	}
	count := 0
	for _, a := range snapshot.Archives {
		count += len(a.Points)
	}
	return metadata, count, nil
}
