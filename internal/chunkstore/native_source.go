package chunkstore

import (
	"cmp"
	"context"
	"errors"
	"io"
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
)

// NativeSource pins only committed native history. Expiration and ordinary
// writes may continue; no application configuration or input fence is needed.
type NativeSource struct {
	mu           sync.RWMutex
	store        *Store
	view         *pebble.Snapshot
	closed       bool
	pending      map[string]struct{}
	checkCurrent bool
}

// SetNativeReady fences incomplete conversion/recovery without imposing
// expiration/config/pressure admission on a native snapshot.
func (s *Store) SetNativeReady(ready func() error) { s.nativeReady = ready }
func (s *Store) checkNativeReady() error {
	if s.nativeReady != nil {
		return s.nativeReady()
	}
	return nil
}

func (s *Store) PinNativeSource() (*NativeSource, error) {
	if err := errors.Join(s.checkNativeReady(), s.fileOwnershipError(), s.nativeDiskCheck()); err != nil {
		return nil, err
	}
	source := &NativeSource{store: s, view: s.db.NewSnapshot(), checkCurrent: s.nativePendingSnapshot != nil}
	if s.nativePendingSnapshot != nil {
		pending, err := s.nativePendingSnapshot()
		if err != nil {
			source.view.Close()
			return nil, err
		}
		source.pending = pending
	}
	return source, nil
}

// The callback is installed before listeners start. It captures queued source
// names once per pin; the export hot path never takes cache/metric locks.
func (s *Store) SetNativePendingSnapshot(snapshot func() (map[string]struct{}, error)) {
	s.nativePendingSnapshot = snapshot
}
func (s *NativeSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.view.Close()
}
func (s *NativeSource) Write(ctx context.Context, names []string, out io.Writer) error {
	ctx, cancel := s.store.NativeTransferContext(ctx)
	defer cancel()
	release, err := s.store.admitNativeExport(ctx)
	if err != nil {
		return err
	}
	defer release()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errors.New("native source session is closed")
	}
	if err := ValidateNativeNames(names); err != nil {
		return err
	}
	walk := &transferReader{reader: s.view, ordered: true, native: true, export: &s.store.transferExport}
	defer walk.close()
	order := make([]string, len(names))
	copy(order, names)
	slices.Sort(order)
	metas := make([]Metadata, len(names))
	metadataBytes := int64(0)
	for i, name := range order {
		if err := ctx.Err(); err != nil {
			return err
		}
		if i > 0 && name == order[i-1] {
			return errors.New("duplicate native name")
		}
		value, err := walk.lookup(&walk.catalog, catalogLower, catalogUpper, catalogKey(name))
		if errors.Is(err, ErrNotFound) {
			metas[i] = Metadata{MetricConfig: MetricConfig{Name: name}}
			continue
		}
		if err != nil {
			return err
		}
		m, err := decodeCatalog(name, value)
		if err != nil {
			return err
		}
		metadataBytes += nativeMetadataSize(m.MetricConfig)
		if metadataBytes > nativeRangeMetadataBytes {
			return ErrTransferLimit
		}
		metas[i] = m
	}
	slices.SortFunc(metas, func(a, b Metadata) int { return cmp.Compare(a.ID, b.ID) })
	var current *transferReader
	if s.checkCurrent {
		view := s.store.db.NewSnapshot()
		defer view.Close()
		current = &transferReader{reader: view, ordered: true, native: true}
		defer current.close()
	}
	for i, m := range metas {
		if m.ID == 0 {
			continue
		}
		m, err := walk.revision(m)
		if err != nil {
			return err
		}
		if err := s.store.checkFileOwnership(m); err != nil {
			return err
		}
		if m.FileMigration == FileCleanupPending {
			return ErrConflict
		}
		metas[i] = m
		if _, pending := s.pending[m.Name]; pending {
			metas[i] = Metadata{MetricConfig: MetricConfig{Name: m.Name}}
			continue
		}
		if current != nil {
			latest, err := current.revision(m)
			if errors.Is(err, ErrNotFound) || err == nil && transferVersion(latest) != transferVersion(m) {
				metas[i] = Metadata{MetricConfig: MetricConfig{Name: m.Name}}
				continue
			}
			if err != nil {
				return err
			}
		}
	}
	s.store.transferExport.requests.Add(1)
	s.store.transferExport.metrics.Add(uint64(len(names)))
	return writeTransferStream2(out, len(metas), func(i int) (TransferMetric, error) {
		if metas[i].ID == 0 {
			return TransferMetric{Config: metas[i].MetricConfig, Version: TransferVersion{Name: metas[i].Name}}, nil
		}
		walk.arena.free = walk.arena.slab
		return s.store.transferFromLimit(ctx, s.view, metas[i], nativeMetricBytes, walk)
	})
}
