package carbonserver

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dgryski/go-trigram"
	"github.com/go-graphite/go-carbon/internal/v2index"
	"go.uber.org/zap"
)

// v2CatalogBinding builds off the catalog writer path and pins each find image.
// Only index builders serialize; catalog publishers and existing readers don't.
type v2CatalogBinding struct {
	source v2index.Source
	mu     sync.Mutex
	image  atomic.Pointer[v2CatalogImage]
}
type v2CatalogImage struct {
	snapshot *v2index.Snapshot
	listener *CarbonserverListener
}

// SetV2Catalog attaches a live host-wide catalog before serving requests. The
// source owns admission/expiry fencing and durable recovery. Discovery uses the
// configured classic backend, with no filesystem fallback or stale-result TTL.
// Passing nil detaches it. Call before Start, like the other listener setters.
func (l *CarbonserverListener) SetV2Catalog(source v2index.Source) error {
	if source == nil {
		l.v2Catalog.Store(nil)
		return nil
	}
	binding := &v2CatalogBinding{source: source}
	if _, err := binding.load(l); err != nil {
		return err
	}
	l.v2Catalog.Store(binding)
	return nil
}

func (b *v2CatalogBinding) load(l *CarbonserverListener) (*v2CatalogImage, error) {
	snapshot := b.source.Snapshot()
	if snapshot == nil {
		return nil, errors.New("v2 index: nil catalog snapshot")
	}
	if image := b.image.Load(); image != nil && image.snapshot.Revision() == snapshot.Revision() {
		return image, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Re-read after waiting so a delayed query cannot republish an older image.
	snapshot = b.source.Snapshot()
	if snapshot == nil {
		return nil, errors.New("v2 index: nil catalog snapshot")
	}
	if image := b.image.Load(); image != nil && image.snapshot.Revision() == snapshot.Revision() {
		return image, nil
	}
	next, err := buildV2CatalogImage(snapshot, l)
	if err != nil {
		return nil, err
	}
	b.image.Store(next)
	return next, nil
}

// Ported from the parts operational-parity adapter. Only the name projection is
// needed here: quotas and physical metadata remain with their existing owners.
func buildV2CatalogImage(snapshot *v2index.Snapshot, parent *CarbonserverListener) (*v2CatalogImage, error) {
	l := &CarbonserverListener{logger: zap.NewNop(), accessLogger: zap.NewNop(), trieIndex: parent.trieIndex, trigramIndex: !parent.trieIndex, maxGlobs: parent.maxGlobs, maxMetricsGlobbed: parent.maxMetricsGlobbed, failOnMaxGlobs: parent.failOnMaxGlobs}
	fidx := &fileIndex{}
	if l.trieIndex {
		fidx.trieIdx = newTrie(".wsp", 0, nil)
	}
	// Sorted canonical names make paths for common parents adjacent. Remembering
	// the last path per depth avoids a second million-entry namespace map.
	var last []string
	for name := range snapshot.Names() {
		path := metricStoreVirtualPath(name)
		if l.trieIndex {
			if _, err := fidx.trieIdx.insert(path, 0, 0, 0, 0); err != nil {
				return nil, fmt.Errorf("v2 index %q: %w", name, err)
			}
		} else {
			paths := metricStoreTrigramPaths(path)
			for depth, p := range paths {
				if depth >= len(last) {
					last = append(last, p)
					fidx.files = append(fidx.files, p)
				} else if last[depth] != p {
					last[depth] = p
					fidx.files = append(fidx.files, p)
				}
			}
		}
	}
	if !l.trieIndex {
		fidx.idx = trigram.NewIndex(fidx.files)
		fidx.idx.Prune(0.95)
	}
	l.UpdateFileIndex(fidx)
	return &v2CatalogImage{snapshot: snapshot, listener: l}, nil
}

func (l *CarbonserverListener) expandV2Catalog(binding *v2CatalogBinding, query string) *ExpandedGlobResponse {
	image, err := binding.load(l)
	if err != nil {
		return &ExpandedGlobResponse{Name: query, Err: err}
	}
	pinned := image.listener
	if pinned.trieIndex {
		files, leafs, nodes, lookups, err := pinned.expandGlobsTrie(query)
		return &ExpandedGlobResponse{query, files, leafs, nodes, lookups, err}
	}
	globs, err := pinned.globPatterns(query, pinned.logger)
	if err != nil {
		return &ExpandedGlobResponse{Name: query, Err: err}
	}
	files := pinned.matchGlobIndex(pinned.CurrentFileIndex(), globs, false)
	leafs := make([]bool, len(files))
	for i, path := range files {
		files[i], leafs[i] = pinned.normalizeGlobFile(path, nil)
	}
	return &ExpandedGlobResponse{query, files, leafs, nil, 0, nil}
}
