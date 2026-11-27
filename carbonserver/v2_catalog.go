package carbonserver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/go-graphite/go-carbon/internal/v2index"
	"go.uber.org/zap"
)

// v2CatalogBinding pins immutable catalog publications without a build mutex.
type v2CatalogBinding struct {
	source v2index.Source
	fence  func(context.Context, ...string) error
	image  atomic.Pointer[v2CatalogImage]
}
type v2CatalogImage struct{ snapshot *v2index.Snapshot }

// SetV2Catalog attaches the published name projection. Requests only pin a
// snapshot; the optional fence validates selected owners before returning names.
// Neither attaching nor querying constructs another whole-name index.
func (l *CarbonserverListener) SetV2Catalog(source v2index.Source, fence ...func(context.Context, ...string) error) error {
	if source == nil {
		l.v2Catalog.Store(nil)
		return nil
	}
	binding := &v2CatalogBinding{source: source}
	if len(fence) > 0 {
		binding.fence = fence[0]
	}
	if _, err := binding.load(l); err != nil {
		return err
	}
	l.v2Catalog.Store(binding)
	return nil
}
func (b *v2CatalogBinding) load(_ *CarbonserverListener) (*v2CatalogImage, error) {
	snapshot := b.source.Snapshot()
	if snapshot == nil {
		return nil, errors.New("v2 index: nil catalog snapshot")
	}
	image := &v2CatalogImage{snapshot: snapshot}
	b.image.Store(image)
	return image, nil
}

func (l *CarbonserverListener) expandV2Catalog(binding *v2CatalogBinding, query string) *ExpandedGlobResponse {
	return l.expandV2CatalogContext(context.Background(), binding, query)
}
func (l *CarbonserverListener) expandV2CatalogContext(ctx context.Context, binding *v2CatalogBinding, query string) *ExpandedGlobResponse {
	image, err := binding.load(l)
	if err != nil {
		return &ExpandedGlobResponse{Name: query, Err: err}
	}
	logger := l.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	var globs []string
	if l.trieIndex {
		normalized := strings.TrimSpace(query)
		if normalized == "" {
			normalized = "*"
		}
		globs, err = l.expandGlobBraces([]string{strings.ReplaceAll(normalized, ".", "/")})
	} else {
		globs, err = l.globPatterns(query, logger)
	}
	if err != nil {
		return &ExpandedGlobResponse{Name: query, Err: err}
	}
	paths := make(map[string]struct{})
	for _, glob := range globs {
		if !l.trieIndex {
			err = image.snapshot.MatchPaths(ctx, glob, func(path string) error { paths[path] = struct{}{}; return nil })
		} else {
			parts := strings.Split(glob, "/")
			matchers := make([]*gmatcher, len(parts))
			for i, part := range parts {
				matchers[i], err = newGlobState(part, l.expandGlobBraces)
				if err != nil {
					return &ExpandedGlobResponse{Name: query, Err: err}
				}
			}
			err = image.snapshot.VisitPaths(ctx, glob, func(path string) error {
				candidate := strings.TrimSuffix(strings.TrimPrefix(path, "/"), ".wsp")
				components := strings.Split(candidate, "/")
				if len(components) != len(matchers) {
					return nil
				}
				for i, component := range components {
					state := matchers[i].dstates[0]
					for j := 0; j < len(component); j++ {
						state = state.step(component[j])
						if len(state.gstates) == 0 {
							return nil
						}
					}
					if !state.matched() {
						return nil
					}
				}
				paths[path] = struct{}{}
				return nil
			})
		}
		if err != nil {
			return &ExpandedGlobResponse{Name: query, Err: err}
		}
	}
	files := make([]string, 0, len(paths))
	for path := range paths {
		files = append(files, path)
	}
	slices.Sort(files)
	if l.trieIndex {
		slices.SortFunc(files, func(a, b string) int {
			aa, bb := strings.TrimSuffix(a, ".wsp"), strings.TrimSuffix(b, ".wsp")
			if n := strings.Compare(aa, bb); n != 0 {
				return n
			}
			// Classic trie emits a leaf before the directory of the same name.
			return -strings.Compare(a, b)
		})
		limit := max(1, l.maxMetricsGlobbed)
		if len(files) > limit {
			if strings.TrimSuffix(files[limit-1], ".wsp") == strings.TrimSuffix(files[limit], ".wsp") {
				limit++
			}
			files = files[:limit]
		}
	}
	leafs := make([]bool, len(files))
	for i, path := range files {
		leafs[i] = strings.HasSuffix(path, ".wsp")
		files[i] = strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(path, "/"), ".wsp"), "/", ".")
	}
	if binding.fence != nil {
		// Leaf matches select their physical owners. Directories and negative
		// lookups may touch any owner, so they require the complete fence.
		names := files
		for _, leaf := range leafs {
			if !leaf {
				names = nil
				break
			}
		}
		if err := binding.fence(ctx, names...); err != nil {
			return &ExpandedGlobResponse{Name: query, Err: err}
		}
	}
	return &ExpandedGlobResponse{query, files, leafs, nil, 0, nil}
}
