package carbonserver

import (
	"errors"
	"iter"
)

var errRestorePending = errors.New("metric restored history is still being persisted")

// SetRestorePending gates reads independently of intake. In particular, coarse
// archives do not overlay raw cache points, so they must not silently serve a
// partial restored history. Configure before Listen.
func (l *CarbonserverListener) SetRestorePending(pending func(string) bool) {
	l.restorePending = pending
}

func (l *CarbonserverListener) checkRestoreGlobs(groups map[string]globs) error {
	if l.restorePending == nil {
		return nil
	}
	for _, group := range groups {
		for i, name := range group.Files {
			if group.Leafs[i] && l.restorePending(name) {
				return errRestorePending
			}
		}
	}
	return nil
}

// PrepareRestoreIndex publishes uncreated restored names and their estimated
// storage/point quota usage before live admission. Called before Listen and the
// periodic index refresher; the store catalog remains authoritative afterwards.
func (l *CarbonserverListener) PrepareRestoreIndex(names iter.Seq[string]) error {
	if err := l.RefreshMetricStoreIndex(); err != nil {
		return err
	}
	index := l.CurrentFileIndex()
	if (index == nil || index.trieIdx == nil) && l.metricStoreFileIndex {
		// Mixed storage discovers unconverted originals from the file tree.
		// Warmup only reads a saved index; if it is absent, build the ordinary
		// mixed index before admitting input rather than replacing it with a
		// catalog that omits originals. Listen has not started any refresher.
		l.updateFileList(l.whisperData, nil, nil)
		index = l.CurrentFileIndex()
	}
	if index == nil || index.trieIdx == nil {
		return errors.New("restore intake requires a ready trie index")
	}
	for name := range names {
		if l.MetricExists(name) {
			continue
		}
		path, ok := l.storableMetricPath(name)
		if !ok {
			continue
		}
		logical, physical, points := int64(0), int64(0), int64(0)
		if l.estimateSize != nil {
			logical, physical, points = l.estimateSize(name)
		}
		if _, err := index.trieIdx.insert(path, logical, physical, points, 0); err != nil {
			return err
		}
	}
	if l.isQuotaEnabled() {
		return l.applySharedStoreQuotas(index.trieIdx)
	}
	return nil
}
