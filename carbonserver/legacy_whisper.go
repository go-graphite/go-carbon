package carbonserver

import (
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
)

// SetLegacyWhisperDisabled retires source I/O after durable source completion.
// It is safe while reads and an earlier filesystem scan are finishing.
func (l *CarbonserverListener) SetLegacyWhisperDisabled(disabled bool) {
	l.legacyWhisperDisabled.Store(disabled)
	// Stop new source operations and let in-flight reads/scans leave their gates.
	l.legacyWhisperMu.Lock()
	defer l.legacyWhisperMu.Unlock()
	if disabled {
		atomic.StoreUint64(&l.metrics.LockFiles, 0)
		atomic.StoreUint64(&l.metrics.OOOFiles, 0)
		atomic.StoreUint64(&l.metrics.OOOPhysicalBytes, 0)
	}
}

// Report a source discovered by an in-flight scan after completion. Completed
// stores never probe the old tree on cache misses just to look for violations.
func (l *CarbonserverListener) unexpectedWhisperFile(path string) {
	if strings.HasSuffix(path, ".wsp") {
		atomic.AddUint64(&l.metrics.UnexpectedWhisperFiles, 1)
		if l.prometheus.enabled {
			l.prometheus.unexpectedWhisperFiles.Inc()
		}
		l.logger.Error("unexpected Whisper file after source completion; refusing legacy I/O", zap.String("path", path))
	}
}

// Shared disk accounting belongs to the destination after source retirement.
func (l *CarbonserverListener) storageFilesystemPath() string {
	if db := l.getMetricStore(); db != nil && (l.legacyWhisperDisabled.Load() || l.getMetricIndexStore() != nil) {
		return db.Directory()
	}
	return l.whisperData
}

// Source completion changes disk accounting, not the identity of an existing
// saved catalog. Older generations are rooted at whisperData; validate those
// with the same path/device/inode and checksums they were written with. Newly
// built catalogs use the store root and need no source access on later starts.
func (l *CarbonserverListener) openSavedIndexSnapshot() (*indexSnapshot, error) {
	root := l.storageFilesystemPath()
	snapshot, err := openIndexSnapshot(l.fileListCache, root)
	if err == nil || root == l.whisperData {
		return snapshot, err
	}
	legacy, legacyErr := openIndexSnapshot(l.fileListCache, l.whisperData)
	if legacyErr == nil {
		l.logger.Info("reusing saved index with Whisper root identity")
		return legacy, nil
	}
	return nil, err
}
