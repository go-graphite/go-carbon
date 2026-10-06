package carbonserver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

type snapshotOverlayManifest struct {
	Version int
	Base    string
	Records uint64
	File    snapshotFile
}

func (s *indexSnapshot) identity() string {
	data, _ := json.Marshal(s.manifest)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
func overlayManifestPath(cache string) string { return snapshotManifestPath(cache) + ".overlay" }
func overlayIdentity(m snapshotOverlayManifest) string {
	data, _ := json.Marshal(m)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
func validOverlayFile(name string) bool {
	return filepath.Base(name) == name && strings.HasPrefix(name, ".carbon-overlay-") && strings.HasSuffix(name, ".flc")
}

func readOverlayManifest(path string) (snapshotOverlayManifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return snapshotOverlayManifest{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 16385))
	if err != nil {
		return snapshotOverlayManifest{}, err
	}
	var m snapshotOverlayManifest
	if len(raw) > 16384 {
		return m, fmt.Errorf("snapshot overlay manifest oversized")
	}
	if err = json.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	if m.Version != 1 || !validOverlayFile(m.File.Name) {
		return m, fmt.Errorf("unsupported snapshot overlay")
	}
	return m, nil
}

func (ti *trieIndex) loadSnapshotOverlay(cache string) error {
	m, err := readOverlayManifest(overlayManifestPath(cache))
	if err != nil {
		return err
	}
	if m.Base != ti.snapshot.identity() {
		return fmt.Errorf("snapshot overlay belongs to another generation")
	}
	path := filepath.Join(filepath.Dir(cache), m.File.Name)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	digest := sha256.New()
	size, hashErr := io.Copy(digest, file)
	closeErr := file.Close()
	if err = errors.Join(hashErr, closeErr); err != nil {
		return err
	}
	var checksum [sha256.Size]byte
	copy(checksum[:], digest.Sum(nil))
	if size != m.File.Size || checksum != m.File.SHA256 {
		return fmt.Errorf("snapshot overlay checksum differs")
	}
	reader, err := NewFileListCache(path, FLCVersion2, 'r')
	if err != nil {
		return err
	}
	defer reader.Close()
	// Decode into a private overlay; an invalid tail must not publish its prefix.
	pending := newTrie(ti.fileExt, 0, ti.estimateSize)
	var count uint64
	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if _, err = pending.insert(entry.Path, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt); err != nil {
			return err
		}
		count++
	}
	if count != m.Records {
		return fmt.Errorf("snapshot overlay record count differs")
	}
	ti.root, ti.depth, ti.fileCount, ti.longestMetric = pending.root, pending.depth, pending.fileCount, pending.longestMetric
	ti.recoveryID = overlayIdentity(m)
	return nil
}

// CheckpointReadIndex is called after input/persistence stop and before closing
// read listeners. Index workers are stopped first; in-flight reads remain live.
func (l *CarbonserverListener) CheckpointReadIndex() (string, error) {
	l.PauseIndexUpdates()
	index := l.CurrentFileIndex()
	if index == nil || index.trieIdx == nil || index.trieIdx.snapshot == nil {
		return "", nil
	}
	ti := index.trieIdx
	l.drainRealtimeMetrics(ti)
	dir := filepath.Dir(l.fileListCache)
	placeholder, err := os.CreateTemp(dir, ".carbon-overlay-*.flc")
	if err != nil {
		return "", err
	}
	path := placeholder.Name()
	if err = placeholder.Close(); err != nil {
		return "", err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(path)
		}
	}()
	writer, err := NewFileListCache(path, FLCVersion2, 'w')
	if err != nil {
		return "", err
	}
	defer writer.Abort()
	names, nodes, _, _, _ := ti.allMetricsNodeMutable(ti.root, '.', "", int(^uint(0)>>1), false)
	for i, name := range names {
		m := nodes[i].meta.(*fileMeta)
		entry := FLCEntry{Path: "/" + strings.ReplaceAll(name, ".", "/") + ".wsp", LogicalSize: atomic.LoadInt64(&m.logicalSize), PhysicalSize: atomic.LoadInt64(&m.physicalSize), DataPoints: atomic.LoadInt64(&m.dataPoints), FirstSeenAt: atomic.LoadInt64(&m.firstSeenAt)}
		if err = writer.Write(&entry); err != nil {
			return "", err
		}
	}
	if err = writer.Close(); err != nil {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return "", err
	}
	digest := sha256.New()
	size, hashErr := io.Copy(digest, file)
	closeErr := file.Close()
	if err = errors.Join(hashErr, closeErr); err != nil {
		return "", err
	}
	m := snapshotOverlayManifest{Version: 1, Base: ti.snapshot.identity(), Records: uint64(len(names)), File: snapshotFile{Name: filepath.Base(path), Size: size}}
	copy(m.File.SHA256[:], digest.Sum(nil))
	old, _ := readOverlayManifest(overlayManifestPath(l.fileListCache))
	raw, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	manifest, err := os.CreateTemp(dir, ".carbon-overlay-manifest-*")
	if err != nil {
		return "", err
	}
	defer func() { _ = manifest.Close(); _ = os.Remove(manifest.Name()) }()
	if _, err = manifest.Write(raw); err != nil {
		return "", err
	}
	if err = manifest.Sync(); err != nil {
		return "", err
	}
	if err = manifest.Close(); err != nil {
		return "", err
	}
	if err = os.Rename(manifest.Name(), overlayManifestPath(l.fileListCache)); err != nil {
		return "", err
	}
	keep = true
	directory, err := os.Open(dir)
	if err != nil {
		return "", err
	}
	err = directory.Sync()
	_ = directory.Close()
	if err != nil {
		return "", err
	}
	if validOverlayFile(old.File.Name) && old.File.Name != m.File.Name {
		_ = os.Remove(filepath.Join(dir, old.File.Name))
	}
	return overlayIdentity(m), nil
}

// SavedMetricExists is used while writing a pending-point checkpoint to record
// only names absent from the immutable base. The small mutable overlay is saved
// separately by CheckpointReadIndex.
func (l *CarbonserverListener) SavedMetricExists(metric string) bool {
	index := l.CurrentFileIndex()
	if index == nil || index.trieIdx == nil || index.trieIdx.snapshot == nil {
		return false
	}
	_, found, err := index.trieIdx.snapshot.lookup("/" + strings.ReplaceAll(metric, ".", "/") + ".wsp")
	return err == nil && found
}
func (l *CarbonserverListener) HasMappedIndex() bool {
	index := l.CurrentFileIndex()
	return index != nil && index.trieIdx != nil && index.trieIdx.snapshot != nil
}
func (l *CarbonserverListener) RecoveryIndexID() string {
	index := l.CurrentFileIndex()
	if index == nil || index.trieIdx == nil {
		return ""
	}
	return index.trieIdx.recoveryID
}

// PreparePendingReadIndex runs after warmup and before Listen. It preserves the
// complete-index and initial-quota gates when saved points introduce new names.
func (l *CarbonserverListener) PreparePendingReadIndex(visit func(func(string) error) error) error {
	index := l.CurrentFileIndex()
	if index == nil || index.trieIdx == nil || index.trieIdx.snapshot == nil {
		return fmt.Errorf("mapped read index unavailable")
	}
	if err := visit(func(name string) error {
		_, err := index.trieIdx.insert("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0)
		return err
	}); err != nil {
		return err
	}
	l.refreshIndexQuotaAndUsage(index, nil)
	atomic.StoreUint64(&l.metrics.MetricsKnown, index.trieIdx.snapshot.manifest.Records+uint64(index.trieIdx.fileCount))
	return nil
}
