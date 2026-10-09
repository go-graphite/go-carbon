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
	"runtime"
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

// readSnapshotOverlay builds privately so decoding can overlap validation of the
// immutable base. The caller must check the generation before installing it.
func (ti *trieIndex) readSnapshotOverlay(cache string) (_ *trieIndex, base string, err error) {
	m, err := readOverlayManifest(overlayManifestPath(cache))
	if err != nil {
		return nil, "", err
	}
	path := filepath.Join(filepath.Dir(cache), m.File.Name)
	file, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.New()
	size, hashErr := io.Copy(digest, file)
	closeErr := file.Close()
	if err = errors.Join(hashErr, closeErr); err != nil {
		return nil, "", err
	}
	var checksum [sha256.Size]byte
	copy(checksum[:], digest.Sum(nil))
	if size != m.File.Size || checksum != m.File.SHA256 {
		return nil, "", fmt.Errorf("snapshot overlay checksum differs")
	}
	reader, err := NewFileListCache(path, FLCVersion2, 'r')
	if err != nil {
		return nil, "", err
	}
	defer reader.Close()
	// Decode into a private overlay; an invalid tail must not publish its prefix.
	pending := newTrie(ti.fileExt, 0, ti.estimateSize)
	if ti.builder != nil {
		pending.builder = &trieBulkBuilder{}
	}
	var count uint64
	v2, _ := reader.(*fileListCacheV2)
	var entry FLCEntry
	for {
		var borrowed []byte
		if v2 != nil {
			borrowed, err = v2.readRecord(&entry)
		} else {
			var next *FLCEntry
			next, err = reader.Read()
			if next != nil {
				entry = *next
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, "", err
		}
		if v2 != nil {
			_, err = pending.insertMutableBytes(borrowed, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt)
		} else {
			_, err = pending.insert(entry.Path, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt)
		}
		if err != nil {
			return nil, "", err
		}
		count++
	}
	if count != m.Records {
		return nil, "", fmt.Errorf("snapshot overlay record count differs")
	}
	pending.recoveryID = overlayIdentity(m)
	return pending, m.Base, nil
}

func (ti *trieIndex) installSnapshotOverlay(pending *trieIndex, base string) error {
	if ti.snapshot == nil || base != ti.snapshot.identity() {
		return fmt.Errorf("snapshot overlay belongs to another generation")
	}
	ti.root, ti.depth, ti.fileCount, ti.longestMetric = pending.root, pending.depth, pending.fileCount, pending.longestMetric
	ti.builder = pending.builder
	ti.recoveryID = pending.recoveryID
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
		m := nodes[i].meta.Load().(*fileMeta)
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

// SavedMetricLookup captures the read generation after PauseIndexUpdates for
// serialized checkpoint construction. The saved overlay belongs to this same
// frozen generation, so its metrics need no reinsertion on restart. The returned
// function owns its reusable reader and key buffer and must be called serially.
func (l *CarbonserverListener) SavedMetricLookup() func(string) bool {
	return l.SavedMetricLookups()()
}

// SavedMetricLookups captures the frozen generation once and returns a factory
// of independent SavedMetricLookup functions, one per checkpoint worker.
func (l *CarbonserverListener) SavedMetricLookups() func() func(string) bool {
	index := l.CurrentFileIndex()
	if index == nil || index.trieIdx == nil || index.trieIdx.snapshot == nil {
		return func() func(string) bool { return func(string) bool { return false } }
	}
	return func() func(string) bool { return savedMetricLookup(index) }
}

func savedMetricLookup(index *fileIndex) func(string) bool {
	snapshot := index.trieIdx.snapshot
	reader, err := snapshot.index.Reader()
	if err != nil {
		return func(string) bool { return false }
	}
	key := make([]byte, 0, 256)
	return func(metric string) bool {
		defer runtime.KeepAlive(snapshot)
		key = append(key[:0], 0)
		for i := 0; i < len(metric); i++ {
			b := metric[i]
			if b == '.' {
				b = 0
			}
			key = append(key, b)
		}
		key = append(key, ".wsp"...)
		_, found, err := reader.Get(key)
		if err != nil || found {
			return err == nil && found
		}
		_, isNew := index.trieIdx.metricPathMutable(metric, nil)
		return !isNew
	}
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

// insertPendingMetric adds a name the checkpoint classified as absent from this
// exact snapshot generation (the caller matched its read-index ID), so the
// snapshot lookup in insert, ~6µs per name on large hosts, is skipped.
func (ti *trieIndex) insertPendingMetric(name string) error {
	_, err := ti.insertMutable("/"+strings.ReplaceAll(name, ".", "/")+".wsp", 0, 0, 0, 0)
	return err
}
