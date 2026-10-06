package carbonserver

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/blevesearch/mmap-go"
	"github.com/blevesearch/vellum"
)

const indexSnapshotVersion = 1

type snapshotFile struct {
	Name   string
	Size   int64
	SHA256 [sha256.Size]byte
}

type indexSnapshotManifest struct {
	Version         int
	Root            string
	Device, Inode   uint64
	Records         uint64
	Index, Metadata snapshotFile
	Source          snapshotSource
}

type snapshotSource struct {
	Device, Inode  uint64
	Size, Modified int64
}

func snapshotSourceIdentity(path string) (snapshotSource, error) {
	info, err := os.Stat(path)
	if err != nil {
		return snapshotSource{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() {
		return snapshotSource{}, fmt.Errorf("snapshot source identity is unavailable")
	}
	return snapshotSource{uint64(stat.Dev), uint64(stat.Ino), info.Size(), info.ModTime().UnixNano()}, nil
}

func snapshotManifestPath(fileListCache string) string {
	// Hidden names cannot be mistaken for legacy cache.<pid>.<time> dump files
	// when an installation puts its index and dumps in the same directory.
	return filepath.Join(filepath.Dir(fileListCache), "."+filepath.Base(fileListCache)+".snapshot.json")
}

func snapshotRootIdentity(root string) (string, uint64, uint64, error) {
	path, err := filepath.Abs(root)
	if err != nil {
		return "", 0, 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", 0, 0, err
	}
	if !info.IsDir() {
		return "", 0, 0, fmt.Errorf("snapshot data root is not a directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", 0, 0, fmt.Errorf("snapshot data root identity is unavailable")
	}
	return path, uint64(stat.Dev), uint64(stat.Ino), nil
}

// indexSnapshotWriter receives the same complete, ordered filesystem scan as
// FLCv2. It never replaces the legacy cache. A failed/incomplete scan aborts this
// optional accelerator, leaving the previous published generation intact.
type indexSnapshotWriter struct {
	fileListCache           string
	manifestPath            string
	manifest                indexSnapshotManifest
	indexFile, metaFile     *os.File
	indexHash, metaHash     hash.Hash
	indexBuffer, metaBuffer *bufio.Writer
	builder                 *vellum.Builder
	metadata                snapshotMetadataWriter
	key, previous           []byte
	closed                  bool
	failed                  error
}

func newIndexSnapshotWriter(fileListCache, root string) (_ *indexSnapshotWriter, err error) {
	w := &indexSnapshotWriter{fileListCache: fileListCache, manifestPath: snapshotManifestPath(fileListCache)}
	w.manifest.Version = indexSnapshotVersion
	w.manifest.Root, w.manifest.Device, w.manifest.Inode, err = snapshotRootIdentity(root)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = w.abort()
		}
	}()
	dir := filepath.Dir(w.manifestPath)
	w.indexFile, err = os.CreateTemp(dir, ".carbon-index-*.fst")
	if err != nil {
		return nil, err
	}
	w.metaFile, err = os.CreateTemp(dir, ".carbon-index-*.meta")
	if err != nil {
		return nil, err
	}
	w.indexHash, w.metaHash = sha256.New(), sha256.New()
	w.indexBuffer = bufio.NewWriterSize(io.MultiWriter(w.indexFile, w.indexHash), 1<<20)
	w.metaBuffer = bufio.NewWriterSize(io.MultiWriter(w.metaFile, w.metaHash), 1<<20)
	w.builder, err = vellum.New(w.indexBuffer, nil)
	if err != nil {
		return nil, err
	}
	w.metadata.w = w.metaBuffer
	return w, nil
}

// encodeSnapshotPath uses a separator smaller than every legal filename byte.
// This preserves filepath.Walk's directory-first lexical order without retaining
// and sorting the entire file list. FST builders require strictly ordered keys.
func encodeSnapshotPath(dst []byte, path string) ([]byte, error) {
	if strings.IndexByte(path, 0) >= 0 {
		return nil, fmt.Errorf("snapshot path contains NUL")
	}
	dst = append(dst[:0], path...)
	for i, b := range dst {
		if b == '/' {
			dst[i] = 0
		}
	}
	return dst, nil
}

func (w *indexSnapshotWriter) append(entry *FLCEntry) (err error) {
	if w.closed {
		return os.ErrClosed
	}
	if w.failed != nil {
		return w.failed
	}
	defer func() {
		if err != nil {
			w.failed = err
		}
	}()
	w.key, err = encodeSnapshotPath(w.key, entry.Path)
	if err != nil {
		return err
	}
	if len(w.key) == 0 || entry.Path[0] != '/' || !strings.HasSuffix(entry.Path, ".wsp") || strings.HasSuffix(entry.Path, "/.wsp") {
		return fmt.Errorf("snapshot requires a complete metric path")
	}
	if w.manifest.Records != 0 && bytes.Compare(w.previous, w.key) >= 0 {
		return fmt.Errorf("snapshot scan paths are not strictly ordered")
	}
	if err := w.builder.Insert(w.key, w.manifest.Records); err != nil {
		return err
	}
	if err := w.metadata.append([4]int64{entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt}); err != nil {
		return err
	}
	w.previous = append(w.previous[:0], w.key...)
	w.manifest.Records++
	return nil
}

func snapshotWrittenFile(file *os.File, digest hash.Hash) (snapshotFile, error) {
	info, err := file.Stat()
	if err != nil {
		return snapshotFile{}, err
	}
	result := snapshotFile{Name: filepath.Base(file.Name()), Size: info.Size()}
	copy(result.SHA256[:], digest.Sum(nil))
	return result, nil
}

func (w *indexSnapshotWriter) finish() (err error) {
	if w.closed {
		return os.ErrClosed
	}
	defer func() {
		if err != nil {
			_ = w.abort()
		}
	}()
	if w.failed != nil {
		return w.failed
	}
	w.manifest.Source, err = snapshotSourceIdentity(w.fileListCache)
	if err != nil {
		return err
	}
	if err = w.builder.Close(); err != nil {
		return err
	}
	if err = w.metadata.finish(); err != nil {
		return err
	}
	if err = errors.Join(w.indexBuffer.Flush(), w.metaBuffer.Flush()); err != nil {
		return err
	}
	if err = errors.Join(w.indexFile.Sync(), w.metaFile.Sync()); err != nil {
		return err
	}
	w.manifest.Index, err = snapshotWrittenFile(w.indexFile, w.indexHash)
	if err != nil {
		return err
	}
	w.manifest.Metadata, err = snapshotWrittenFile(w.metaFile, w.metaHash)
	if err != nil {
		return err
	}
	if err = errors.Join(w.indexFile.Close(), w.metaFile.Close()); err != nil {
		return err
	}
	data, err := json.Marshal(w.manifest)
	if err != nil {
		return err
	}
	previous, _ := readIndexSnapshotManifest(w.manifestPath)
	published, err := publishSnapshotManifest(w.manifestPath, data)
	if published {
		// Published files must survive a directory-sync error.
		w.closed = true
	}
	if err != nil {
		return err
	}
	// Already-open mappings remain valid on Unix after unlink. New readers see
	// the new manifest; a racing open of the old generation falls back safely.
	if previous != nil {
		removeSnapshotFiles(filepath.Dir(w.manifestPath), previous)
	}
	return nil
}

func publishSnapshotManifest(path string, data []byte) (published bool, err error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".carbon-index-manifest-*")
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	if _, err = f.Write(data); err != nil {
		return false, err
	}
	if err = f.Sync(); err != nil {
		return false, err
	}
	if err = f.Close(); err != nil {
		return false, err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return false, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return true, err
	}
	return true, errors.Join(dir.Sync(), dir.Close())
}

func (w *indexSnapshotWriter) abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var err error
	for _, file := range []*os.File{w.indexFile, w.metaFile} {
		if file != nil {
			err = errors.Join(err, file.Close(), os.Remove(file.Name()))
		}
	}
	return err
}

func validSnapshotFileName(name, extension string) bool {
	return filepath.Base(name) == name && strings.HasPrefix(name, ".carbon-index-") && strings.HasSuffix(name, extension)
}

func removeSnapshotFiles(dir string, manifest *indexSnapshotManifest) {
	for _, file := range []struct{ name, ext string }{{manifest.Index.Name, ".fst"}, {manifest.Metadata.Name, ".meta"}} {
		if validSnapshotFileName(file.name, file.ext) {
			_ = os.Remove(filepath.Join(dir, file.name))
		}
	}
}

func readIndexSnapshotManifest(path string) (*indexSnapshotManifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 16*1024+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 16*1024 {
		return nil, fmt.Errorf("snapshot manifest is oversized")
	}
	var manifest indexSnapshotManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if manifest.Version != indexSnapshotVersion || !validSnapshotFileName(manifest.Index.Name, ".fst") || !validSnapshotFileName(manifest.Metadata.Name, ".meta") {
		return nil, fmt.Errorf("unsupported index snapshot")
	}
	return &manifest, nil
}

type indexSnapshot struct {
	cleanup               runtime.Cleanup
	nodes                 snapshotQueryNodes
	openedAt              int64
	manifest              indexSnapshotManifest
	indexMap, metadataMap mmap.MMap
	index                 *vellum.FST
	metadata              *snapshotMetadata
}

func mapSnapshotFile(dir string, expected snapshotFile) (_ mmap.MMap, err error) {
	f, err := os.Open(filepath.Join(dir, expected.Name))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != expected.Size || info.Size() == 0 {
		return nil, fmt.Errorf("snapshot file size or type changed")
	}
	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(data) != expected.SHA256 {
		_ = data.Unmap()
		return nil, fmt.Errorf("snapshot checksum mismatch")
	}
	return data, nil
}

func openIndexSnapshot(fileListCache, root string) (_ *indexSnapshot, err error) {
	path := snapshotManifestPath(fileListCache)
	manifest, err := readIndexSnapshotManifest(path)
	if err != nil {
		return nil, err
	}
	root, device, inode, err := snapshotRootIdentity(root)
	if err != nil {
		return nil, err
	}
	if manifest.Root != root || manifest.Device != device || manifest.Inode != inode {
		return nil, fmt.Errorf("snapshot data root changed")
	}
	source, err := snapshotSourceIdentity(fileListCache)
	if err != nil {
		return nil, err
	}
	if source != manifest.Source {
		return nil, fmt.Errorf("snapshot source cache changed")
	}
	s := &indexSnapshot{manifest: *manifest, openedAt: time.Now().Unix()}
	defer func() {
		if err != nil {
			_ = s.close()
		}
	}()
	s.indexMap, err = mapSnapshotFile(filepath.Dir(path), manifest.Index)
	if err != nil {
		return nil, err
	}
	s.metadataMap, err = mapSnapshotFile(filepath.Dir(path), manifest.Metadata)
	if err != nil {
		return nil, err
	}
	s.index, err = vellum.Load(s.indexMap)
	if err != nil {
		return nil, err
	}
	s.metadata, err = openSnapshotMetadata(s.metadataMap)
	if err != nil {
		return nil, err
	}
	if uint64(s.index.Len()) != manifest.Records || s.metadata.count != manifest.Records {
		return nil, fmt.Errorf("snapshot record counts differ")
	}
	s.cleanup = runtime.AddCleanup(s, func(m snapshotMappings) { _ = m.close() }, snapshotMappings{s.index, s.indexMap, s.metadataMap})
	return s, nil
}

// The mappings outlive the last query holding this snapshot, including queries
// using an index retired by a concurrent scan. Cached result nodes own their
// metadata and never point into the mappings.
type snapshotMappings struct {
	index                 *vellum.FST
	indexMap, metadataMap mmap.MMap
}

func (m snapshotMappings) close() error {
	var err error
	if m.index != nil {
		err = m.index.Close()
	}
	if m.indexMap != nil {
		err = errors.Join(err, m.indexMap.Unmap())
	}
	if m.metadataMap != nil {
		err = errors.Join(err, m.metadataMap.Unmap())
	}
	return err
}

// Explicit close is only for unpublished snapshots or tests with drained readers.
// Published generations use the reachability cleanup above.
func (s *indexSnapshot) close() error {
	s.cleanup.Stop()
	err := (snapshotMappings{s.index, s.indexMap, s.metadataMap}).close()
	s.index, s.indexMap, s.metadataMap = nil, nil, nil
	return err
}

func (s *indexSnapshot) lookup(path string) (*FLCEntry, bool, error) {
	defer runtime.KeepAlive(s)
	key, err := encodeSnapshotPath(nil, path)
	if err != nil {
		return nil, false, err
	}
	row, ok, err := s.index.Get(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	values, err := s.metadata.get(row)
	if err != nil {
		return nil, false, err
	}
	return &FLCEntry{Path: path, LogicalSize: values[0], PhysicalSize: values[1], DataPoints: values[2], FirstSeenAt: values[3]}, true, nil
}
