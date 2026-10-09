package carbonserver

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// file list cache
//
// why supporting different versions: backward compatibility.
//
// principles:
//   * higher version cache logic should gracefully start from lower version
//     and upgrade it properly.
//   * if users decide to go back to the old cache logic (like going back from
//     v2 to v1), they need to manually remove the existing cache. otherwise,
//     we would have an undefined behavior in go-carbon (most likely things
//     are going to be fine, but there is no guarantee).
//
// v1 cache contains a simple list of directory and whisper file paths separated
// by a new line ('\n'), and it's also gzip-ed.
//
// v2 cache contains list of whisper file paths, each path also followed by 3
// stats: logical size, physical size, number of data points. v2 cache is also
// gzipped. each entry to write to disk using the following format:
//
//   [8 bytes: len($whisper_file_path)]
//   [$whisper_file_path]
//   [8 bytes: logical_size]
//   [8 bytes: physical_size]
//   [8 bytes: data_points]
//   [8 bytes: first_seen_at]
//   [1 byte:  \n (0x0a)]

const (
	// All currently supported flc version numbers.
	FLCVersionUnspecified FLCVersion = 0
	FLCVersion1           FLCVersion = 1
	FLCVersion2           FLCVersion = 2

	// note: there is no v1 magic string as it wasn't designed in the first place.
	version2MagicString = "\x7fflc02"

	flcv2StatFieldSize      = 8
	flcv2StatFieldCount     = 4
	flcv2EntrySeparatorSize = 1
	flcv2EntryStatLen       = flcv2StatFieldSize*flcv2StatFieldCount + flcv2EntrySeparatorSize
)

// FLC: file list cache
type FLCVersion int

// FileListCache lets users interact with the file list cache file.
type FileListCache interface {
	GetVersion() FLCVersion
	Write(*FLCEntry) error
	Read() (*FLCEntry, error)
	Close() error
	Abort() error
}

// FLCEntry is an entry in the file list cache.
type FLCEntry struct {
	// Shared by both v1 and v2
	Path string

	// V2 only fields
	LogicalSize, PhysicalSize, DataPoints int64
	// Caveat: this is a best effort feature, please treat it with caution.
	// In theory, this value could be interpreted as file/metric creation time.
	FirstSeenAt int64
}

type fileListCacheCommon struct {
	version FLCVersion
	path    string
	mode    byte
	file    *os.File
	reader  *gzip.Reader
	writer  *gzip.Writer
}

type fileListCacheV1 struct {
	*fileListCacheCommon
	scanner *bufio.Scanner
}

type fileListCacheV2 struct {
	*fileListCacheCommon
	pathLength  [flcv2StatFieldSize]byte
	entryBuffer []byte
}

// ReadFileListCache dumps cache data in csv to writer.
func ReadFileListCache(p string, version FLCVersion, writer io.Writer) error {
	flc, err := NewFileListCache(p, version, 'r')
	if err != nil {
		return err
	}
	defer flc.Close()

	switch flc.GetVersion() {
	case FLCVersion1:
		fmt.Fprintf(writer, "path\n")
	case FLCVersion2:
		fmt.Fprintf(writer, "path,logical_size,physical_size,data_points,first_seen_at\n")
	}

	for {
		entry, err := flc.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		switch flc.GetVersion() {
		case FLCVersion1:
			fmt.Fprintf(writer, "%s\n", entry.Path)
		case FLCVersion2:
			fmt.Fprintf(writer, "%s,%d,%d,%d,%d\n", entry.Path, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt)
		}
	}
}

// NewFileListCache returns a FileListCache.
//
// Supported mode values are: 'r' for read only, 'w' for write only.
func NewFileListCache(p string, version FLCVersion, mode byte) (FileListCache, error) {
	var flc FileListCache
	var flcc = &fileListCacheCommon{
		version: version,
		path:    p,
		mode:    mode,
	}

	var err error
	switch flcc.mode {
	case 'r':
		flcc.file, err = os.Open(p)
		if err != nil {
			return nil, err
		}
		flcc.reader, err = gzip.NewReader(flcc.file)
		if err != nil {
			return nil, err
		}

		// FLCVersionUnspecified: if possible, for future flc versions,
		// go-carbon should be able to transparently detect versions of
		// the cache file.
		switch version {
		case FLCVersion1:
			flc = newFileListCacheV1ReadOnly(flcc)
		case FLCVersionUnspecified, FLCVersion2:
			flcc.version = FLCVersion2                  // for supporting FLCVersionUnspecified
			flc, err = newFileListCacheV2ReadOnly(flcc) // flcc is already closed if there is error.
			if errors.Is(err, errFLCFallbackToV1) {
				// transparently detect which cache version is it.
				return NewFileListCache(p, FLCVersion1, mode)
			}
		default:
			return nil, fmt.Errorf("unknown file list cache version: %d", flcc.version)
		}
	case 'w':
		flcc.file, err = os.Create(p + ".tmp")
		if err != nil {
			return nil, err
		}
		flcc.writer = gzip.NewWriter(flcc.file)

		// FLCVersionUnspecified is not supported here because go-carbon
		// need to know what version of flc it needs to generate.
		switch version {
		case FLCVersion1:
			flc = &fileListCacheV1{fileListCacheCommon: flcc}
		case FLCVersion2:
			flc = &fileListCacheV2{fileListCacheCommon: flcc}

			if _, err := flc.(*fileListCacheV2).writer.Write([]byte(version2MagicString)); err != nil {
				flcc.file.Close()

				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown file list cache version: %d", flcc.version)
		}
	}

	return flc, err
}

// GetVersion returns the version of the file list cache file.
func (flc *fileListCacheCommon) GetVersion() FLCVersion { return flc.version }

// Abort closes an unfinished cache without replacing the previous snapshot.
func (flc *fileListCacheCommon) Abort() error {
	err := flc.file.Close()
	if flc.mode == 'w' {
		if removeErr := os.Remove(flc.path + ".tmp"); err == nil {
			err = removeErr
		}
	}
	return err
}

// Closes close the cache file and related writers and readers.
// For 'w' mode, it also performs a `mv tmp_cache_file cache_file`.
func (flc *fileListCacheCommon) Close() error {
	var errs []string
	if flc.mode == 'w' {
		if err := flc.writer.Flush(); err != nil {
			errs = append(errs, fmt.Sprintf("gzip.flush: %s", err))
		}
		if err := flc.writer.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("gzip.close: %s", err))
		}
		if err := flc.file.Sync(); err != nil {
			errs = append(errs, fmt.Sprintf("file.sync: %s", err))
		}
	}
	if err := flc.file.Close(); err != nil {
		errs = append(errs, fmt.Sprintf("file.sync: %s", err))
	}

	if flc.mode == 'w' && len(errs) == 0 {
		if err := os.Rename(flc.path+".tmp", flc.path); err != nil {
			errs = append(errs, fmt.Sprintf("file.rename: %s", err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ";"))
	}

	return nil
}

func newFileListCacheV1ReadOnly(flcc *fileListCacheCommon) *fileListCacheV1 {
	flc := fileListCacheV1{fileListCacheCommon: flcc}
	flc.scanner = bufio.NewScanner(flc.reader)
	return &flc
}

func (flc *fileListCacheV1) Write(entry *FLCEntry) error {
	_, err := flc.writer.Write([]byte(entry.Path + "\n"))
	return err
}

func (flc *fileListCacheV1) Read() (entry *FLCEntry, err error) {
	eof := flc.scanner.Scan()
	if !eof {
		err = io.EOF
		return
	}

	return &FLCEntry{Path: flc.scanner.Text()}, nil
}

var errFLCFallbackToV1 = errors.New("fallback to flc v1")

func newFileListCacheV2ReadOnly(flcc *fileListCacheCommon) (*fileListCacheV2, error) {
	flc := fileListCacheV2{
		fileListCacheCommon: flcc,
	}

	magic := make([]byte, len(version2MagicString))
	switch n, err := flc.reader.Read(magic); {
	case err != nil:
		return nil, err
	case n != len(version2MagicString):
		return nil, fmt.Errorf("failed to read full v2 magic string (%d): %d", len(version2MagicString), n)
	case !bytes.Equal(magic, []byte(version2MagicString)):
		// ignoring error here. this path is only run
		// once by go-carbon. not an actual concern.
		flc.Close()

		// v1 cache detected, falls back to v1 data
		return nil, errFLCFallbackToV1
	}

	return &flc, nil
}

func (flc *fileListCacheV2) Write(entry *FLCEntry) error {
	flc.entryBuffer = appendFLCv2Entry(flc.entryBuffer[:0], entry.Path, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt)
	_, err := flc.writer.Write(flc.entryBuffer)

	return err
}

func appendFLCv2Entry[P string | []byte](dst []byte, path P, logicalSize, physicalSize, dataPoints, firstSeenAt int64) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(len(path)))
	dst = append(dst, path...)
	dst = binary.BigEndian.AppendUint64(dst, uint64(logicalSize))
	dst = binary.BigEndian.AppendUint64(dst, uint64(physicalSize))
	dst = binary.BigEndian.AppendUint64(dst, uint64(dataPoints))
	dst = binary.BigEndian.AppendUint64(dst, uint64(firstSeenAt))
	return append(dst, '\n')
}

func (flc *fileListCacheV2) Read() (entry *FLCEntry, err error) {
	entry = new(FLCEntry)
	if err = flc.readInto(entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (flc *fileListCacheV2) buffer(size int) []byte {
	if cap(flc.entryBuffer) < size {
		flc.entryBuffer = make([]byte, size)
	}
	return flc.entryBuffer[:size]
}

// readInto reuses the record and decode buffers during startup. Path still owns
// its bytes: the trie may retain it as longestMetric after the next read.
func (flc *fileListCacheV2) readInto(entry *FLCEntry) error {
	path, err := flc.readRecord(entry)
	if err != nil {
		return err
	}
	entry.Path = string(path)
	return nil
}

// readRecord borrows the path until the next read. Read/readInto retain their
// owned-string contract; only synchronous index construction uses this method.
func (flc *fileListCacheV2) readRecord(entry *FLCEntry) ([]byte, error) {
	if _, err := io.ReadFull(flc.reader, flc.pathLength[:]); err != nil {
		return nil, fmt.Errorf("flcv2: failed to read path len: %w", err)
	}
	pathLength := binary.BigEndian.Uint64(flc.pathLength[:])

	// filepath on linux has a 4k limit, but we are timing it 2 here just to
	// be flexible and avoid bugs or corruptions to causes panics or oom in
	// go-carbon
	//
	// * https://man7.org/linux/man-pages/man3/realpath.3.html#NOTES
	// * https://www.ibm.com/docs/en/spectrum-protect/8.1.9?topic=parameters-file-specification-syntax
	const maxPathLen = 4096 * 2
	if pathLength > maxPathLen {
		return nil, fmt.Errorf("flcv2: illegal file path length %d (max: %d)", pathLength, maxPathLen)
	}
	plen := int(pathLength)

	data := flc.buffer(plen + flcv2EntryStatLen)
	if _, err := io.ReadFull(flc.reader, data); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("flcv2: failed to read full data: %w", err)
	}
	if data[len(data)-1] != '\n' {
		return nil, errors.New("flcv2: invalid entry separator")
	}

	*entry = FLCEntry{
		LogicalSize:  int64(binary.BigEndian.Uint64(data[plen:])),
		PhysicalSize: int64(binary.BigEndian.Uint64(data[plen+flcv2StatFieldSize:])),
		DataPoints:   int64(binary.BigEndian.Uint64(data[plen+flcv2StatFieldSize*2:])),
		FirstSeenAt:  int64(binary.BigEndian.Uint64(data[plen+flcv2StatFieldSize*3:])),
	}

	return data[:plen], nil
}

// writeFLCv2Members writes an FLC v2 cache as one gzip member per chunk of
// entries, compressed concurrently. gzip readers, including NewFileListCache in
// older releases, read concatenated members as one stream, so the file is
// equivalent to one written by fileListCacheV2.
func writeFLCv2Members(w io.Writer, n, workers int, entry func(i int, e *FLCEntry)) error {
	workers = max(1, min(workers, n/65536+1))
	members := make([][]byte, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for k := range workers {
		wg.Go(func() {
			var out bytes.Buffer
			z := gzip.NewWriter(&out)
			flc := &fileListCacheV2{fileListCacheCommon: &fileListCacheCommon{version: FLCVersion2, mode: 'w', writer: z}}
			if k == 0 {
				if _, errs[k] = z.Write([]byte(version2MagicString)); errs[k] != nil {
					return
				}
			}
			var e FLCEntry
			for i := k * n / workers; i < (k+1)*n/workers; i++ {
				entry(i, &e)
				if errs[k] = flc.Write(&e); errs[k] != nil {
					return
				}
			}
			// Flush before Close like fileListCacheCommon.Close: the sync
			// block keeps a reader's magic read from coinciding with EOF
			// when a member holds no entries.
			if errs[k] = z.Flush(); errs[k] == nil {
				errs[k] = z.Close()
			}
			members[k] = out.Bytes()
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	for _, m := range members {
		if _, err := w.Write(m); err != nil {
			return err
		}
	}
	return nil
}
