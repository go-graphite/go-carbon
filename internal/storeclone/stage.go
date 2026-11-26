// Package storeclone copies a whole Pebble store as raw files into an empty
// directory on another host. Every file number and metric ID stays as it is,
// so raw tables are valid there; merging into a live store still needs the
// re-keyed native transfer.
//
// The source store keeps running and is never written. Stage pins its
// immutable tables with hard links in a staging directory on the same
// filesystem, copies the current manifest up to its last complete record, and
// repeats until every table that manifest names is linked. WAL files are not
// copied: the clone is the source's last flushed state.
package storeclone

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
)

// File is one staged file to send.
type File struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	// SHA256 is filled by the sender while streaming.
	SHA256 string `json:"sha256,omitempty"`
}

// skipped never leaves the source: WALs and the lock belong to the running
// process, and the metadata cache is bound to an exact sequence the clone
// does not share, so the daemon would ignore it anyway.
func skipped(name string) bool {
	return strings.HasSuffix(name, ".log") || name == "LOCK" || name == "metadata-cache-v1" ||
		strings.HasPrefix(name, "MANIFEST-") || strings.HasPrefix(name, "marker.manifest.") ||
		name == "CURRENT" || strings.HasSuffix(name, ".sst")
}

// stageHooks let tests change the source between staging steps.
type stageHooks struct {
	afterLink     func()
	beforeClosure func()
	skipClosure   bool // mutation switch for the closure test
}

// maxRounds bounds the closure loop; each round needs a compaction to delete a
// table created after the previous manifest copy, so a few rounds suffice.
const maxRounds = 50

// Stage fills staging (which must not exist) with a closed, openable set of
// files and returns the files to send. The caller removes staging.
func Stage(source, staging string) ([]File, error) {
	return stage(source, staging, stageHooks{})
}

func stage(source, staging string, hooks stageHooks) ([]File, error) {
	if err := os.Mkdir(staging, 0o750); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			if err := linkTable(source, staging, e.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
	}
	if hooks.afterLink != nil {
		hooks.afterLink()
	}
	for round := 0; round < maxRounds; round++ {
		if err := copyMetadata(source, staging); err != nil {
			return nil, err
		}
		if hooks.beforeClosure != nil {
			hooks.beforeClosure()
			hooks.beforeClosure = nil
		}
		// Pebble refuses to open a manifest naming a file it cannot list, so
		// the open sees the source's tables too; a live table missing from
		// both was compacted away after the copy.
		live, err := liveTables(staging, source)
		if err != nil && (errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "unknown to the objstorage provider") ||
			strings.Contains(err.Error(), "no such file")) {
			continue
		} else if err != nil {
			return nil, err
		}
		closed := true
		for _, name := range live {
			if hooks.skipClosure {
				break
			}
			if _, err := os.Lstat(filepath.Join(staging, name)); err == nil {
				continue
			}
			if err := linkTable(source, staging, name); errors.Is(err, os.ErrNotExist) {
				closed = false // compacted away after the copy: take a newer manifest
				break
			} else if err != nil {
				return nil, err
			}
		}
		if closed {
			return sendSet(staging, live)
		}
	}
	return nil, fmt.Errorf("manifest did not close after %d rounds", maxRounds)
}

func linkTable(source, staging, name string) error {
	return os.Link(filepath.Join(source, name), filepath.Join(staging, name))
}

// copyMetadata replaces the staged manifest and small metadata files with the
// source's current ones.
func copyMetadata(source, staging string) error {
	entries, err := os.ReadDir(staging)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sst") {
			if err := os.Remove(filepath.Join(staging, e.Name())); err != nil {
				return err
			}
		}
	}
	// A rotation can delete the manifest CURRENT named; read CURRENT again.
	var manifest string
	for attempt := 0; ; attempt++ {
		if manifest, err = currentManifest(source); err != nil {
			return err
		}
		err = copyManifest(filepath.Join(source, manifest), filepath.Join(staging, manifest))
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) || attempt == 10 {
			return err
		}
	}
	// CURRENT is rewritten rather than copied so it names the manifest just
	// copied even if the source rotated in between.
	if err := os.WriteFile(filepath.Join(staging, "CURRENT"), []byte(manifest+"\n"), 0o644); err != nil {
		return err
	}
	entries, err = os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.Type().IsRegular() || skipped(e.Name()) {
			continue
		}
		if err := copyFile(filepath.Join(source, e.Name()), filepath.Join(staging, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func currentManifest(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	// Newer format versions name the manifest in a marker file instead.
	var marker string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "marker.manifest.") && e.Name() > marker {
			marker = e.Name()
		}
	}
	if marker != "" {
		parts := strings.SplitN(marker, ".", 4)
		if len(parts) == 4 && strings.HasPrefix(parts[3], "MANIFEST-") {
			return parts[3], nil
		}
		return "", fmt.Errorf("unexpected manifest marker %q", marker)
	}
	data, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(string(data))
	if !strings.HasPrefix(name, "MANIFEST-") || strings.ContainsRune(name, '/') {
		return "", fmt.Errorf("unexpected CURRENT %q", name)
	}
	return name, nil
}

// copyManifest copies the manifest up to its last complete record. The
// running process may append a record while we read.
func copyManifest(source, target string) error {
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	r := record.NewReader(bytes.NewReader(data), 0)
	var good int64
	for {
		rec, err := r.Next()
		if err != nil {
			break
		}
		if _, err := io.Copy(io.Discard, rec); err != nil {
			break
		}
		good = r.Offset()
	}
	if good == 0 {
		return fmt.Errorf("manifest %s has no complete record", source)
	}
	if good > int64(len(data)) {
		good = int64(len(data))
	}
	return os.WriteFile(target, data[:good], 0o644)
}

func copyFile(source, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	return errors.Join(err, out.Close())
}

// liveTables opens dir read-only and returns the file names of every table
// its manifest names. Tables of alsoListed count as present for the open.
func liveTables(dir, alsoListed string) ([]string, error) {
	db, err := openReadOnly(dir, alsoListed)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	levels, err := db.SSTables()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var names []string
	for _, level := range levels {
		for _, t := range level {
			name := fmt.Sprintf("%06d.sst", uint64(t.BackingSSTNum))
			if !seen[name] {
				seen[name] = true
				names = append(names, name)
			}
		}
	}
	return names, nil
}

// openReadOnly opens a Pebble directory without the store's merge operator:
// listing tables and validating checksums never merge, so a stand-in carrying
// the recorded name passes Pebble's options check.
func openReadOnly(dir, alsoListed string) (*pebble.DB, error) {
	merger, err := recordedMerger(dir)
	if err != nil {
		return nil, err
	}
	opts := &pebble.Options{ReadOnly: true, Logger: quietLogger{}}
	if alsoListed != "" {
		opts.FS = unionFS{FS: vfs.Default, dir: filepath.Clean(dir), also: alsoListed}
	}
	if merger != "" {
		m := *pebble.DefaultMerger
		m.Name = merger
		opts.Merger = &m
	}
	return pebble.Open(dir, opts)
}

func recordedMerger(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var latest string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "OPTIONS-") && e.Name() > latest {
			latest = e.Name()
		}
	}
	if latest == "" {
		return "", nil
	}
	f, err := os.Open(filepath.Join(dir, latest))
	if err != nil {
		return "", err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if key, value, ok := strings.Cut(strings.TrimSpace(scanner.Text()), "="); ok && key == "merger" && value != "nullptr" {
			return value, nil
		}
	}
	return "", scanner.Err()
}

// unionFS lists and stats also's tables as part of dir. Nothing reads them:
// the open only checks presence and size, and SSTables reads the manifest.
type unionFS struct {
	vfs.FS
	dir, also string
}

func (u unionFS) List(dir string) ([]string, error) {
	names, err := u.FS.List(dir)
	if err != nil || filepath.Clean(dir) != u.dir {
		return names, err
	}
	more, err := u.FS.List(u.also)
	for _, name := range more {
		if strings.HasSuffix(name, ".sst") {
			names = append(names, name)
		}
	}
	return names, err
}

func (u unionFS) Stat(name string) (os.FileInfo, error) {
	info, err := u.FS.Stat(name)
	if err != nil && filepath.Dir(filepath.Clean(name)) == u.dir && strings.HasSuffix(name, ".sst") {
		return u.FS.Stat(filepath.Join(u.also, filepath.Base(name)))
	}
	return info, err
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{})  {}
func (quietLogger) Errorf(string, ...interface{}) {}
func (quietLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// sendSet lists the live tables plus every staged non-table file.
func sendSet(staging string, live []string) ([]File, error) {
	entries, err := os.ReadDir(staging)
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, name := range live {
		want[name] = true
	}
	var files []File
	for _, e := range entries {
		name := e.Name()
		if name == "LOCK" || strings.HasSuffix(name, ".sst") && !want[name] {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		files = append(files, File{Name: name, Size: info.Size()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Size > files[j].Size })
	return files, nil
}
