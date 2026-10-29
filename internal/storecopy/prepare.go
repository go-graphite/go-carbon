package storecopy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/cockroachdb/pebble/vfs"
)

// Prepare verifies a sealed copy before installing its replay/startup guards.
// It never starts the target or changes its ingestion identity.
func Prepare(directory string, reuse bool) error {
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || resolved != absolute {
		return errors.New("canonical staging directory required")
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return err
	}
	defer root.Close()
	if _, err := root.Stat("copy.lock"); !errors.Is(err, os.ErrNotExist) {
		return errors.New("copy is still running")
	}
	var state Progress
	data, err := root.ReadFile("copy-state.json")
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	complete, err := root.ReadFile("COPY-COMPLETE.json")
	if err != nil {
		return err
	}
	if state.Manifest == nil || !state.Manifest.Sealed || !bytes.Equal(bytes.TrimSpace(complete), bytes.TrimSpace(mustJSON(state.Manifest))) {
		return errors.New("sealed completed copy required")
	}
	if err := validateManifest(state.Manifest); err != nil {
		return err
	}
	storePath := filepath.Join(absolute, "store")
	if resolved, err := filepath.EvalSymlinks(storePath); err != nil || resolved != storePath {
		return errors.New("canonical staged store required")
	}
	lock, err := vfs.Default.Lock(filepath.Join(storePath, "LOCK"))
	if err != nil {
		return fmt.Errorf("target store is running: %w", err)
	}
	defer lock.Close()
	type part struct {
		offset, length int64
		digest         string
	}
	chunks := make(map[string][]part)
	for key, digest := range state.Chunks {
		last := strings.LastIndexByte(key, ':')
		if last < 0 {
			return errors.New("invalid chunk state")
		}
		length, err := strconv.ParseInt(key[last+1:], 10, 64)
		if err != nil {
			return err
		}
		head := key[:last]
		last = strings.LastIndexByte(head, ':')
		if last < 0 {
			return errors.New("invalid chunk state")
		}
		offset, err := strconv.ParseInt(head[last+1:], 10, 64)
		if err != nil {
			return err
		}
		name := head[:last]
		chunks[name] = append(chunks[name], part{offset, length, digest})
	}
	for _, file := range state.Manifest.Files {
		f, err := root.Open(file.Name)
		if err != nil {
			return err
		}
		err = func() error {
			defer f.Close()
			info, err := f.Stat()
			if err != nil {
				return err
			}
			if info.Size() != file.Size || !info.Mode().IsRegular() {
				return errors.New("staging file size/type differs")
			}
			current, err := stamp(info)
			if err != nil {
				return err
			}
			known := reuse && state.Files[file.Name] == current
			parts := chunks[file.Name]
			sort.Slice(parts, func(i, j int) bool { return parts[i].offset < parts[j].offset })
			end := int64(0)
			for _, p := range parts {
				if p.offset > end || p.offset < 0 || p.length <= 0 || p.offset > file.Size-p.length {
					return errors.New("unverified staged range")
				}
				if !known {
					h := sha256.New()
					n, err := io.Copy(h, io.NewSectionReader(f, p.offset, p.length))
					if err != nil || n != p.length || hex.EncodeToString(h.Sum(nil)) != p.digest {
						return errors.New("staging checksum differs")
					}
				}
				end = max(end, p.offset+p.length)
			}
			if end != file.Size {
				return errors.New("incomplete staged file")
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	var logs []File
	for _, file := range state.Manifest.Files {
		if strings.HasPrefix(file.Name, "input-log/") {
			logs = append(logs, File{Name: filepath.Base(file.Name), Size: file.Size})
		}
	}
	if err := writePreparedJSON(root, "input-log/REQUIRE-COMPLETE-REPLAY", struct {
		ID    string `json:"id"`
		Files []File `json:"files"`
	}{state.Manifest.ID, logs}); err != nil {
		return err
	}
	if _, err := root.Stat("store/INCREMENTAL-FILE-MIGRATION"); err == nil {
		if err := root.MkdirAll("empty-originals", 0700); err != nil {
			return err
		}
		entries, err := os.ReadDir(filepath.Join(absolute, "empty-originals"))
		if err != nil || len(entries) != 0 {
			return errors.New("prepared originals directory must be empty")
		}
		info, err := root.Stat("empty-originals")
		if err != nil {
			return err
		}
		st := info.Sys().(*syscall.Stat_t)
		text := fmt.Sprintf("go-carbon-incremental-v1\n%s\n%d:%d\n", filepath.Join(absolute, "empty-originals"), st.Dev, st.Ino)
		if err := writePreparedBytes(root, "store/INCREMENTAL-FILE-MIGRATION", []byte(text)); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writePreparedJSON(root, "PREPARED", map[string]string{"id": state.Manifest.ID}); err != nil {
		return err
	}
	if err := root.Remove("store/NEEDS-CATCHUP"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, path := range []string{"store", "input-log", "."} {
		f, err := root.Open(path)
		if err != nil {
			return err
		}
		if err := errors.Join(f.Sync(), f.Close()); err != nil {
			return err
		}
	}
	return nil
}
func writePreparedJSON(root *os.Root, path string, value any) error {
	return writePreparedBytes(root, path, mustJSON(value))
}
func writePreparedBytes(root *os.Root, path string, data []byte) error {
	f, err := root.OpenFile(path+".next", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := root.Rename(path+".next", path); err != nil {
		return err
	}
	directory, err := root.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
