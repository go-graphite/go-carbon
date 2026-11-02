package chunkstore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/cockroachdb/pebble/vfs"
)

const incrementalMarker = "INCREMENTAL-FILE-MIGRATION"
const incrementalMarkerVersion = "go-carbon-incremental-v2\n"
const incrementalMarkerLegacy = "go-carbon-incremental-v1\n"
const incrementalSourceToken = ".go-carbon-migration-source"

type incrementalIdentity struct{ legacy, stable string }

func readIncrementalIdentity(fs vfs.FS, source string) (incrementalIdentity, error) {
	info, err := fs.Stat(source)
	if err != nil {
		return incrementalIdentity{}, err
	}
	if !info.IsDir() {
		return incrementalIdentity{}, errors.New("incremental source is not a directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return incrementalIdentity{"virtual", "virtual"}, nil
	}
	identity := incrementalIdentity{legacy: fmt.Sprintf("%d:%d", uint64(stat.Dev), uint64(stat.Ino))}
	identity.stable, err = incrementalBirthIdentity(source, stat)
	return identity, err
}

func incrementalMarkerContents(source, identity string) string {
	return incrementalMarkerVersion + source + "\n" + identity + "\n"
}

func readIncrementalToken(fs vfs.FS, source string) (string, error) {
	value, err := readStoreMarker(fs, source, incrementalSourceToken)
	if err != nil {
		return "", err
	}
	if len(value) != 32 {
		return "", fmt.Errorf("missing or invalid source identity file %s", fs.PathJoin(source, incrementalSourceToken))
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", fmt.Errorf("invalid source identity file %s", fs.PathJoin(source, incrementalSourceToken))
		}
	}
	return "token:" + value, nil
}

func createIncrementalToken(fs vfs.FS, source string) (string, error) {
	name := fs.PathJoin(source, incrementalSourceToken)
	if _, err := fs.Stat(name); !errors.Is(err, os.ErrNotExist) {
		// A previous attempt may have linked the token but failed its directory
		// sync. Make that link durable before any destination records it.
		f, err := fs.Open(name)
		if err != nil {
			return "", err
		}
		if err := errors.Join(f.Sync(), f.Close(), syncStoreDirectory(fs, source)); err != nil {
			return "", err
		}
		return readIncrementalToken(fs, source)
	}
	// A random temporary name and an exclusive link publish only a complete,
	// synced token, even when two destinations adopt the same source at once.
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	value := fmt.Sprintf("%x", token)
	temporary := name + "." + value + ".tmp"
	f, err := fs.Create(temporary)
	if err != nil {
		return "", fmt.Errorf("source has no birth time; creating %s requires a writable source: %w", name, err)
	}
	defer fs.Remove(temporary)
	_, writeErr := f.Write([]byte(value + "\n"))
	if err := errors.Join(writeErr, f.Sync(), f.Close()); err != nil {
		return "", err
	}
	if err := fs.Link(temporary, name); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	if err := fs.Remove(temporary); err != nil {
		return "", err
	}
	if err := syncStoreDirectory(fs, source); err != nil {
		return "", err
	}
	return readIncrementalToken(fs, source)
}

func checkIncrementalMarker(fs vfs.FS, dir, source string, identify func(vfs.FS, string) (incrementalIdentity, error)) (bool, error) {
	marker := fs.PathJoin(dir, incrementalMarker)
	f, err := fs.Open(marker)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	data, readErr := io.ReadAll(io.LimitReader(f, 16384))
	if err := errors.Join(readErr, f.Close()); err != nil {
		return false, err
	}
	parts := strings.Split(string(data), "\n")
	legacy := strings.HasPrefix(string(data), incrementalMarkerLegacy)
	recorded, recordedSource, current := string(data), "unknown", "disabled"
	var identity incrementalIdentity
	if source != "" {
		identity, err = identify(fs, source)
		current = identity.stable
		if legacy {
			current = identity.legacy
		} else if len(parts) == 4 && strings.HasPrefix(parts[2], "token:") && err == nil {
			current, err = readIncrementalToken(fs, source)
		}
	}
	if len(parts) == 4 {
		recorded = parts[2]
		recordedSource = parts[1]
		version := incrementalMarkerVersion
		if legacy {
			version = incrementalMarkerLegacy
		}
		if source != "" && err == nil && current != "" && string(data) == version+source+"\n"+current+"\n" {
			return legacy, nil
		}
	}
	if source != "" && current == "" && err == nil {
		err = errors.New("source directory birth time is unavailable")
	}
	if current == "" {
		current = "unavailable"
	}
	return false, errors.Join(fmt.Errorf("%w: incremental migration marker %s: recorded identity %q at %q, current identity %q at %q; verify the original filesystem is mounted at the recorded path and the directory has not been replaced; restore the original source or recover from a verified backup", ErrFormat, marker, recorded, recordedSource, current, source), err)
}

func (s *Store) configureIncrementalSource(fs vfs.FS, dir, source string, identify func(vfs.FS, string) (incrementalIdentity, error)) error {
	if source == "" {
		return nil
	}
	if !s.exactBlocks || !filepath.IsAbs(source) || filepath.Clean(source) != source || strings.ContainsAny(source, "\r\n") {
		return errors.New("incremental migration requires exact encoding and a canonical absolute source directory")
	}
	for _, pair := range [][2]string{{source, dir}, {dir, source}} {
		relative, err := filepath.Rel(pair[0], pair[1])
		if err != nil || filepath.IsLocal(relative) {
			return errors.New("incremental source and destination must be separate directory trees")
		}
	}
	// Recheck under Pebble's lock before creating or upgrading the binding.
	identity, err := identify(fs, source)
	if err != nil {
		return err
	}
	legacy, err := checkIncrementalMarker(fs, dir, source, func(vfs.FS, string) (incrementalIdentity, error) { return identity, nil })
	if err != nil {
		return err
	}
	marker := fs.PathJoin(dir, incrementalMarker)
	_, statErr := fs.Stat(marker)
	if errors.Is(statErr, os.ErrNotExist) {
		watermark, err := s.CatalogHighWatermark(context.Background())
		if err != nil {
			return err
		}
		if watermark != 0 {
			return errors.New("incremental migration requires an empty destination, not an unverified baseline")
		}
		for _, name := range []string{"NEEDS-CATCHUP", "MIRROR-RECONCILE-REQUIRED"} {
			if _, err := fs.Stat(fs.PathJoin(dir, name)); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("incremental migration cannot adopt %s", name)
			}
		}
	} else if statErr != nil {
		return statErr
	}
	if legacy || errors.Is(statErr, os.ErrNotExist) {
		if identity.stable == "" {
			identity.stable, err = createIncrementalToken(fs, source)
			if err != nil {
				return err
			}
		}
		if err := writeStoreMarker(fs, dir, incrementalMarker, strings.TrimSuffix(incrementalMarkerContents(source, identity.stable), "\n")); err != nil {
			return err
		}
	}
	f, err := fs.Open(marker)
	if err != nil {
		return err
	}
	if err := errors.Join(f.Sync(), f.Close(), syncStoreDirectory(fs, dir)); err != nil {
		return err
	}
	// These reserved directories contain only disposable captures and sort runs.
	for _, name := range []string{".incremental-capture", ".incremental-index"} {
		if err := fs.RemoveAll(fs.PathJoin(dir, name)); err != nil {
			return err
		}
	}
	s.incrementalSource = source
	return nil
}
