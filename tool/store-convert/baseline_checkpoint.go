package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// These receipts prove individual baseline captures, never completion of the
// source walk or permission to serve. An older partial baseline has no receipts:
// its existing blocks are retained and compared once before a receipt is made.
type baselineCheckpoint struct {
	db      *pebble.DB
	skipped atomic.Uint64
}

func openBaselineCheckpoint(dir string, id identity) (*baselineCheckpoint, error) {
	path := filepath.Join(dir, ".baseline-verification")
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	want, err := json.Marshal(struct {
		Version  int
		Identity identity
	}{1, id})
	if err != nil {
		return nil, err
	}
	got, err := os.ReadFile(filepath.Join(path, "IDENTITY.json"))
	switch {
	case errors.Is(err, os.ErrNotExist):
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			return nil, readErr
		}
		if len(entries) != 0 {
			return nil, errors.New("nonempty baseline checkpoint has no identity")
		}
		if err := writeCheckpointFile(path, "IDENTITY.json", want, os.O_EXCL); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	case !bytes.Equal(got, want):
		return nil, errors.New("baseline checkpoint identity differs")
	}
	cache := pebble.NewCache(16 << 20)
	defer cache.Unref()
	db, err := pebble.Open(path, &pebble.Options{Cache: cache, MemTableSize: 8 << 20})
	if err != nil {
		return nil, err
	}
	return &baselineCheckpoint{db: db}, nil
}

// Ignore access time. Main and OOO identity/change time invalidate replacement,
// in-place modification, sidecar addition/removal and inode reuse after a crash.
func baselineCaptureStamp(path string) ([32]byte, error) {
	h := sha256.New()
	for i, name := range []string{path, whisper.OutOfOrderSidecarPath(path)} {
		info, err := os.Lstat(name)
		if i == 1 && errors.Is(err, os.ErrNotExist) {
			_, _ = h.Write([]byte{0})
			continue
		}
		if err != nil {
			return [32]byte{}, err
		}
		if !info.Mode().IsRegular() {
			return [32]byte{}, fmt.Errorf("baseline capture is not a regular file: %s", name)
		}
		device, inode, changed, err := checkpointFileIdentity(info)
		if err != nil {
			return [32]byte{}, err
		}
		var raw [41]byte
		raw[0] = 1
		for j, n := range []uint64{device, inode, uint64(info.Size()), uint64(info.ModTime().UnixNano()), uint64(changed)} {
			binary.BigEndian.PutUint64(raw[1+j*8:], n)
		}
		_, _ = h.Write(raw[:])
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, nil
}

func (p *baselineCheckpoint) match(name string, m chunkstore.Metadata, stamp [32]byte) (int, bool, error) {
	data, closer, err := p.db.Get([]byte(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(data) != 65 || data[0] != 1 || !bytes.Equal(data[1:25], proofBytes(m)) || !bytes.Equal(data[33:], stamp[:]) {
		return 0, false, nil
	}
	count := binary.BigEndian.Uint64(data[25:33])
	if uint64(int(count)) != count || int(count) < 0 {
		return 0, false, errors.New("invalid baseline checkpoint point count")
	}
	p.skipped.Add(1)
	return int(count), true, nil
}

func (p *baselineCheckpoint) verified(name string, m chunkstore.Metadata, stamp [32]byte, count int) error {
	var data [65]byte
	data[0] = 1
	copy(data[1:25], proofBytes(m))
	binary.BigEndian.PutUint64(data[25:33], uint64(count))
	copy(data[33:], stamp[:])
	// Destination catalog and blocks are one atomic batch. A receipt surviving
	// a lost destination batch cannot match its metadata. A recapture changes
	// the file stamp, so stale receipts also fail after interrupted recapture.
	// Group-sync receipts instead of adding one fsync to every converted metric.
	return p.db.Set([]byte(name), data[:], pebble.NoSync)
}

func (p *baselineCheckpoint) sync() error { return p.db.LogData(nil, pebble.Sync) }

func (p *baselineCheckpoint) close() error {
	return errors.Join(p.sync(), p.db.Close())
}
