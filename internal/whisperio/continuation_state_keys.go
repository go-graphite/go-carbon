package whisperio

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

// StateKey reports the continuation keys the store may keep in its metadata
// cache: what every update and fetch reads before touching a block, and
// which this package rewrites only through its store io. These are file names
// (continuationNameKey), manifests (continuationManifestKey), paged header
// pages (continuationHeaderPrefix plus a 2-byte page) and the file identity
// sequence. Block descriptors and point groups are not state.
func (c *ContinuationCodec) StateKey(suffix []byte) bool {
	switch {
	case bytes.HasPrefix(suffix, continuationNamesPrefix), bytes.Equal(suffix, continuationSequenceKey):
		return true
	case len(suffix) == continuationFileKeyLen && suffix[0] == 'f' && suffix[continuationFileKeyLen-1] == 'h':
		return true
	case len(suffix) == continuationFileKeyLen+2 && suffix[0] == 'f' && suffix[continuationFileKeyLen-1] == 'H':
		return true
	}
	return false
}

// StatePrefix reports the state prefixes this package walks whole: the file
// names (prefetchNames, flush) and one file's header pages (loadHeader).
func (c *ContinuationCodec) StatePrefix(prefix []byte) bool {
	return bytes.Equal(prefix, continuationNamesPrefix) ||
		len(prefix) == continuationFileKeyLen && prefix[0] == 'f' && prefix[continuationFileKeyLen-1] == 'H'
}

var (
	continuationNamesPrefix = []byte("n/")
	continuationSequenceKey = []byte("sequence")
)

// continuationFileKeyLen is the length of a file's manifest key and header
// page prefix: 'f', the 8-byte file id and one kind byte.
const continuationFileKeyLen = 10

// LoadState reads through kv what Update reads before it merges any point
// (its Load stage): the file names, then, opening the main file, its manifest
// and header pages and those of its OOO sidecar, if any. The store records
// these state keys to load them into its metadata cache ahead of updates (see
// store.Store.PrewarmMetadata).
func (c *ContinuationCodec) LoadState(ctx context.Context, kv store.ContinuationIO, m store.Metadata, clock func() time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fs := newContinuationFiles(kv, true).withClock(clock)
	if err := fs.prefetchNames(); err != nil {
		return err
	}
	// Cache the raw state keys; ordinary opens still validate/decode their
	// headers. Decoding them here would be repeated by the first update.
	main := fs.main()
	if _, err := fs.load(filepath.Base(main)); err != nil {
		return err
	}
	_, err := fs.load(filepath.Base(whisper.OutOfOrderSidecarPath(main)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

var (
	_ store.ContinuationStateKeys   = (*ContinuationCodec)(nil)
	_ store.ContinuationStateLoader = (*ContinuationCodec)(nil)
)

// StateSlot keeps only the most recently used block descriptor and point
// group per file/archive. Including the full key in the slot's value makes a
// rotation or a change of time bucket miss instead of reusing the old tail.
func (c *ContinuationCodec) StateSlot(key []byte) []byte {
	if len(key) > 5 && key[0] == 'T' {
		key = key[5:]
	}
	if len(key) != 16 && len(key) != 20 || key[0] != 'f' {
		return nil
	}
	if key[9] != 'b' && key[9] != 'c' {
		return nil
	}
	slot := make([]byte, 13)
	copy(slot[1:], key[:12])
	return slot
}
