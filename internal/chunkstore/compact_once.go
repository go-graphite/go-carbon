package chunkstore

import (
	"fmt"
	"sync/atomic"
)

// compactOnceMarker records the token of the last finished full compaction.
const compactOnceMarker = "COMPACTED"

type compactOnceState struct {
	running atomic.Bool
	err     atomic.Pointer[string]
}

// startCompactOnce compacts the whole keyspace in the background, once per
// token. Compacting every table down to the bottom level drops the tombstones
// nothing below still needs, including range tombstones written by earlier
// binaries that otherwise cost every read opening those tables. A restart
// before it finishes leaves the token unrecorded, so the next start resumes it;
// levels already compacted are cheap to pass again.
func (s *Store) startCompactOnce(token string) error {
	if token == "" {
		return nil
	}
	done, err := readStoreMarker(s.fs, s.dir, compactOnceMarker)
	if err != nil || done == token {
		return err
	}
	s.compactOnce.running.Store(true)
	go func() {
		defer s.compactOnce.running.Store(false)
		// Close cancels the compaction; Compact panics if it starts after.
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("panic: %v", r)
				s.compactOnce.err.Store(&msg)
			}
		}()
		// Every key starts with a printable type byte, so [0x00, 0xff)
		// covers the keyspace.
		err := s.db.Compact([]byte{0x00}, []byte{0xff}, true)
		if err == nil {
			err = writeStoreMarker(s.fs, s.dir, compactOnceMarker, token)
		}
		if err != nil {
			msg := err.Error()
			s.compactOnce.err.Store(&msg)
		}
	}()
	return nil
}
