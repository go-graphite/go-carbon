package chunkstore

import (
	"errors"
	"io"
	"os"

	"github.com/cockroachdb/pebble"
)

// Checkpoint pins Pebble's immutable tables with hard links and copies its WAL
// and manifest. The caller owns destination cleanup and must retain the input
// log before calling: the store does not include points still in the cache.
// Completed incremental stores retain their source binding; a clone must rebind
// it to a new empty original directory before starting incremental mode.
func (s *Store) Checkpoint(destination string) (err error) {
	s.coldPack.run.Lock()
	defer s.coldPack.run.Unlock()
	if err := s.fileOwnershipError(); err != nil {
		return err
	}
	if s.incrementalSource != "" {
		// A copied binding can refer only to an empty replacement source. Hidden
		// import stages still need their original files and cannot be relocated.
		it, e := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("m/"), UpperBound: []byte("m0")})
		if e != nil {
			return e
		}
		for valid := it.First(); valid; valid = it.Next() {
			value := it.Value()
			if len(value) == 0 || value[0]&0x7f == 1 {
				_ = it.Close()
				return errors.New("checkpoint contains an unowned file import; finish conversion before copying")
			}
		}
		if e := errors.Join(it.Error(), it.Close()); e != nil {
			return e
		}
	}
	if err = s.db.Checkpoint(destination, pebble.WithFlushedWAL()); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = s.fs.RemoveAll(destination)
		}
	}()
	for _, name := range []string{"CHUNKSTORE", incrementalMarker} {
		source, e := s.fs.Open(s.fs.PathJoin(s.dir, name))
		if errors.Is(e, os.ErrNotExist) && name == incrementalMarker {
			continue
		}
		if e != nil {
			return e
		}
		target, e := s.fs.Create(s.fs.PathJoin(destination, name))
		if e != nil {
			_ = source.Close()
			return e
		}
		_, e = io.Copy(target, source)
		if e = errors.Join(e, target.Sync(), target.Close(), source.Close()); e != nil {
			return e
		}
	}
	return syncStoreDirectory(s.fs, destination)
}
