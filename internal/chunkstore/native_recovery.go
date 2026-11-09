package chunkstore

import (
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
)

// Called under Pebble's process ownership before listeners/background work.
// Only the engine's reserved invisible ranges and private staging dirs retire.
func (s *Store) recoverNative() error {
	lower := []byte("NATIVE/")
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: prefixEnd(lower)})
	if err != nil {
		return err
	}
	any := false
	var intents []string
	for it.First(); it.Valid(); it.Next() {
		any = true
		if identity, ok := strings.CutPrefix(string(it.Key()), "NATIVE/intent/"); ok {
			if len(identity) != 64 {
				return errors.Join(ErrFormat, it.Close())
			}
			if _, err := hex.DecodeString(identity); err != nil {
				return errors.Join(ErrFormat, it.Close())
			}
			intents = append(intents, identity)
		}
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return err
	}
	for _, identity := range intents {
		if err := s.cleanupNativeIntent(identity); err != nil {
			return err
		}
	}
	if !any {
		return nil
	}
	entries, err := s.fs.List(s.dir)
	if err != nil {
		return err
	}
	for _, name := range entries {
		if !strings.HasPrefix(name, ".native-") {
			continue
		}
		if len(name) != len(".native-")+64+1+20 {
			return ErrFormat
		}
		identity := name[len(".native-") : len(".native-")+64]
		if _, err := hex.DecodeString(identity); err != nil {
			return ErrFormat
		}
		first, err := strconv.ParseUint(name[len(name)-20:], 10, 64)
		if err != nil || name != nativeDirectory(identity, first) {
			return ErrFormat
		}
		if err := s.fs.RemoveAll(s.fs.PathJoin(s.dir, name)); err != nil {
			return err
		}
	}
	return nil
}
