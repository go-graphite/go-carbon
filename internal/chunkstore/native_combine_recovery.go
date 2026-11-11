package chunkstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/cockroachdb/pebble"
)

func (s *Store) discardNativeCombine(identity string, intent nativeCombineIntent) error {
	if intent.Ready {
		return errors.New("cannot discard a prepared native combine")
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := nativeCombineCleanup(b, intent, nil); err != nil {
		return err
	}
	if err := b.Delete(nativeCombineIntentKey(identity), nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	return s.removeNativeCombineDirectories(intent)
}

func (s *Store) removeNativeCombineDirectories(intent nativeCombineIntent) error {
	for _, directory := range intent.Directories {
		name := s.fs.PathBase(directory)
		if !strings.HasPrefix(name, ".native-"+intent.Receipt.Identity+"-") || directory != s.fs.PathJoin(s.dir, name) {
			return ErrFormat
		}
		if err := s.fs.RemoveAll(directory); err != nil {
			return err
		}
	}
	return nil
}

// Unprepared ranges have no visible names and can be discarded. Prepared
// ranges retain their original hidden donor bytes and atomic name receipts;
// NativeImport resumes them without depending on the lifetime of a donor pin.
func (s *Store) recoverNativeCombines() error {
	prefix := []byte("COMBINE/intent/")
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return err
	}
	for it.First(); it.Valid(); it.Next() {
		identity := strings.TrimPrefix(string(it.Key()), string(prefix))
		if err := s.nativeCombineNotAbandoned(identity); errors.Is(err, ErrConflict) {
			continue // Explicit cleanup owns this intent, including after a crash.
		} else if err != nil {
			return errors.Join(err, it.Close())
		}
		if len(it.Value()) > nativeRangeMetadataBytes {
			// Preserve legacy state and any published pieces. Starting the
			// store must not decode an oversized intent or silently abandon it.
			s.pebbleOptions.Logger.Infof("native combine %s retained: metadata %d bytes exceeds %d-byte budget", it.Key(), len(it.Value()), nativeRangeMetadataBytes)
			s.nativeCounters.errors.Add(1)
			continue
		}
		var intent nativeCombineIntent
		if err := json.Unmarshal(it.Value(), &intent); err != nil {
			return errors.Join(err, it.Close())
		}
		identity, names, err := intent.Request.Identity()
		if err != nil || !bytes.Equal(it.Key(), nativeCombineIntentKey(identity)) || identity != intent.Receipt.Identity || names != intent.Receipt.NamesSHA256 || intent.Receipt.Count != len(intent.Request.Names) || len(intent.Raw) != len(intent.Positions) || len(intent.Raw) != len(intent.Origins) {
			return errors.Join(ErrFormat, it.Close())
		}
		if err := validateNativeCombineIntent(identity, intent); err != nil {
			return errors.Join(err, it.Close())
		}
		// Process one intent at a time rather than retaining every range's
		// decoded metadata until the scan ends. The iterator pins its view.
		if !intent.Ready {
			if err := s.discardNativeCombine(intent.Receipt.Identity, intent); err != nil {
				return errors.Join(err, it.Close())
			}
		} else {
			if _, err := s.readNativeCombinePieces(intent); err != nil {
				return errors.Join(err, it.Close())
			}
			if err := s.removeNativeCombineDirectories(intent); err != nil {
				return errors.Join(err, it.Close())
			}
		}
	}
	return errors.Join(it.Error(), it.Close())
}
