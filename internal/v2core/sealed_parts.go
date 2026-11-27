package v2core

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// Descriptor bindings live in the same atomic manifest edit as data/frontier.
// Files created before a failed ingest are never visible through this namespace.
var sealedPartPrefix = []byte{255, 'V', '2', 'P', 0}

func isRollupFile(file []coreKV) bool {
	return len(file) > 0 && (bytes.HasPrefix(file[0].Key, partKeyMagic) || bytes.HasPrefix(file[0].Key, idKeyMagic))
}

func openPartSource(root, journal string, source v2part.Provenance) (v2part.Provenance, error) {
	path := filepath.Join(root, "PARTSOURCE")
	source.MinOrder, source.MaxOrder = 0, 0
	if b, err := os.ReadFile(path); err == nil {
		var old v2part.Provenance
		if err = json.Unmarshal(b, &old); err != nil {
			return source, err
		}
		if old.Incarnation == (v2part.PartID{}) || old.Stream == "" {
			return source, errors.New("invalid persisted part source")
		}
		if source.Incarnation != (v2part.PartID{}) && source != old {
			return source, errors.New("part source changed")
		}
		return old, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return source, err
	}
	if source.Incarnation == (v2part.PartID{}) && source.Stream == "" {
		// JournalIdentity can be a shared configuration label. It must never
		// make two independent roots claim the same original ordering stream.
		if _, err := rand.Read(source.Incarnation[:]); err != nil {
			return source, err
		}
		source.Stream = journal
		if source.Stream == "" {
			source.Stream = "offline"
		}
	}
	if source.Incarnation == (v2part.PartID{}) || source.Stream == "" {
		return source, errors.New("incomplete part source")
	}
	b, err := json.Marshal(source)
	if err != nil {
		return source, err
	}
	f, err := os.CreateTemp(root, ".PARTSOURCE-")
	if err != nil {
		return source, err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		// Publish only the fully synced identity. A partial initial write must
		// not leave an unreadable PARTSOURCE that prevents journal recovery.
		err = os.Link(f.Name(), path)
	}
	if err == nil {
		err = SyncPartDirectory(root)
	}
	return source, err
}

// retainSealedPart links a synced part into dir/parts and writes its sidecar.
// The caller syncs dir/parts once (syncRetainedParts) before it binds any
// returned descriptor; the owner open made the parts/ entry itself durable.
func retainSealedPart(dir, path string, d v2part.Descriptor) (coreKV, []string, error) {
	var saved []string
	b, err := d.MarshalBinary()
	if err != nil {
		return coreKV{}, saved, err
	}
	parts := filepath.Join(dir, "parts")
	if err = os.MkdirAll(parts, 0750); err != nil {
		return coreKV{}, saved, err
	}
	id := hex.EncodeToString(d.SHA256[:])
	target := filepath.Join(parts, id+".sst")
	if err = os.Link(path, target); errors.Is(err, os.ErrExist) {
		// Replay can encounter an unpublished original still in its GC grace.
		// The filename binds complete bytes; verify before reusing the link.
		old, e := os.Open(target)
		if e != nil {
			return coreKV{}, saved, e
		}
		e = errors.Join(v2part.VerifyStream(context.Background(), io.Discard, old, d), old.Close())
		if e != nil {
			return coreKV{}, saved, e
		}
	} else if err != nil {
		return coreKV{}, saved, err
	} else {
		saved = append(saved, target)
	}
	// The sidecar is a copy of the binding value, not a durability step: the
	// owner open rewrites any bound part's sidecar that a crash left short.
	sidecar := target + ".descriptor"
	// A matching one can outlive an interrupted data-link deletion and is
	// reused; an unbound leftover (a crash before publication) is rewritten.
	if old, e := os.ReadFile(sidecar); e != nil || !bytes.Equal(old, b) {
		if errors.Is(e, os.ErrNotExist) {
			saved = append(saved, sidecar)
		}
		if err = writeSidecar(sidecar, b, false); err != nil {
			return coreKV{}, saved, err
		}
	}
	key := append(bytes.Clone(sealedPartPrefix), d.SHA256[:]...)
	return coreKV{Key: key, Value: b}, saved, nil
}

// repairSidecar rewrites path to want, synced, unless it already matches.
func repairSidecar(path string, want []byte) (bool, error) {
	if b, err := os.ReadFile(path); err == nil && bytes.Equal(b, want) {
		return false, nil
	}
	return true, writeSidecar(path, want, true)
}

func writeSidecar(path string, b []byte, sync bool) error {
	f, err := ownerFS.Create(path)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil && sync {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// SealedSnapshotReader enumerates original part bytes bound to one immutable
// owner snapshot. The caller retains Snapshot and Directory until hard-linking
// completes. Uncommitted files and Pebble compaction outputs are never exported.
// This is archive coverage only; callers separately bind catalog and log roots.
type SealedSnapshotReader struct {
	Snapshot   *pebble.Snapshot
	Directory  string       // owner directory, containing parts/
	Dictionary *v2dict.Dict // required when the snapshot contains ID-keyed parts
}

func (r SealedSnapshotReader) ReadPartFiles(ctx context.Context) ([]SnapshotPart, error) {
	if r.Snapshot == nil {
		return nil, errors.New("sealed snapshot required")
	}
	it, err := r.Snapshot.NewIter(&pebble.IterOptions{LowerBound: sealedPartPrefix, UpperBound: v2part.PrefixEnd(sealedPartPrefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []SnapshotPart
	for it.First(); it.Valid(); it.Next() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		d, e := v2part.ParseDescriptor(it.Value())
		if e != nil {
			return nil, e
		}
		if !bytes.Equal(it.Key(), append(bytes.Clone(sealedPartPrefix), d.SHA256[:]...)) {
			return nil, errors.New("sealed descriptor binding mismatch")
		}
		id := hex.EncodeToString(d.SHA256[:])
		path := filepath.Join(r.Directory, "parts", id+".sst")
		names, e := ExportMoveDictionary(d, r.Dictionary)
		if e != nil {
			return nil, e
		}
		out = append(out, SnapshotPart{Path: path, ID: id, SHA256: id, Bytes: int64(d.Bytes), Dictionary: names})
		// Bind each descriptor too, so an opaque receiver can recover the one reader.
		sum := sha256.Sum256(it.Value())
		did := hex.EncodeToString(sum[:])
		out = append(out, SnapshotPart{Path: path + ".descriptor", ID: did, SHA256: did, Bytes: int64(len(it.Value()))})
	}
	return out, it.Error()
}

// syncRetainedParts makes every link retained in dir/parts durable with one
// directory sync, before the caller publishes their descriptors.
func syncRetainedParts(dir string) error {
	return SyncPartDirectory(filepath.Join(dir, "parts"))
}
