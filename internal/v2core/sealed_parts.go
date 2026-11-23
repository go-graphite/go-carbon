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
	sidecar := target + ".descriptor"
	f, err := os.OpenFile(sidecar, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		old, e := os.ReadFile(sidecar)
		if e != nil {
			return coreKV{}, saved, e
		}
		if !bytes.Equal(old, b) {
			return coreKV{}, saved, errors.New("existing descriptor mismatch")
		}
		// The sidecar can outlive an interrupted data-link deletion. A
		// newly recreated link still needs directory durability before binding.
		if e = SyncPartDirectory(parts); e != nil {
			return coreKV{}, saved, e
		}
		if e = SyncPartDirectory(dir); e != nil {
			return coreKV{}, saved, e
		}
		key := append(bytes.Clone(sealedPartPrefix), d.SHA256[:]...)
		return coreKV{Key: key, Value: b}, saved, nil
	}
	if err != nil {
		return coreKV{}, saved, err
	}
	saved = append(saved, sidecar)
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = SyncPartDirectory(parts)
	}
	if err == nil {
		err = SyncPartDirectory(dir)
	}
	key := append(bytes.Clone(sealedPartPrefix), d.SHA256[:]...)
	return coreKV{Key: key, Value: b}, saved, err
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
