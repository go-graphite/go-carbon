package v2core

// Immutable-part transport boundary. A format owner supplies a coherent,
// retained inventory; this code never walks a live database or decodes rows.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// SnapshotPart identifies an immutable file by its complete encoded bytes.
// Path is local input only and is removed from the published snapshot.
type SnapshotPart struct {
	ID         string
	Bytes      int64
	SHA256     string
	Path       string          `json:",omitempty"`
	Dictionary *MoveDictionary `json:",omitempty"`
}

// PartSnapshot is a file inventory, not an installed or READY certificate.
// Metadata files (catalogue, policies, tombstones, frontiers) must be included
// by the format owner in the same coherent inventory as the data files.
type PartSnapshot struct {
	Version int
	Parts   []SnapshotPart
}

// PartSnapshotReader is implemented by the format's retained snapshot reader.
// Its file set and bytes must remain immutable until LinkPartSnapshot returns.
// The caller must release the reader's lease after this call, including errors.
type PartSnapshotReader interface {
	ReadPartFiles(context.Context) ([]SnapshotPart, error)
}

// Validate rejects ambiguous identities and all receiver-controlled paths.
func (s PartSnapshot) Validate() error {
	if s.Version != 1 || len(s.Parts) > 1<<20 {
		return errors.New("invalid part snapshot version or size")
	}
	seen := make(map[string]bool, len(s.Parts))
	for _, p := range s.Parts {
		b, err := hex.DecodeString(p.ID)
		if err != nil || len(b) != sha256.Size || p.ID != p.SHA256 || p.ID != hex.EncodeToString(b) || p.Bytes < 0 || p.Bytes > 1<<40 || p.Path != "" || seen[p.ID] {
			return errors.New("invalid or duplicate immutable part")
		}
		seen[p.ID] = true
	}
	return nil
}

// LinkPartSnapshot pins whole files with hard links, hashes encoded bytes once,
// and publishes only after file and directory sync. There is no copy fallback:
// crossing filesystems fails rather than silently rewriting the history.
func LinkPartSnapshot(ctx context.Context, dir string, reader PartSnapshotReader) (out PartSnapshot, err error) {
	files, err := reader.ReadPartFiles(ctx)
	if err != nil {
		return out, err
	}
	if len(files) > 1<<20 {
		return out, errors.New("too many parts")
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return out, err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dir)
		}
	}()
	out.Version = 1
	seen := make(map[string]bool)
	for i, p := range files {
		if err = ctx.Err(); err != nil {
			return out, err
		}
		stage := filepath.Join(dir, fmt.Sprintf("pin-%08d", i))
		st, e := os.Lstat(p.Path)
		if e != nil {
			return out, e
		}
		if !st.Mode().IsRegular() {
			return out, errors.New("part is not a regular file")
		}
		if err = os.Link(p.Path, stage); err != nil {
			return out, err
		}
		f, e := os.Open(stage)
		if e != nil {
			return out, e
		}
		h := sha256.New()
		n, e := io.Copy(h, f)
		if e == nil {
			e = f.Sync()
		}
		e = errors.Join(e, f.Close())
		if e != nil {
			return out, e
		}
		id := hex.EncodeToString(h.Sum(nil))
		if (p.ID != "" && p.ID != id) || (p.SHA256 != "" && p.SHA256 != id) || (p.Bytes != 0 && p.Bytes != n) {
			return out, errors.New("source part checksum or size mismatch")
		}
		if seen[id] {
			_ = os.Remove(stage)
			continue
		}
		seen[id] = true
		if err = os.Rename(stage, filepath.Join(dir, id+".part")); err != nil {
			return out, err
		}
		out.Parts = append(out.Parts, SnapshotPart{ID: id, Bytes: n, SHA256: id, Dictionary: p.Dictionary})
	}
	if err = out.Validate(); err != nil {
		return out, err
	}
	b, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "snapshot.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return out, err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = SyncPartDirectory(dir)
	}
	if err == nil {
		err = SyncPartDirectory(filepath.Dir(dir))
	}
	return out, err
}

// SyncPartDirectory makes renamed files durable before publishing completion.
func SyncPartDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
