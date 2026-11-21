package v2core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/go-graphite/go-carbon/internal/v2part"
)

// PortableSnapshotReader adapts an already-retained, coherent portable manifest
// to whole-file copy. Directory holds immutable <physical-sha256>.sst files.
// The caller owns its lease through LinkPartSnapshot; this adapter does not
// discover live files or manufacture catalog/policy/frontier coverage.
// The manifest is itself copied, binding the physical files to original scopes.
type PortableSnapshotReader struct {
	ManifestPath string
	Directory    string
}

func (r PortableSnapshotReader) ReadPartFiles(ctx context.Context) ([]SnapshotPart, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(r.ManifestPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, v2part.MaxManifestBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > v2part.MaxManifestBytes {
		return nil, errors.New("portable manifest too large")
	}
	m, err := v2part.ParseManifest(b)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	id := hex.EncodeToString(sum[:])
	out := []SnapshotPart{{Path: r.ManifestPath, ID: id, SHA256: id, Bytes: int64(len(b))}}
	for _, d := range m.Parts {
		if d.Bytes > 1<<40 {
			return nil, errors.New("portable part exceeds copy limit")
		}
		id := hex.EncodeToString(d.SHA256[:])
		out = append(out, SnapshotPart{Path: filepath.Join(r.Directory, id+".sst"), ID: id, SHA256: id, Bytes: int64(d.Bytes)})
	}
	return out, nil
}
