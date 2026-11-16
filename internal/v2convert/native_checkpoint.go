package v2convert

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

// NativeCheckpointPlan is a caller-certified immutable checkpoint inventory and
// a bounded name selection. Files covers every checkpoint file except LOCK.
// Baseline.Snapshot must equal InventoryDigest. The caller also certifies the
// input-log catch-up fence associated with Baseline.Source/Order. File hashes
// cannot establish that external fence, and this plan never authorizes adoption.
type NativeCheckpointPlan struct {
	Version  int
	Baseline NativeBaseline
	Now      int64
	Names    []string
	Files    []File
}

func (p NativeCheckpointPlan) InventoryDigest() (v2part.Digest, error) {
	files := slices.Clone(p.Files)
	slices.SortFunc(files, func(a, b File) int { return strings.Compare(a.Name, b.Name) })
	seen := map[string]bool{}
	for _, f := range files {
		decoded, err := hex.DecodeString(f.SHA256)
		if f.Name == "LOCK" || f.Name == "" || filepath.Base(f.Name) != f.Name || f.Name == "." || f.Name == ".." || seen[f.Name] || f.Bytes < 0 || err != nil || len(decoded) != 32 {
			return v2part.Digest{}, errors.New("invalid native checkpoint file")
		}
		seen[f.Name] = true
	}
	if !seen["CHUNKSTORE"] || !seen["CURRENT"] {
		return v2part.Digest{}, errors.New("native checkpoint missing store authority")
	}
	data, err := json.Marshal(files)
	if err != nil {
		return v2part.Digest{}, err
	}
	return sha256.Sum256(append([]byte("go-carbon-native-checkpoint-v1\x00"), data...)), nil
}

// OpenNativeCheckpoint opens only a normal exclusively locked offline source,
// validates its complete immutable file inventory, and uses the existing
// native reader to resolve point versions, merges and continuation state.
func OpenNativeCheckpoint(ctx context.Context, directory string, plan NativeCheckpointPlan) (_ *chunkstore.TransferSource, err error) {
	if plan.Version != 1 || plan.Now <= 0 {
		return nil, errors.New("invalid native checkpoint plan")
	}
	if err := plan.Baseline.Validate(); err != nil {
		return nil, err
	}
	digest, err := plan.InventoryDigest()
	if err != nil || digest != plan.Baseline.Snapshot {
		return nil, errors.New("native checkpoint inventory authority mismatch")
	}
	marker, err := os.ReadFile(filepath.Join(directory, "CHUNKSTORE"))
	if err != nil {
		return nil, err
	}
	var codec chunkstore.ContinuationCodec
	if string(marker) != "go-carbon-chunks-v4\n" {
		codec = &whisperio.ContinuationCodec{}
	}
	source, err := chunkstore.OpenTransferSource(directory, chunkstore.TransferSourceOptions{Continuation: codec, CacheBytes: 64 << 20, Now: func() time.Time { return time.Unix(plan.Now, 0) }})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, source.Close())
		}
	}()
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]File, len(plan.Files))
	for _, f := range plan.Files {
		seen[f.Name] = f
	}
	for _, entry := range entries {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if entry.Name() == "LOCK" {
			continue
		}
		file, ok := seen[entry.Name()]
		if !ok || entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			return nil, fmt.Errorf("unbound native checkpoint entry %q", entry.Name())
		}
		sum, size, e := FileSum(filepath.Join(directory, entry.Name()))
		if e != nil {
			return nil, e
		}
		if sum != file.SHA256 || size != file.Bytes {
			return nil, fmt.Errorf("native checkpoint checksum mismatch: %s", file.Name)
		}
		delete(seen, entry.Name())
	}
	if len(seen) != 0 {
		return nil, errors.New("native checkpoint file missing")
	}
	return source, nil
}
