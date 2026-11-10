package carbon

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-graphite/go-carbon/internal/recovery"
)

const startupGenerationName = ".startup-input-generation.json"

// This is an intake-routing hint, never a read accelerator or a point source.
// Publish it while the incumbent still receives and persists, so optional
// checkpoint failure cannot remove the successor's early-capture route.
type startupGeneration struct {
	Version int
	Format  recovery.Format
	Suffix  string
}

func (g startupGeneration) valid() bool {
	if g.Version != 1 || g.Format != recovery.FormatLegacy && g.Format != recovery.FormatCompact && g.Format != recovery.FormatBlocks {
		return false
	}
	parts := strings.Split(strings.TrimSuffix(g.Suffix, ".bin"), ".")
	if !strings.HasSuffix(g.Suffix, ".bin") || len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil || n == 0 || strconv.FormatUint(n, 10) != part {
			return false
		}
	}
	return true
}

func publishStartupGeneration(dir string, format recovery.Format, suffix string) error {
	g := startupGeneration{Version: 1, Format: format, Suffix: suffix}
	if !g.valid() {
		return errors.New("invalid startup input generation")
	}
	file, err := os.CreateTemp(dir, ".startup-input-generation-*")
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	if err := json.NewEncoder(file).Encode(g); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(dir, startupGenerationName)); err != nil {
		return err
	}
	return syncDir(dir)
}

// startupGenerationManifest supplies only the names early intake needs. The
// ordinary restore still validates and loads authoritative dump/WAL data; this
// hint does not make an unavailable checkpoint valid. A stale hint cannot
// bypass tryStartupInput's single-generation check.
func startupGenerationManifest(dir string) (recovery.Manifest, bool, error) {
	var manifest recovery.Manifest
	f, err := os.Open(filepath.Join(dir, startupGenerationName))
	if errors.Is(err, os.ErrNotExist) {
		return manifest, false, nil
	}
	if err != nil {
		return manifest, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return manifest, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return manifest, false, errors.New("invalid startup input generation file")
	}
	var g startupGeneration
	decoder := json.NewDecoder(io.LimitReader(f, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&g); err != nil {
		return manifest, false, fmt.Errorf("read startup input generation: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) || !g.valid() {
		return manifest, false, errors.New("invalid startup input generation")
	}
	manifest.Cache.Name = recovery.CacheName(g.Format, g.Suffix)
	manifest.WAL.Name = recovery.InputName(recovery.FormatLegacy, g.Suffix)
	compact := recovery.InputName(recovery.FormatCompact, g.Suffix)
	if _, err := os.Stat(filepath.Join(dir, compact)); err == nil {
		if _, err := os.Stat(filepath.Join(dir, manifest.WAL.Name)); !errors.Is(err, os.ErrNotExist) {
			return manifest, false, errors.New("ambiguous startup input generation")
		}
		manifest.WAL.Name = compact
	} else if !errors.Is(err, os.ErrNotExist) {
		return manifest, false, err
	}
	return manifest, true, nil
}
