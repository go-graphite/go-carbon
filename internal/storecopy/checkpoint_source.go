package storecopy

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CheckpointSourceManifest accepts a fully synced baseline pull without sealing
// input or preparing a replacement store. Only export may use this checkpoint;
// it still carries NEEDS-CATCHUP and must never become a serving store.
func CheckpointSourceManifest(directory string) (*Manifest, error) {
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(filepath.Join(directory, "store"))
	if err != nil || canonical != filepath.Join(absolute, "store") {
		return nil, errors.New("checkpoint must have canonical directory and store paths")
	}
	data, err := os.ReadFile(filepath.Join(directory, "copy-state.json"))
	if err != nil {
		return nil, err
	}
	var state Progress
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	m := state.Manifest
	if m == nil || len(m.ID) != 32 || (m.Version != 1 && m.Version != 2) {
		return nil, errors.New("invalid checkpoint copy state")
	}
	if _, err := hex.DecodeString(m.ID); err != nil {
		return nil, errors.New("invalid checkpoint ID")
	}
	storeFiles := 0
	seen := make(map[string]bool)
	for _, file := range m.Files {
		if !strings.HasPrefix(file.Name, "store/") {
			continue
		}
		if filepath.Base(file.Name) != strings.TrimPrefix(file.Name, "store/") {
			return nil, errors.New("invalid checkpoint path")
		}
		if seen[file.Name] {
			return nil, errors.New("duplicate checkpoint file")
		}
		seen[file.Name] = true
		info, err := os.Lstat(filepath.Join(directory, file.Name))
		if err != nil {
			return nil, err
		}
		current, err := stamp(info)
		previous, ok := state.Files[file.Name]
		if err != nil || !ok || current != previous || current.Size != file.Size {
			return nil, fmt.Errorf("incomplete or modified checkpoint file: %s", file.Name)
		}
		storeFiles++
	}
	if storeFiles == 0 || !seen["store/CHUNKSTORE"] || !seen["store/CURRENT"] {
		return nil, errors.New("checkpoint is missing verified store files")
	}
	return m, nil
}
