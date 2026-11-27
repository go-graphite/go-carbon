package v2daemon

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

func (a *MoveAdmin) requireMoveVersion() error {
	root := filepath.Dir(a.root)
	b, e := os.ReadFile(filepath.Join(root, "restart.json"))
	if e != nil {
		return e
	}
	var geometry restartGeometry
	if e = json.Unmarshal(b, &geometry); e != nil {
		return e
	}
	if geometry.Version != restartVersion {
		return errors.New("unsupported restart format version")
	}
	return nil
}
func validateMoveRecords(root string) error {
	entries, e := os.ReadDir(filepath.Join(root, "moves"))
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if !entry.IsDir() || !validMoveID(entry.Name()) {
			continue
		}
		b, e := os.ReadFile(filepath.Join(root, "moves", entry.Name(), "record.json"))
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		var version struct{ Version int }
		if json.Unmarshal(b, &version) != nil || version.Version != MoveRecordVersion {
			return errors.New("unsupported move record version")
		}
	}
	return nil
}
