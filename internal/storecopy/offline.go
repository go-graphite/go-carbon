package storecopy

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
)

// OpenOffline serves an operator-owned, immutable checkpoint for local transfer
// measurements. It cannot create, seal or delete snapshots, or retain live input.
func OpenOffline(directory string) (http.Handler, *Manifest, error) {
	if handoff.Locked(filepath.Join(directory, "LOCK")) {
		return nil, nil, errors.New("offline checkpoint is open by a writer")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, nil, err
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, nil, err
	}
	manifest := &Manifest{Version: 1, ID: hex.EncodeToString(id), Created: time.Now().UTC(), Expires: time.Now().UTC().Add(time.Hour)}
	m := &Manager{manifest: manifest, paths: make(map[string]fileSource), slots: make(chan struct{}, 64)}
	for _, entry := range entries {
		if entry.Name() == "LOCK" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, nil, errors.New("checkpoint contains nonregular file")
		}
		name := "store/" + entry.Name()
		manifest.Files = append(manifest.Files, File{name, info.Size()})
		m.paths[name] = fileSource{filepath.Join(directory, entry.Name()), info.Size()}
	}
	if err := validateManifest(manifest); err != nil {
		return nil, nil, err
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "offline checkpoint is read only", http.StatusMethodNotAllowed)
			return
		}
		m.ServeHTTP(w, r)
	})
	return handler, manifest, nil
}
