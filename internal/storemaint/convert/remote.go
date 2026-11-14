package convert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/storemaint/remote"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/golang-jwt/jwt/v5"
)

// convertRemote uses the ordinary archive reader, not physical continuation
// decoding. This deliberately recovers the visible archive view from files
// whose obsolete physical blocks cannot be preserved as a continuation.
// Originals are never removed; only verified native transfers reach the target.
func convertRemote(ctx context.Context, source, target, secretFile, destination string) error {
	source, err := physicalPath(source)
	if err != nil {
		return err
	}
	client, err := remote.New(target, secretFile, true)
	if err != nil {
		return err
	}
	root, err := os.MkdirTemp(destination, "archive-import-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	physical := filepath.Join(root, "source")
	if err := os.Mkdir(physical, 0700); err != nil {
		return err
	}
	db, err := chunkstore.Open(filepath.Join(root, "store"), chunkstore.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: physical, CacheSize: 8 << 20, MemTableSize: 8 << 20})
	if err != nil {
		return err
	}
	defer db.Close()
	return filepath.WalkDir(source, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || filepath.Ext(path) != ".wsp" {
			return nil
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if err := validateSourcePath(source, relative); err != nil {
			return err
		}
		name := metricName(relative)
		// Include the virtual compressed rollups, as the offline converter does.
		snapshot, err := whisperio.ReadSnapshotIn(ctx, name, path, root)
		if err != nil {
			return err
		}
		before, exists, err := remoteSnapshot(ctx, client, name, root)
		if err != nil {
			return err
		}
		canonical := filepath.Join(physical, "canonical.wsp")
		_ = os.Remove(canonical)
		if err := new(whisperio.Adapter).ExportSnapshot(ctx, snapshot, canonical); err != nil {
			return err
		}
		stagedSnapshot := snapshot
		stagedSnapshot.SourceState = canonical
		metadata, err := db.StageFileSnapshot(ctx, stagedSnapshot)
		if err != nil {
			return err
		}
		metadata, err = db.PublishFileOwnership(ctx, stagedSnapshot, metadata)
		if err != nil {
			return err
		}
		if err := db.CompleteFileCleanup(ctx, metadata); err != nil {
			return err
		}
		records, err := db.ExportTransfer(ctx, []string{name})
		if err != nil {
			return err
		}
		var body bytes.Buffer
		if err := chunkstore.WriteTransfer(&body, records); err != nil {
			return err
		}
		response, err := remoteRequest(ctx, client, http.MethodPost, "/v1/transfer/import", &body)
		if err != nil {
			return err
		}
		var proofs []chunkstore.TransferProof
		err = json.NewDecoder(response.Body).Decode(&proofs)
		response.Body.Close()
		if err != nil {
			return err
		}
		var verified map[string]int
		if err := client.DoContext(ctx, http.MethodPost, "/v1/transfer/verify", proofs, &verified); err != nil {
			return err
		}
		after, found, err := remoteSnapshot(ctx, client, name, root)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("imported metric %s is missing", name)
		}
		if err := verifyArchiveUnion(snapshot, before, after, exists); err != nil {
			return fmt.Errorf("verify %s: %w", name, err)
		}
		counts := make([]int, len(snapshot.Archives))
		for i, a := range snapshot.Archives {
			counts[i] = len(a.Points)
		}
		report, _ := json.Marshal(map[string]any{"metric": name, "archives": counts, "target_existed": exists, "verified": true, "original_retained": path})
		fmt.Println(string(report))
		return nil
	})
}
func metricName(relative string) string {
	name := relative[:len(relative)-len(".wsp")]
	out := []byte(name)
	for i := range out {
		if out[i] == filepath.Separator {
			out[i] = '.'
		}
	}
	return string(out)
}
func remoteRequest(ctx context.Context, c *remote.Client, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.Endpoint+path, body)
	if err != nil {
		return nil, err
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(5 * time.Minute).Unix(), "namespaces": []string{"*"}, "ops": []string{"read", "update"}}).SignedString(c.Secret)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Buckyd-Authorization", token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, data)
	}
	return resp, nil
}
func remoteSnapshot(ctx context.Context, c *remote.Client, name, root string) (chunkstore.Snapshot, bool, error) {
	// A HEAD distinguishes absence without treating other errors as an empty target.
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.Endpoint+"/metrics/"+url.PathEscape(name), nil)
	if err != nil {
		return chunkstore.Snapshot{}, false, err
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(5 * time.Minute).Unix(), "namespaces": []string{"*"}, "ops": []string{"read"}}).SignedString(c.Secret)
	if err != nil {
		return chunkstore.Snapshot{}, false, err
	}
	req.Header.Set("X-Buckyd-Authorization", token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return chunkstore.Snapshot{}, false, err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return chunkstore.Snapshot{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return chunkstore.Snapshot{}, false, fmt.Errorf("HEAD %s: HTTP %d", name, resp.StatusCode)
	}
	resp, err = remoteRequest(ctx, c, http.MethodGet, "/metrics/"+url.PathEscape(name), nil)
	if err != nil {
		return chunkstore.Snapshot{}, false, err
	}
	defer resp.Body.Close()
	file, err := os.CreateTemp(root, "readback-*.wsp")
	if err != nil {
		return chunkstore.Snapshot{}, false, err
	}
	defer os.Remove(file.Name())
	_, err = io.Copy(file, io.LimitReader(resp.Body, 64<<20))
	errClose := file.Close()
	if err != nil {
		return chunkstore.Snapshot{}, false, err
	}
	if errClose != nil {
		return chunkstore.Snapshot{}, false, errClose
	}
	snapshot, err := whisperio.ReadSnapshot(ctx, name, file.Name())
	return snapshot, true, err
}
func verifyArchiveUnion(source, before, after chunkstore.Snapshot, existed bool) error {
	if len(after.Archives) != len(source.Archives) {
		return fmt.Errorf("archive count differs")
	}
	for i, a := range source.Archives {
		got := after.Archives[i]
		if got.Retention != a.Retention {
			return fmt.Errorf("archive %d retention differs", i)
		}
		want := make(map[int64]float64, len(a.Points))
		for _, p := range a.Points {
			want[p.Timestamp] = p.Value
		}
		if existed {
			if len(before.Archives) != len(source.Archives) || before.Archives[i].Retention != a.Retention {
				return fmt.Errorf("target policy differs")
			}
			for _, p := range before.Archives[i].Points {
				want[p.Timestamp] = p.Value
			}
		}
		actual := make(map[int64]float64, len(got.Points))
		for _, p := range got.Points {
			actual[p.Timestamp] = p.Value
		}
		for ts, value := range want {
			v, ok := actual[ts]
			if !ok || math.Float64bits(v) != math.Float64bits(value) {
				return fmt.Errorf("archive %d point %d differs or is missing", i, ts)
			}
		}
	}
	return nil
}
