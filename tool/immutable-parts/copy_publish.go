package main

// Offline integration primitives. The daemon must supply a coherent, pinned
// snapshot and a durable target generation before it can use this transport.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/go-graphite/go-carbon/internal/partsformat"
)

const copyManifestLimit = 256 << 20

type copySnapshot struct {
	Version                                 uint32
	Catalogue, Policy, Tombstones, Frontier [32]byte
	Partitions                              []copyPartition
}

// Paths never cross the publication boundary. The content address binds all
// roots and every physical/logical descriptor to one immutable publication.
func copyPublish(snapshot copySnapshot) ([]byte, [32]byte, error) {
	if snapshot.Version != 1 {
		return nil, [32]byte{}, errors.New("unsupported snapshot")
	}
	snapshot.Partitions = append([]copyPartition(nil), snapshot.Partitions...)
	for i := range snapshot.Partitions {
		p := &snapshot.Partitions[i]
		p.Files = append([]copyFile(nil), p.Files...)
		for j := range p.Files {
			p.Files[j].Path = ""
		}
	}
	b, err := json.Marshal(snapshot)
	if len(b) > copyManifestLimit {
		return nil, [32]byte{}, errors.New("manifest too large")
	}
	return b, sha256.Sum256(b), err
}

// Require TLS with verified client chains AND an explicitly allowed certificate.
// The caller configures tls.RequireAndVerifyClientCert with its private trust
// roots. Content addressing and standard byte ranges permit exact resumption.
func copyManifestHandler(blob []byte, allowed map[[32]byte]bool) http.Handler {
	frozen := bytes.Clone(blob)
	sum := sha256.Sum256(frozen)
	path := "/v1/manifests/" + hex.EncodeToString(sum[:])
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	peers := make(map[[32]byte]bool, len(allowed))
	for k, v := range allowed {
		peers[k] = v
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 || !peers[sha256.Sum256(r.TLS.PeerCertificates[0].Raw)] {
			http.Error(w, "client certificate denied", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != path {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, immutable")
		http.ServeContent(w, r, "manifest.json", time.Time{}, bytes.NewReader(frozen))
	})
}

// Each successful chunk is synced before returning. Interrupted downloads keep
// only their private partial file. A completed object is hash checked, renamed,
// and directory synced. Reopening a finished object issues no network request.
func copyFetchManifest(ctx context.Context, client *http.Client, base, dir string, sum [32]byte) ([]byte, error) {
	if err := copyEnsureDir(dir); err != nil {
		return nil, err
	}
	name := hex.EncodeToString(sum[:])
	final := filepath.Join(dir, name+".json")
	if b, err := os.ReadFile(final); err == nil {
		if sha256.Sum256(b) != sum {
			return nil, errors.New("completed manifest checksum mismatch")
		}
		return b, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(final+".partial", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	offset := st.Size()
	if offset > copyManifestLimit {
		return nil, errors.New("partial manifest too large")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/manifests/"+name, nil)
	if err != nil {
		return nil, err
	}
	if req.URL.Scheme != "https" {
		return nil, errors.New("manifest publication requires HTTPS")
	}
	req.Header.Set("If-Match", `"`+name+`"`)
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && offset > 0 {
		// The previous process may have received every byte before returning an
		// interruption. Only the content hash can promote that existing partial.
		b, e := os.ReadFile(final + ".partial")
		if e != nil {
			return nil, e
		}
		if sha256.Sum256(b) != sum {
			return nil, errors.New("invalid complete partial")
		}
	} else {
		expected := http.StatusOK
		if offset > 0 {
			expected = http.StatusPartialContent
		}
		if resp.StatusCode != expected || resp.Header.Get("ETag") != `"`+name+`"` {
			return nil, errors.New("manifest version/range changed")
		}
		if offset > 0 {
			var start, end, total int64
			n, e := fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total)
			if e != nil || n != 3 || start != offset || end < start || total != end+1 || total > copyManifestLimit {
				return nil, errors.New("invalid manifest range")
			}
		}
		if _, err = f.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
		_, readErr := io.Copy(f, io.LimitReader(resp.Body, copyManifestLimit-offset+1))
		if err = f.Sync(); err != nil {
			return nil, err
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	b, err := os.ReadFile(final + ".partial")
	if err != nil {
		return nil, err
	}
	if len(b) > copyManifestLimit || sha256.Sum256(b) != sum {
		return nil, errors.New("manifest checksum mismatch")
	}
	if err = os.Rename(final+".partial", final); err != nil {
		return nil, err
	}
	return b, copySyncDir(dir)
}

// Sync each newly created directory entry as well as receipt/object contents.
func copyEnsureDir(dir string) error {
	if st, err := os.Stat(dir); err == nil {
		if !st.IsDir() {
			return errors.New("copy state path is not a directory")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(filepath.Clean(dir))
	if err := copyEnsureDir(parent); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return copySyncDir(parent)
}

func copySyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// A target generation identifies its durable store incarnation and installed
// partition frontier. The owner MUST change it whenever the target changes.
// Reconcile resolves the crash gap after durable ingest but before receipt sync
// using the target's persisted logical digest, without retransferring data.
// One scheduler owns a ledger directory; partition tasks are unique.
type copyDoneLedger struct {
	Dir        string
	Snapshot   [32]byte
	Generation func(int) (string, error)
	Reconcile  func(int) (bool, error)
}
type copyReceipt struct {
	Snapshot   [32]byte
	Partition  [32]byte
	Generation string
	Done       bool
}

func copyReceiptKey(p copyPartition) ([32]byte, error) {
	if p.Blocks != nil {
		prefix, err := partsformat.PartitionPrefixForGroup(p.Manifest.Group, p.Manifest.VShard, p.Manifest.Partition)
		if err != nil {
			return [32]byte{}, err
		}
		if p.Blocks.Version != 1 || !bytes.Equal(prefix, p.Blocks.Prefix) || p.Manifest.Policy != p.Blocks.Policy || copyLogicalDigest(*p.Blocks) != p.Blocks.Digest {
			return [32]byte{}, errors.New("invalid logical receipt identity")
		}
		h := sha256.New()
		_, _ = h.Write([]byte("copy-logical-receipt-v1"))
		_, _ = h.Write(prefix)
		_, _ = h.Write(p.Manifest.Policy[:])
		_, _ = h.Write(p.Blocks.Digest[:])
		var sum [32]byte
		copy(sum[:], h.Sum(nil))
		return sum, nil
	}
	b, _, err := copyPublish(copySnapshot{Version: 1, Partitions: []copyPartition{p}})
	return sha256.Sum256(b), err
}
func (l *copyDoneLedger) state(pi int, p copyPartition) (copyReceipt, string, error) {
	var r copyReceipt
	if l.Generation == nil || l.Reconcile == nil {
		return r, "", errors.New("durable target generation and reconciliation required")
	}
	key, err := copyReceiptKey(p)
	if err != nil {
		return r, "", err
	}
	gen, err := l.Generation(pi)
	if err != nil || gen == "" {
		return r, "", errors.Join(err, errors.New("empty target generation"))
	}
	r = copyReceipt{Snapshot: l.Snapshot, Partition: key, Generation: gen}
	path := filepath.Join(l.Dir, hex.EncodeToString(key[:])+".json")
	return r, path, nil
}
func (l *copyDoneLedger) begin(pi int, p copyPartition) (bool, error) {
	want, path, err := l.state(pi, p)
	if err != nil {
		return false, err
	}
	b, err := os.ReadFile(path)
	if err == nil {
		var got copyReceipt
		if err = json.Unmarshal(b, &got); err != nil {
			return false, err
		}
		if got.Partition == want.Partition {
			if got.Done && got.Snapshot == want.Snapshot && got.Generation == want.Generation {
				return true, nil
			}
			// Ingest can advance the durable target frontier before a pending receipt
			// is finished. Reconcile even when that advancement changed Generation.
			done, e := l.Reconcile(pi)
			if e != nil {
				return false, e
			}
			if done {
				return true, l.finish(pi, p)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return false, l.write(path, want)
}
func (l *copyDoneLedger) finish(pi int, p copyPartition) error {
	r, path, err := l.state(pi, p)
	if err != nil {
		return err
	}
	r.Done = true
	return l.write(path, r)
}
func (l *copyDoneLedger) write(path string, r copyReceipt) (retErr error) {
	if err := copyEnsureDir(l.Dir); err != nil {
		return err
	}
	f, err := os.CreateTemp(l.Dir, "receipt-")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return copySyncDir(l.Dir)
}
