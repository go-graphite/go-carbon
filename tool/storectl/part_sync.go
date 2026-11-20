package main

// Whole-file transfer adapted from the immutable-parts copy experiment:
// bounded parallel streams, SHA256 while receiving, fsync before completion.
// The destination is an offline staging repository, never a live DB ingest.
import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/internal/v2core"
)

const partManifestLimit = 64 << 20

type partManifestReader struct{ parts []v2core.SnapshotPart }

func (r partManifestReader) ReadPartFiles(context.Context) ([]v2core.SnapshotPart, error) {
	return r.parts, nil
}

func readPartSnapshot(path string) (v2core.PartSnapshot, error) {
	var s v2core.PartSnapshot
	f, err := os.Open(path)
	if err != nil {
		return s, err
	}
	defer f.Close()
	err = json.NewDecoder(io.LimitReader(f, partManifestLimit)).Decode(&s)
	return s, err
}

func partSyncMain(args []string) error {
	fs := flag.NewFlagSet("sync --parts", flag.ContinueOnError)
	fs.Bool("parts", false, "copy immutable parts into an offline staging repository")
	portable := fs.String("portable-manifest", "", "retained canonical v2part manifest; files are <sha256>.sst beside it")
	source := fs.String("snapshot-source", "", "trusted JSON inventory from a retained format reader (local Paths)")
	dir := fs.String("snapshot-dir", "", "hard-link snapshot directory (new when --snapshot-source is set)")
	listen := fs.String("part-listen", "", "serve this immutable snapshot until stopped")
	from := fs.String("part-from", "", "snapshot server URL")
	target := fs.String("part-target", "", "offline part repository; reuse unchanged to resume")
	streams := fs.Int("part-streams", 8, "parallel whole-part streams (1..64)")
	tokenPath := fs.String("part-token-file", "", "bearer token file; required for non-loopback server")
	allowHTTP := fs.Bool("part-allow-http", false, "allow trusted fixture transfer over unencrypted HTTP")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected part-copy arguments")
	}
	var token string
	if *tokenPath != "" {
		b, e := os.ReadFile(*tokenPath)
		if e != nil {
			return e
		}
		token = strings.TrimSpace(string(b))
		if len(token) < 32 {
			return errors.New("part token must contain at least 32 characters")
		}
	}
	if *portable != "" {
		if *source != "" || *dir == "" {
			return errors.New("--portable-manifest requires --snapshot-dir and excludes --snapshot-source")
		}
		_, err := v2core.LinkPartSnapshot(context.Background(), *dir, v2core.PortableSnapshotReader{ManifestPath: *portable, Directory: filepath.Dir(*portable)})
		return err
	}
	if *source != "" {
		if *dir == "" {
			return errors.New("--snapshot-source requires --snapshot-dir")
		}
		s, e := readPartSnapshot(*source)
		if e != nil {
			return e
		}
		if _, e = v2core.LinkPartSnapshot(context.Background(), *dir, partManifestReader{s.Parts}); e != nil {
			return e
		}
	}
	if *listen != "" {
		if *dir == "" || *from != "" || *target != "" {
			return errors.New("part server requires only --snapshot-dir and --part-listen")
		}
		host, _, e := net.SplitHostPort(*listen)
		if e != nil {
			return e
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			if token == "" || !*allowHTTP {
				return errors.New("non-loopback fixture server requires --part-token-file and --part-allow-http")
			}
		}
		s, e := readPartSnapshot(filepath.Join(*dir, "snapshot.json"))
		if e != nil {
			return e
		}
		if e = s.Validate(); e != nil {
			return e
		}
		srv := &http.Server{Addr: *listen, Handler: partSnapshotHandler(*dir, s, token), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: time.Minute, WriteTimeout: 10 * time.Minute}
		return srv.ListenAndServe()
	}
	if *from != "" {
		u, e := url.Parse(*from)
		if e != nil {
			return e
		}
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "https" && !(u.Scheme == "http" && (*allowHTTP || (ip != nil && ip.IsLoopback()))) {
			return errors.New("part copy requires HTTPS or explicit --part-allow-http for fixtures")
		}
		if *target == "" {
			return errors.New("--part-from requires --part-target")
		}
		result, e := receivePartSnapshot(context.Background(), http.DefaultClient, *from, token, *target, *streams)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	if *source != "" {
		return nil
	}
	return errors.New("choose --snapshot-source, --part-listen, or --part-from")
}

func partSnapshotHandler(dir string, s v2core.PartSnapshot, token string) http.Handler {
	manifest, _ := json.Marshal(s)
	parts := make(map[string]v2core.SnapshotPart, len(s.Parts))
	for _, p := range s.Parts {
		parts[p.ID] = p
	}
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := sha256.Sum256([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
		if token != "" && subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/snapshot.json" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(manifest)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/parts/")
		p, ok := parts[id]
		if !ok || r.URL.Path != "/parts/"+id {
			http.NotFound(w, r)
			return
		}
		f, e := os.Open(filepath.Join(dir, id+".part"))
		if e != nil {
			http.Error(w, "part unavailable", 500)
			return
		}
		defer f.Close()
		st, e := f.Stat()
		if e != nil || st.Size() != p.Bytes {
			http.Error(w, "pinned part changed", 500)
			return
		}
		w.Header().Set("ETag", `"`+id+`"`)
		w.Header().Set("Content-Length", fmt.Sprint(p.Bytes))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = io.CopyN(w, f, p.Bytes)
	})
}

type partCopyResult struct {
	Parts   int
	Skipped int
	Bytes   int64
	Seconds float64
	GBps    float64
}

func partGet(ctx context.Context, client *http.Client, base, path, token string) (*http.Response, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+path, nil)
	if e != nil {
		return nil, e
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, e := client.Do(req)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("part HTTP status %d", resp.StatusCode)
	}
	return resp, nil
}

func receivePartSnapshot(ctx context.Context, client *http.Client, base, token, dir string, streams int) (out partCopyResult, err error) {
	if streams < 1 || streams > 64 {
		return out, errors.New("part streams must be 1..64")
	}
	start := time.Now()
	if err = os.MkdirAll(dir, 0700); err != nil {
		return out, err
	}
	if err = v2core.SyncPartDirectory(filepath.Dir(dir)); err != nil {
		return out, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "copy.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return out, err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return out, errors.New("part repository is already in use")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	resp, err := partGet(ctx, client, base, "/snapshot.json", token)
	if err != nil {
		return out, err
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, partManifestLimit+1))
	_ = resp.Body.Close()
	if err != nil {
		return out, err
	}
	if len(b) > partManifestLimit {
		return out, errors.New("part manifest too large")
	}
	var s v2core.PartSnapshot
	if err = json.Unmarshal(b, &s); err != nil {
		return out, err
	}
	if err = s.Validate(); err != nil {
		return out, err
	}
	// An interrupted file has no completion status. A renamed, hash-valid file
	// is itself the receipt; after a crash before directory fsync it is either
	// present and reconciled locally, or absent and must be fetched again.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan v2core.SnapshotPart)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if ctx.Err() != nil {
					continue
				}
				skipped, n, e := receiveOnePart(ctx, client, base, token, dir, p)
				mu.Lock()
				switch {
				case e != nil:
					if first == nil {
						first = e
					}
					cancel()
				case skipped:
					out.Skipped++
				default:
					out.Parts++
					out.Bytes += n
				}
				mu.Unlock()
			}
		}()
	}
	for _, p := range s.Parts {
		select {
		case jobs <- p:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return out, first
	}
	if err = ctx.Err(); err != nil {
		return out, err
	}
	// Atomically publish the requested manifest only after all parts are durable.
	// Target-only parts remain in the repository for other snapshots/owners.
	tmp, e := os.CreateTemp(dir, "manifest-*.partial")
	if e != nil {
		return out, e
	}
	name := tmp.Name()
	defer os.Remove(name)
	_, err = tmp.Write(b)
	if err == nil {
		err = tmp.Sync()
	}
	err = errors.Join(err, tmp.Close())
	if err != nil {
		return out, err
	}
	if err = os.Rename(name, filepath.Join(dir, "snapshot.json")); err != nil {
		return out, err
	}
	if err = v2core.SyncPartDirectory(dir); err != nil {
		return out, err
	}
	out.Seconds = time.Since(start).Seconds()
	out.GBps = float64(out.Bytes) / out.Seconds / 1e9
	return out, nil
}

func receiveOnePart(ctx context.Context, client *http.Client, base, token, dir string, p v2core.SnapshotPart) (bool, int64, error) {
	final := filepath.Join(dir, p.ID+".part")
	if f, e := os.Open(final); e == nil {
		h := sha256.New()
		n, e := io.Copy(h, f)
		e = errors.Join(e, f.Close())
		if e != nil {
			return false, 0, e
		}
		if n != p.Bytes || hex.EncodeToString(h.Sum(nil)) != p.SHA256 {
			return false, 0, errors.New("completed part checksum mismatch")
		}
		return true, 0, nil
	} else if !errors.Is(e, os.ErrNotExist) {
		return false, 0, e
	}
	resp, e := partGet(ctx, client, base, "/parts/"+p.ID, token)
	if e != nil {
		return false, 0, e
	}
	defer resp.Body.Close()
	if resp.ContentLength >= 0 && resp.ContentLength != p.Bytes {
		return false, 0, errors.New("part length mismatch")
	}
	// Fixed per-ID stage names bound crash leftovers to one part per stream.
	stage := final + ".partial"
	f, e := os.OpenFile(stage, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return false, 0, e
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(stage)
		}
	}()
	h := sha256.New()
	n, e := io.CopyBuffer(io.MultiWriter(f, h), io.LimitReader(resp.Body, p.Bytes+1), make([]byte, 1<<20))
	if e == nil && (n != p.Bytes || hex.EncodeToString(h.Sum(nil)) != p.SHA256) {
		e = errors.New("part checksum or length mismatch")
	}
	if e == nil {
		e = f.Sync()
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return false, n, e
	}
	if e = os.Rename(stage, final); e != nil {
		return false, n, e
	}
	if e = v2core.SyncPartDirectory(dir); e != nil {
		return false, n, e
	}
	ok = true
	return false, n, nil
}
