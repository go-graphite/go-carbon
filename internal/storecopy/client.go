package storecopy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
)

type Client struct {
	URL   string
	Token string
	HTTP  *http.Client
}

func (c *Client) request(ctx context.Context, method, path string, query url.Values) (*http.Response, error) {
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+Endpoint+path+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	r.Header.Set("X-Buckyd-Authorization", c.Token)
	h := c.HTTP
	if h == nil {
		h = &http.Client{Timeout: 2 * time.Minute}
	}
	if h.CheckRedirect == nil {
		configured := *h
		configured.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		h = &configured
	}
	response, err := h.Do(r)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("copy API: %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	return response, nil
}

func (c *Client) Manifest(ctx context.Context, action, id string) (*Manifest, error) {
	method, path := http.MethodGet, ""
	switch action {
	case "begin":
		method = http.MethodPost
	case "capture", "seal":
		method, path = http.MethodPost, "/"+action
	case "release":
		method = http.MethodDelete
	case "status":
	default:
		return nil, errors.New("unknown copy action")
	}
	r, err := c.request(ctx, method, path, url.Values{"id": {id}})
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	var manifest *Manifest
	err = json.NewDecoder(io.LimitReader(r.Body, 32<<20)).Decode(&manifest)
	return manifest, err
}

type Limits struct {
	Workers int       `json:"workers"`
	Chunk   int64     `json:"chunk_bytes"`
	Rate    int64     `json:"bytes_per_second"`
	Expires time.Time `json:"expires"`
	Reason  string    `json:"reason,omitempty"`
}

type PullOptions struct {
	Workers       int
	Chunk         int64
	Rate          int64
	Apply         bool
	Lease         string
	Output        io.Writer
	Reserve       int64
	ReuseVerified bool
}

type Progress struct {
	Manifest *Manifest            `json:"manifest"`
	Chunks   map[string]string    `json:"chunks"`
	Files    map[string]FileStamp `json:"verified_files,omitempty"`
}

type chunk struct {
	file   File
	offset int64
	length int64
}

func (c chunk) key() string { return fmt.Sprintf("%s:%d:%d", c.file.Name, c.offset, c.length) }

func validateManifest(m *Manifest) error {
	if m == nil || m.Version != 1 || len(m.ID) != 32 || len(m.Files) == 0 || !time.Now().Before(m.Expires) {
		return errors.New("invalid or expired copy manifest")
	}
	seen := make(map[string]bool)
	for _, file := range m.Files {
		parts := strings.Split(file.Name, "/")
		if len(parts) != 2 || parts[0] != "store" && parts[0] != "input-log" && parts[0] != "config" ||
			parts[1] == "" || parts[1] == "." || parts[1] == ".." || strings.ContainsAny(file.Name, "\\\x00") || file.Size < 0 || seen[file.Name] {
			return errors.New("invalid or duplicate manifest file")
		}
		seen[file.Name] = true
	}
	if !seen["store/CHUNKSTORE"] || !seen["store/CURRENT"] {
		return errors.New("checkpoint is missing required store metadata")
	}
	return nil
}

// Pull transfers file ranges directly to their final offsets. Resume rechecks
// local chunk hashes before skipping; the state file never substitutes for data
// verification. Resume hashes may precede data writeback; every local range is
// rechecked after a restart. Completion is published only after all files sync.
func (c *Client) Pull(ctx context.Context, dest, id string, opts PullOptions) error {
	m, err := c.Manifest(ctx, "status", id)
	if err != nil {
		return err
	}
	if err = validateManifest(m); err != nil {
		return err
	}
	if opts.Workers < 1 || opts.Workers > 64 || opts.Chunk < 1<<20 || opts.Chunk > MaxChunk || opts.Rate < 0 {
		return errors.New("workers must be 1..64, chunk 1..64 MiB, rate nonnegative")
	}
	var total int64
	for _, file := range m.Files {
		total += file.Size
	}
	if opts.Output != nil {
		fmt.Fprintf(opts.Output, "copy id=%s files=%d bytes=%d destination=%s workers=%d chunk=%d rate=%d apply=%t sealed=%t reuse_verified=%t\n", id, len(m.Files), total, dest, opts.Workers, opts.Chunk, opts.Rate, opts.Apply, m.Sealed, opts.ReuseVerified)
	}
	if !opts.Apply {
		return nil
	}
	if opts.Lease != "" {
		limits, err := readLease(opts.Lease)
		if err != nil {
			return err
		}
		if limits.Workers == 0 {
			return errors.New("copy admissions are paused by the controller")
		}
	}
	if err := os.MkdirAll(dest, 0700); err != nil {
		return err
	}
	absolute, err := filepath.Abs(dest)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil || resolved != absolute {
		return errors.New("destination must be canonical without symlink parents")
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		return err
	}
	defer root.Close()
	if handoff.Locked(filepath.Join(dest, "store", "LOCK")) {
		return errors.New("destination store is running")
	}
	if _, err := root.Stat("PREPARED"); !errors.Is(err, os.ErrNotExist) {
		return errors.New("prepared destination cannot be copied again")
	}
	state := Progress{Chunks: make(map[string]string)}
	saved, err := root.ReadFile("copy-state.json")
	if err == nil {
		if err := json.Unmarshal(saved, &state); err != nil || state.Manifest == nil || state.Manifest.ID != id {
			return errors.New("destination belongs to another copy or has damaged state")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		entries, err := os.ReadDir(dest)
		if err != nil || len(entries) != 0 {
			return errors.New("new destination must be empty")
		}
	} else {
		return err
	}
	state.Manifest = m
	if state.Chunks == nil {
		return errors.New("invalid chunk state")
	}
	reuse := make(map[string]bool)
	if opts.ReuseVerified {
		for _, file := range m.Files {
			info, err := root.Stat(file.Name)
			if err != nil {
				continue
			}
			current, err := stamp(info)
			previous, known := state.Files[file.Name]
			if err == nil && known && current == previous && current.Size == file.Size {
				reuse[file.Name] = true
			}
		}
	}
	// One process owns the staging directory; a stale lock is removed only after
	// verifying that its previous process is gone. Never target a serving store.
	lock, err := root.OpenFile("copy.lock", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("acquire copy.lock: %w", err)
	}
	defer root.Remove("copy.lock")
	fmt.Fprintf(lock, "%d\n", os.Getpid())
	_ = lock.Close()
	for _, dir := range []string{"store", "input-log", "config"} {
		if err := root.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}
	guard, err := root.OpenFile("store/NEEDS-CATCHUP", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = guard.WriteString(m.ID + "\nwhole-store copy must be prepared before serving\n")
	if err = errors.Join(err, guard.Sync(), guard.Close()); err != nil {
		return err
	}
	dir, err := root.Open("store")
	if err != nil {
		return err
	}
	if err = errors.Join(dir.Sync(), dir.Close()); err != nil {
		return err
	}
	// Create each file once before disjoint ranges write concurrently. In
	// particular, two first chunks must not race to create the same pathname.
	for _, file := range m.Files {
		f, err := root.OpenFile(file.Name, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err == nil && info.Size() > file.Size {
			err = errors.New("destination file exceeds immutable manifest size")
		}
		if err == nil && info.Size() < file.Size {
			err = f.Truncate(file.Size)
		}
		if err = errors.Join(err, f.Close()); err != nil {
			return err
		}
	}
	if err := saveProgress(root, &state); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if opts.Lease != "" {
		// Cancellation must also interrupt rate reservations and HTTP bodies,
		// not only workers waiting for a concurrency admission.
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					limits, err := readLease(opts.Lease)
					if err != nil || strings.HasPrefix(limits.Reason, "hard_stop") {
						cancel()
						return
					}
				}
			}
		}()
	}
	var stateMu sync.Mutex
	var workerMu sync.Mutex
	var workerErr error
	var wg sync.WaitGroup
	jobs := make(chan chunk)
	pace := &pacer{start: time.Now()}
	admission := &copyAdmission{}
	checkpointAt := time.Now()
	for range opts.Workers {
		wg.Go(func() {
			for job := range jobs {
				var err error
				if !reuse[job.file.Name] {
					err = c.copyChunk(ctx, root, job, &state, &stateMu, pace, admission, opts)
				}
				if err == nil {
					stateMu.Lock()
					if time.Since(checkpointAt) > 10*time.Second {
						err = saveProgress(root, &state)
						checkpointAt = time.Now()
					}
					stateMu.Unlock()
				}
				if err != nil {
					workerMu.Lock()
					if workerErr == nil {
						workerErr = err
					}
					workerMu.Unlock()
					cancel()
				}
			}
		})
	}
enqueue:
	for _, file := range m.Files {
		for offset := int64(0); offset < file.Size; {
			size := opts.Chunk
			if opts.Lease != "" {
				if lease, err := readLease(opts.Lease); err == nil && lease.Chunk >= 1<<20 && lease.Chunk <= opts.Chunk {
					size = lease.Chunk
				}
			}
			job := chunk{file, offset, min(size, file.Size-offset)}
			select {
			case jobs <- job:
			case <-ctx.Done():
				break enqueue
			}
			offset += job.length
		}
	}
	close(jobs)
	wg.Wait()
	// Even an interrupted pass publishes only ranges that passed the checksum.
	if err := syncFiles(root, m.Files); err != nil {
		return err
	}
	if err := saveProgress(root, &state); err != nil {
		return err
	}
	if workerErr != nil {
		return workerErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state.Files = make(map[string]FileStamp, len(m.Files))
	for _, file := range m.Files {
		info, err := root.Stat(file.Name)
		if err != nil {
			return err
		}
		if verified, err := stamp(info); err == nil {
			state.Files[file.Name] = verified
		}
	}
	if err := saveProgress(root, &state); err != nil {
		return err
	}
	if m.Sealed {
		return root.WriteFile("COPY-COMPLETE.json", mustJSON(m), 0600)
	}
	return nil
}

func saveProgress(root *os.Root, state *Progress) error {
	f, err := root.OpenFile("copy-state.json.next", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(state)
	if err = errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := root.Rename("copy-state.json.next", "copy-state.json"); err != nil {
		return err
	}
	d, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func mustJSON(value any) []byte { b, _ := json.MarshalIndent(value, "", "  "); return b }

func (c *Client) copyChunk(ctx context.Context, root *os.Root, job chunk, state *Progress, mu *sync.Mutex, pace *pacer, admission *copyAdmission, opts PullOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := CheckFreeSpace(root.Name(), opts.Reserve); err != nil {
		return err
	}
	mu.Lock()
	digest := state.Chunks[job.key()]
	mu.Unlock()
	f, err := root.OpenFile(job.file.Name, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if digest != "" {
		h := sha256.New()
		if n, err := io.Copy(h, io.NewSectionReader(f, job.offset, job.length)); err == nil && n == job.length && hex.EncodeToString(h.Sum(nil)) == digest {
			return nil
		}
	}
	if err := admission.acquire(ctx, opts); err != nil {
		return err
	}
	defer admission.release()
	if err := pace.wait(ctx, job.length, currentRate(opts)); err != nil {
		return err
	}
	query := url.Values{"id": {state.Manifest.ID}, "name": {job.file.Name}, "offset": {strconv.FormatInt(job.offset, 10)}, "length": {strconv.FormatInt(job.length, 10)}}
	r, err := c.request(ctx, http.MethodGet, "/file", query)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	hash := sha256.New()
	writer := io.NewOffsetWriter(f, job.offset)
	n, err := io.CopyBuffer(io.MultiWriter(writer, hash), io.LimitReader(r.Body, job.length+1), make([]byte, 256<<10))
	if err != nil {
		return err
	}
	digest = hex.EncodeToString(hash.Sum(nil))
	if n != job.length || digest != r.Trailer.Get("X-Content-SHA256") {
		return fmt.Errorf("checksum or length mismatch: %s offset=%d", job.file.Name, job.offset)
	}
	mu.Lock()
	state.Chunks[job.key()] = digest
	mu.Unlock()
	return nil
}

func syncFiles(root *os.Root, files []File) error {
	jobs := make(chan File)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var result error
	for range min(8, len(files)) {
		wg.Go(func() {
			for file := range jobs {
				f, err := root.OpenFile(file.Name, os.O_RDWR, 0600)
				if err == nil {
					err = errors.Join(f.Sync(), f.Close())
				}
				if err != nil {
					mu.Lock()
					result = errors.Join(result, err)
					mu.Unlock()
				}
			}
		})
	}
	for _, file := range files {
		jobs <- file
	}
	close(jobs)
	wg.Wait()
	return result
}

func readLease(path string) (Limits, error) {
	data, err := os.ReadFile(path)
	var limits Limits
	if err == nil {
		err = json.Unmarshal(data, &limits)
	}
	if err == nil && (!time.Now().Before(limits.Expires) || limits.Workers < 0 || limits.Workers > 64 || limits.Rate < 0) {
		err = errors.New("copy controller lease expired or is invalid")
	}
	return limits, err
}

type copyAdmission struct {
	mu     sync.Mutex
	active int
}

func (a *copyAdmission) release() { a.mu.Lock(); a.active--; a.mu.Unlock() }

func (a *copyAdmission) acquire(ctx context.Context, opts PullOptions) error {
	for {
		workers := opts.Workers
		if opts.Lease != "" {
			limits, err := readLease(opts.Lease)
			if err != nil {
				return err
			}
			workers = min(workers, limits.Workers)
		}
		a.mu.Lock()
		allowed := a.active < workers
		if allowed {
			a.active++
		}
		a.mu.Unlock()
		if allowed {
			return nil
		}
		if err := waitFor(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
}

func currentRate(opts PullOptions) int64 {
	if opts.Lease != "" {
		if limits, err := readLease(opts.Lease); err == nil && limits.Rate > 0 {
			if opts.Rate == 0 {
				return limits.Rate
			}
			return min(opts.Rate, limits.Rate)
		}
	}
	return opts.Rate
}

type pacer struct {
	mu    sync.Mutex
	start time.Time
}

func (p *pacer) wait(ctx context.Context, n, rate int64) error {
	if rate == 0 {
		return nil
	}
	p.mu.Lock()
	// Reserve bytes on a shared clock, bounding aggregate traffic across streams.
	now := time.Now()
	if p.start.Before(now) {
		p.start = now
	}
	delay := time.Until(p.start)
	p.start = p.start.Add(time.Duration(float64(n) / float64(rate) * float64(time.Second)))
	p.mu.Unlock()
	return waitFor(ctx, delay)
}

func waitFor(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
