// Package mergepass is a finish-first merge of donor stores into a target
// store with bucky semantics: a fixed name list written once, one merge per
// name and donor where the target's existing points win, a failing name logged
// and skipped, and an append-only done log so a rerun skips finished names.
//
// The target does the merge itself through /v1/transfer/pull: it exports the
// names from the donor and imports them with a union in which existing target
// points win. A sum-v1 digest of both sides first skips names whose donor
// archives already equal the target's, because a pull always rewrites the name.
package mergepass

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Options configures one merge pass. State holds every file the pass writes.
type Options struct {
	To        string   // target buckyd origin, e.g. http://10.0.0.1:4242
	From      []string // donor origins as the target's transfer allowlist names them
	State     string
	Streams   int    // default stream count when the streams file is absent
	Batch     int    // names per request
	Limit     int    // at most this many names in the snapshot (0: all)
	After     string // snapshot starts after this name
	Until     int64
	NoDigest  bool // pull every name (no digest gate)
	Sample    int  // parity sample size
	MaxBytes  int64
	JWTSecret string
	Retries   int           // attempts for a single failing name
	Backoff   time.Duration // first retry delay, doubled per attempt
	WaitOut   time.Duration // how long a down or overloaded daemon is waited out
	Client    *http.Client
	Log       io.Writer
	// afterBatch is a test hook called after each batch's done log write.
	afterBatch func(done int64)
}

// Report is written to State/report.json at the end of the pass.
type Report struct {
	Started        time.Time                   `json:"started"`
	Finished       time.Time                   `json:"finished"`
	Seconds        float64                     `json:"seconds"`
	Names          int                         `json:"names"`
	Resumed        int64                       `json:"resumed"`
	Done           int64                       `json:"done"`
	Pulled         int64                       `json:"pulled"`
	Identical      int64                       `json:"identical"`
	Absent         int64                       `json:"absent"`
	Retried        int64                       `json:"retried"`
	SkippedNames   int                         `json:"skipped_names"`
	Skipped        []Failure                   `json:"skipped,omitempty"`
	PullBytes      int64                       `json:"pull_bytes"`
	NamesPerSecond float64                     `json:"names_per_second"`
	Streams        int                         `json:"streams"`
	Parity         map[string]*ParityCounts    `json:"parity"`
	ParitySample   int                         `json:"parity_sample"`
	Window         map[string]int64            `json:"window"`
	ParityDetail   map[string][]ParityMismatch `json:"parity_detail,omitempty"`
}

// ParityCounts classifies sampled names per source after the pass.
type ParityCounts struct {
	Equal         int `json:"equal"`
	MissingTarget int `json:"missing_on_target"`
	Differ        int `json:"differ"`
	SourceAbsent  int `json:"source_absent"`
	Errors        int `json:"errors"`
	Unsupported   int `json:"unsupported_source"`
}

// ParityMismatch names one sampled name that is not equal.
type ParityMismatch struct {
	Name   string `json:"name"`
	Result string `json:"result"`
}

// Failure is one skipped name, the source that failed and the reason.
type Failure struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	Reason string `json:"reason"`
}

type entry struct {
	name string
	mask uint64 // bit i: From[i] listed the name at snapshot time
}

type pass struct {
	o       Options
	client  *http.Client
	token   tokenCache
	doneLog *syncLog
	errLog  *syncLog
	done    map[uint64]struct{}
	doneMu  sync.Mutex
	report  Report
	lastErr sync.Map // name -> Failure
	noSum   sync.Map // source -> true when it cannot serve sum-v1 digests
	limit   *gate
}

// Run executes or resumes the pass. It returns an error only for
// configuration and state-directory problems; skipped names are reported.
func Run(ctx context.Context, o Options) (*Report, error) {
	if err := o.defaults(); err != nil {
		return nil, err
	}
	p := &pass{o: o, client: o.Client, done: map[uint64]struct{}{}, limit: newGate(o.Streams)}
	p.token.secret = o.JWTSecret
	p.report.Started = time.Now()
	if p.client == nil {
		p.client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 256, IdleConnTimeout: time.Minute}, Timeout: 10 * time.Minute}
	}
	if err := os.MkdirAll(o.State, 0o755); err != nil {
		return nil, err
	}
	entries, err := p.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	p.report.Names = len(entries)
	if err := p.loadDone(); err != nil {
		return nil, err
	}
	if p.doneLog, err = openLog(filepath.Join(o.State, "done.log")); err != nil {
		return nil, err
	}
	defer p.doneLog.close()
	if p.errLog, err = openLog(filepath.Join(o.State, "errors.log")); err != nil {
		return nil, err
	}
	defer p.errLog.close()
	pending := make([]entry, 0, len(entries))
	for _, e := range entries {
		if p.isDone(e.name) {
			p.report.Resumed++
		} else {
			pending = append(pending, e)
		}
	}
	p.logf("merge_start names=%d resumed=%d pending=%d sources=%d", len(entries), p.report.Resumed, len(pending), len(o.From))
	p.run(ctx, pending, o.Batch)
	// Final retry pass: every name still not done, one name per request.
	var retry []entry
	for _, e := range pending {
		if !p.isDone(e.name) {
			retry = append(retry, e)
		}
	}
	if len(retry) > 0 && ctx.Err() == nil {
		p.logf("merge_retry_pass names=%d", len(retry))
		p.run(ctx, retry, 1)
	}
	if ctx.Err() != nil {
		return &p.report, ctx.Err()
	}
	for _, e := range retry {
		if !p.isDone(e.name) {
			p.report.SkippedNames++
			if f, ok := p.lastErr.Load(e.name); ok && len(p.report.Skipped) < 10000 {
				p.report.Skipped = append(p.report.Skipped, f.(Failure))
			}
		}
	}
	p.report.Finished = time.Now()
	p.report.Seconds = p.report.Finished.Sub(p.report.Started).Seconds()
	if p.report.Seconds > 0 {
		p.report.NamesPerSecond = float64(p.report.Done) / p.report.Seconds
	}
	p.report.Streams = p.limit.size()
	p.parity(ctx, entries)
	if err := writeJSON(filepath.Join(o.State, "report.json"), &p.report); err != nil {
		return nil, err
	}
	p.logf("merge_done names=%d done=%d pulled=%d identical=%d absent=%d skipped=%d seconds=%.1f", p.report.Names, p.report.Done, p.report.Pulled, p.report.Identical, p.report.Absent, p.report.SkippedNames, p.report.Seconds)
	return &p.report, nil
}

func (o *Options) defaults() error {
	if o.To == "" || len(o.From) == 0 || o.State == "" {
		return errors.New("merge needs --to, --from and --state")
	}
	if len(o.From) > 64 {
		return errors.New("merge supports at most 64 sources")
	}
	for _, origin := range append([]string{o.To}, o.From...) {
		if u, err := url.Parse(origin); err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("invalid origin %q", origin)
		}
	}
	if o.Streams <= 0 {
		o.Streams = 8
	}
	if o.Batch <= 0 || o.Batch > 10000 {
		o.Batch = 500
	}
	if o.Retries <= 0 {
		o.Retries = 3
	}
	if o.Backoff <= 0 {
		o.Backoff = time.Second
	}
	if o.WaitOut <= 0 {
		o.WaitOut = 30 * time.Minute
	}
	if o.Sample < 0 {
		o.Sample = 0
	}
	if o.Log == nil {
		o.Log = os.Stderr
	}
	return nil
}

func (p *pass) logf(format string, args ...any) {
	fmt.Fprintf(p.o.Log, "%s %s\n", time.Now().Format(time.RFC3339), fmt.Sprintf(format, args...))
}

// run processes entries in batches over a resizable number of streams. The
// streams file is read before each batch, so tuning needs no restart.
func (p *pass) run(ctx context.Context, entries []entry, batch int) {
	var wg sync.WaitGroup
	ticker, stop := time.NewTicker(30*time.Second), make(chan struct{})
	defer close(stop)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
			p.logf("merge_progress done=%d pulled=%d identical=%d retried=%d streams=%d", atomic.LoadInt64(&p.report.Done), atomic.LoadInt64(&p.report.Pulled), atomic.LoadInt64(&p.report.Identical), atomic.LoadInt64(&p.report.Retried), p.limit.size())
		}
	}()
	for start := 0; start < len(entries) && ctx.Err() == nil; start += batch {
		end := min(start+batch, len(entries))
		p.limit.resize(p.streams())
		if !p.limit.acquire(ctx) {
			break
		}
		wg.Add(1)
		go func(work []entry) {
			defer wg.Done()
			defer p.limit.release()
			p.batch(ctx, work)
		}(entries[start:end])
	}
	wg.Wait()
}

func (p *pass) streams() int {
	data, err := os.ReadFile(filepath.Join(p.o.State, "streams"))
	if err != nil {
		return p.o.Streams
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || n < 1 || n > 1024 {
		return p.o.Streams
	}
	return n
}

// batch merges every source into the target for these names. A name is done
// only when every source that listed it merged, was identical or was absent.
func (p *pass) batch(ctx context.Context, work []entry) {
	failed := map[string]bool{}
	for i, source := range p.o.From {
		var names []string
		for _, e := range work {
			if e.mask&(1<<i) != 0 && !failed[e.name] {
				names = append(names, e.name)
			}
		}
		if len(names) == 0 {
			continue
		}
		need := names
		if _, off := p.noSum.Load(source); !p.o.NoDigest && !off {
			var bad map[string]string
			need, bad = p.differing(ctx, source, names)
			for name, reason := range bad {
				failed[name] = true
				p.fail(name, source, reason)
			}
		}
		for name, reason := range p.pull(ctx, source, need) {
			failed[name] = true
			p.fail(name, source, reason)
		}
	}
	if ctx.Err() != nil {
		return
	}
	var lines bytes.Buffer
	var n int64
	for _, e := range work {
		if !failed[e.name] {
			lines.WriteString(e.name)
			lines.WriteByte('\n')
			n++
		}
	}
	if n == 0 {
		return
	}
	if err := p.doneLog.write(lines.Bytes()); err != nil {
		p.logf("merge_done_log_error err=%q", err)
		return
	}
	p.doneMu.Lock()
	for _, e := range work {
		if !failed[e.name] {
			p.done[nameKey(e.name)] = struct{}{}
		}
	}
	p.doneMu.Unlock()
	total := atomic.AddInt64(&p.report.Done, n)
	if p.o.afterBatch != nil {
		p.o.afterBatch(total)
	}
}

func (p *pass) fail(name, source, reason string) {
	f := Failure{name, source, reason}
	p.lastErr.Store(name, f)
	line := name + "\t" + source + "\t" + strings.ReplaceAll(reason, "\n", " ") + "\n"
	if err := p.errLog.write([]byte(line)); err != nil {
		p.logf("merge_error_log_error err=%q", err)
	}
}

func (p *pass) isDone(name string) bool {
	p.doneMu.Lock()
	defer p.doneMu.Unlock()
	_, ok := p.done[nameKey(name)]
	return ok
}

// nameKey keeps the done set at 8 bytes per name for tens of millions of names.
func nameKey(name string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(name))
	return h.Sum64()
}

func (p *pass) loadDone() error {
	f, err := os.Open(filepath.Join(p.o.State, "done.log"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		if line := scanner.Text(); line != "" {
			p.done[nameKey(line)] = struct{}{}
		}
	}
	return scanner.Err()
}

// gate is a semaphore whose size may change between acquisitions.
type gate struct {
	mu     sync.Mutex
	cond   *sync.Cond
	limit  int
	active int
}

func newGate(n int) *gate {
	g := &gate{limit: n}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *gate) resize(n int) {
	g.mu.Lock()
	g.limit = n
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *gate) size() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

func (g *gate) acquire(ctx context.Context) bool {
	stop := context.AfterFunc(ctx, g.cond.Broadcast)
	defer stop()
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.active >= g.limit && ctx.Err() == nil {
		g.cond.Wait()
	}
	if ctx.Err() != nil {
		return false
	}
	g.active++
	return true
}

func (g *gate) release() {
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
	g.cond.Broadcast()
}

// syncLog is an append-only file; every write is fsynced before it returns.
type syncLog struct {
	mu sync.Mutex
	f  *os.File
}

func openLog(path string) (*syncLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &syncLog{f: f}, nil
}

func (l *syncLog) write(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.f.Write(data); err != nil {
		return err
	}
	return l.f.Sync()
}

func (l *syncLog) close() { _ = l.f.Close() }

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(path, append(data, '\n'))
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// tokenCache mints a short-lived root read+update token from the shared
// buckyd secret; an empty secret path sends no token (auth disabled).
type tokenCache struct {
	mu      sync.Mutex
	secret  string
	token   string
	renewAt time.Time
}

func (t *tokenCache) get() (string, error) {
	if t.secret == "" {
		return "", nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if t.token != "" && now.Before(t.renewAt) {
		return t.token, nil
	}
	key, err := os.ReadFile(t.secret)
	if err != nil {
		return "", fmt.Errorf("jwt secret: %w", err)
	}
	key = bytes.TrimSpace(key)
	defer clear(key)
	claims := jwt.MapClaims{"iat": now.Unix(), "exp": now.Add(4 * time.Hour).Unix(), "namespaces": []string{"*"}, "ops": []string{"read", "update"}}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(key)
	if err != nil {
		return "", err
	}
	t.token, t.renewAt = signed, now.Add(time.Hour)
	return signed, nil
}

// httpError carries the status of a failed request.
type httpError struct {
	status int
	body   string
}

func (e *httpError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.status, e.body) }

func statusOf(err error) int {
	var h *httpError
	if errors.As(err, &h) {
		return h.status
	}
	return 0
}

// transient errors mean a daemon is restarting or overloaded: wait it out.
func transient(err error) bool {
	switch statusOf(err) {
	case 429, 502, 503, 504:
		return true
	case 0:
		var netErr net.Error
		return errors.As(err, &netErr) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "connection reset")
	}
	return false
}

func (p *pass) do(ctx context.Context, method, rawURL string, body any, out any) (http.Header, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	token, err := p.token.get()
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("X-Buckyd-Authorization", token)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return resp.Header, &httpError{resp.StatusCode, strings.TrimSpace(string(data))}
	}
	if out == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		return resp.Header, err
	}
	return resp.Header, json.NewDecoder(resp.Body).Decode(out)
}

// retry calls fn until it succeeds, a permanent error remains after attempts
// tries, or a transient error outlasts WaitOut. It sleeps with backoff.
func (p *pass) retry(ctx context.Context, attempts int, fn func() error) error {
	delay := p.o.Backoff
	deadline := time.Now().Add(p.o.WaitOut)
	tries := 0
	for {
		err := fn()
		if err == nil || ctx.Err() != nil {
			return err
		}
		if transient(err) {
			if time.Now().After(deadline) {
				return err
			}
		} else if status := statusOf(err); status == 400 || status == 401 || status == 403 || status == 404 || status == 413 {
			return err
		} else if tries++; tries >= attempts {
			return err
		}
		atomic.AddInt64(&p.report.Retried, 1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 30*time.Second)
	}
}

// split runs fn over names, halving a failing batch until single names fail
// on their own. It returns the failing names with their reasons.
func (p *pass) split(ctx context.Context, names []string, fn func([]string) error) map[string]string {
	failed := map[string]string{}
	var walk func([]string)
	walk = func(part []string) {
		if len(part) == 0 || ctx.Err() != nil {
			return
		}
		attempts := 2
		if len(part) == 1 {
			attempts = p.o.Retries
		}
		err := p.retry(ctx, attempts, func() error { return fn(part) })
		if err == nil || ctx.Err() != nil {
			return
		}
		if status := statusOf(err); len(part) == 1 || status == 400 || status == 401 || status == 403 {
			// Bad requests and auth failures are not per name: fail the part.
			for _, name := range part {
				failed[name] = err.Error()
			}
			return
		}
		p.logf("merge_split names=%d err=%q", len(part), err)
		mid := len(part) / 2
		walk(part[:mid])
		walk(part[mid:])
	}
	walk(names)
	return failed
}

// pull asks the target to merge the names from source (target points win).
// A single name the source no longer holds (404) counts as absent.
func (p *pass) pull(ctx context.Context, source string, names []string) map[string]string {
	if len(names) == 0 {
		return nil
	}
	failed := p.split(ctx, names, func(part []string) error {
		header, err := p.do(ctx, http.MethodPost, p.o.To+"/v1/transfer/pull", map[string]any{"Source": source, "Names": part, "MaxBytes": p.o.MaxBytes}, nil)
		if err == nil {
			n, _ := strconv.ParseInt(header.Get("X-Transfer-Bytes"), 10, 64)
			atomic.AddInt64(&p.report.PullBytes, n)
			atomic.AddInt64(&p.report.Pulled, int64(len(part)))
			return nil
		}
		if len(part) == 1 && statusOf(err) == 404 {
			atomic.AddInt64(&p.report.Absent, 1)
			return nil
		}
		return err
	})
	return failed
}

type digestArchive struct {
	Step, Points, Count int
	Hash                string
	From, Until         int
}

type digestResult struct {
	Name     string
	Found    bool
	Archives []digestArchive
}

func (p *pass) window() (int64, int64) {
	if p.o.Until <= 0 {
		return 0, 0
	}
	return 1, p.o.Until
}

func (p *pass) digest(ctx context.Context, store string, names []string) (map[string]digestResult, error) {
	from, until := p.window()
	var results []digestResult
	_, err := p.do(ctx, http.MethodPost, store+"/v1/transfer/digest", map[string]any{"Names": names, "Mode": "sum-v1", "From": from, "Until": until}, &results)
	if err != nil {
		return nil, err
	}
	if len(results) != len(names) {
		return nil, fmt.Errorf("digest returned %d of %d names", len(results), len(names))
	}
	out := make(map[string]digestResult, len(results))
	for i, r := range results {
		if r.Name != names[i] {
			return nil, errors.New("digest reply out of order")
		}
		out[r.Name] = r
	}
	return out, nil
}

// compare classifies one name: "absent" (source lacks it or has no finite
// points), "equal" (every non-empty source archive equals the target's),
// "missing" (target lacks the name) or "differ".
func compare(source, target digestResult) string {
	if !source.Found {
		return "absent"
	}
	type key struct{ step, from, until int }
	have := make(map[key]digestArchive, len(target.Archives))
	for _, a := range target.Archives {
		have[key{a.Step, a.From, a.Until}] = a
	}
	empty := true
	for _, a := range source.Archives {
		if a.Count == 0 {
			continue
		}
		empty = false
		if !target.Found {
			return "missing"
		}
		if t, ok := have[key{a.Step, a.From, a.Until}]; !ok || t.Count != a.Count || t.Hash != a.Hash {
			return "differ"
		}
	}
	if empty {
		return "absent"
	}
	return "equal"
}

// differing digests both sides and returns the names the target must pull.
func (p *pass) differing(ctx context.Context, source string, names []string) ([]string, map[string]string) {
	var mu sync.Mutex
	var need []string
	failed := p.split(ctx, names, func(part []string) error {
		var src, dst map[string]digestResult
		var srcErr, dstErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); dst, dstErr = p.digest(ctx, p.o.To, part) }()
		src, srcErr = p.digest(ctx, source, part)
		wg.Wait()
		if statusOf(srcErr) == 400 && strings.Contains(srcErr.Error(), "Mode") {
			// An older source cannot serve sum-v1: merge every name it holds.
			if _, loaded := p.noSum.LoadOrStore(source, true); !loaded {
				p.logf("merge_digest_unavailable source=%s fallback=pull err=%q", source, srcErr)
			}
			srcErr, dstErr = nil, nil
			src = map[string]digestResult{}
			for _, name := range part {
				src[name] = digestResult{Name: name, Found: true, Archives: []digestArchive{{Count: 1}}}
			}
		}
		if err := errors.Join(srcErr, dstErr); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for _, name := range part {
			switch compare(src[name], dst[name]) {
			case "absent":
				atomic.AddInt64(&p.report.Absent, 1)
			case "equal":
				atomic.AddInt64(&p.report.Identical, 1)
			default:
				need = append(need, name)
			}
		}
		return nil
	})
	return need, failed
}

// parity compares an evenly spaced sample against every source with the same
// sum-v1 digest window as the gate.
func (p *pass) parity(ctx context.Context, entries []entry) {
	from, until := p.window()
	p.report.Window = map[string]int64{"from": from, "until": until}
	p.report.Parity = map[string]*ParityCounts{}
	p.report.ParityDetail = map[string][]ParityMismatch{}
	n := min(p.o.Sample, len(entries))
	p.report.ParitySample = n
	if n == 0 {
		return
	}
	sample := make([]entry, 0, n)
	for i := range n {
		sample = append(sample, entries[i*len(entries)/n])
	}
	for _, source := range p.o.From {
		counts := &ParityCounts{}
		p.report.Parity[source] = counts
		for start := 0; start < len(sample); start += 500 {
			part := sample[start:min(start+500, len(sample))]
			names := make([]string, len(part))
			for j, e := range part {
				names[j] = e.name
			}
			var src, dst map[string]digestResult
			err := p.retry(ctx, 2, func() error {
				var err error
				if src, err = p.digest(ctx, source, names); err != nil {
					return err
				}
				dst, err = p.digest(ctx, p.o.To, names)
				return err
			})
			if statusOf(err) == 400 && strings.Contains(err.Error(), "Mode") {
				counts.Unsupported += len(names) // an older source cannot serve sum-v1
				continue
			}
			if err != nil {
				counts.Errors += len(names)
				continue
			}
			for _, e := range part {
				result := compare(src[e.name], dst[e.name])
				switch result {
				case "equal":
					counts.Equal++
				case "absent":
					counts.SourceAbsent++
				case "missing":
					counts.MissingTarget++
				default:
					counts.Differ++
				}
				if result == "missing" || result == "differ" {
					if d := p.report.ParityDetail[source]; len(d) < 100 {
						p.report.ParityDetail[source] = append(d, ParityMismatch{e.name, result})
					}
				}
			}
		}
	}
}

// snapshot loads State/names or lists every source once and writes it. Each
// line is "name<TAB>mask", mask bit i meaning From[i] listed the name.
func (p *pass) snapshot(ctx context.Context) ([]entry, error) {
	path := filepath.Join(p.o.State, "names")
	if data, err := os.ReadFile(path); err == nil {
		return parseSnapshot(data, len(p.o.From))
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	masks := map[string]uint64{}
	for i, source := range p.o.From {
		after := p.o.After
		listed := 0
		for {
			var page []string
			q := url.Values{"limit": {"10000"}, "after": {after}}
			err := p.retry(ctx, p.o.Retries, func() error {
				_, err := p.do(ctx, http.MethodGet, source+"/v1/transfer/list?"+q.Encode(), nil, &page)
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("list %s: %w", source, err)
			}
			for _, name := range page {
				masks[name] |= 1 << i
			}
			listed += len(page)
			if len(page) < 10000 || (p.o.Limit > 0 && listed >= p.o.Limit) {
				break
			}
			after = page[len(page)-1]
		}
		p.logf("merge_listed source=%s names=%d", source, listed)
	}
	names := make([]string, 0, len(masks))
	for name := range masks {
		names = append(names, name)
	}
	slices.Sort(names)
	if p.o.Limit > 0 && len(names) > p.o.Limit {
		names = names[:p.o.Limit]
	}
	var buf bytes.Buffer
	entries := make([]entry, len(names))
	for i, name := range names {
		entries[i] = entry{name, masks[name]}
		fmt.Fprintf(&buf, "%s\t%x\n", name, masks[name])
	}
	if err := writeAtomic(path, buf.Bytes()); err != nil {
		return nil, err
	}
	return entries, nil
}

func parseSnapshot(data []byte, sources int) ([]entry, error) {
	var entries []entry
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		name, hex, ok := strings.Cut(line, "\t")
		mask, err := strconv.ParseUint(hex, 16, 64)
		if !ok || err != nil || mask>>sources != 0 {
			return nil, fmt.Errorf("names snapshot: bad line %q (sources changed?)", line)
		}
		entries = append(entries, entry{name, mask})
	}
	return entries, nil
}
