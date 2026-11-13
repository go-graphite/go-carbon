package downsize

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// syncLiveBackend reaches real stores: catalogs through buckyd
// /v1/transfer/list, retentions and points through carbonserver v3 info and
// multi-fetch, and copies through the plan-free copy engine (planFreeCopy),
// which owns batching, retries and its own resume journal.
type syncLiveOptions struct {
	carbonPort   int
	copyStreams  int
	tokenEnv     string
	hostWait     time.Duration
	copyAttempts int
}

type syncLiveBackend struct {
	o      syncLiveOptions
	m      *mover
	client *http.Client
	copy   func(context.Context, copyOptions) error
	bytes  atomic.Int64 // response bytes read for verification
	waits  atomic.Int64 // retries while a store was unavailable or busy
}

func (b *syncLiveBackend) syncStats() (int64, int64) { return b.bytes.Load(), b.waits.Load() }

func newSyncLiveBackend(o syncLiveOptions) *syncLiveBackend {
	m := &mover{options: options{engine: "metric", tokenEnv: o.tokenEnv, tick: 2 * time.Second, transferIdle: defaultTransferIdle},
		client: &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{MaxIdleConnsPerHost: 64, ResponseHeaderTimeout: 2 * time.Minute}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	return &syncLiveBackend{o: o, m: m, client: &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}, copy: planFreeCopy}
}

// carbon maps a buckyd origin to the same host's carbonserver.
func (b *syncLiveBackend) carbon(store string) (string, error) {
	u, err := url.Parse(store)
	if err != nil {
		return "", err
	}
	return u.Scheme + "://" + net.JoinHostPort(u.Hostname(), strconv.Itoa(b.o.carbonPort)), nil
}

type syncHTTPError struct {
	status int
	body   string
}

func (e *syncHTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.status, e.body) }

// wait retries transport errors and 429/5xx answers until the store has
// failed for --host-wait without one success: a restart or a busy store is
// waited out inside the tool.
func (b *syncLiveBackend) wait(ctx context.Context, what, store string, call func() error) error {
	delay := 2 * time.Second
	var since time.Time
	for attempt := 1; ; attempt++ {
		err := call()
		if err == nil || ctx.Err() != nil {
			return err
		}
		var httpErr *syncHTTPError
		var api *apiError
		status := 0
		if errors.As(err, &httpErr) {
			status = httpErr.status
		} else if errors.As(err, &api) {
			status = api.Status
		}
		if status != 0 && status != http.StatusTooManyRequests && status < 500 {
			return err
		}
		if since.IsZero() {
			since = time.Now()
		}
		if time.Since(since) > b.o.hostWait {
			return fmt.Errorf("%s %s: %w for %s: %w", what, store, errSyncStoreDown, b.o.hostWait, err)
		}
		b.waits.Add(1)
		event("sync_host_wait", map[string]any{"what": what, "store": store, "attempt": attempt, "delay_seconds": delay.Seconds(), "error": err.Error()})
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
		delay = min(2*delay, 30*time.Second)
	}
}

func (b *syncLiveBackend) List(ctx context.Context, store, prefix, after string, limit int) ([]string, error) {
	query := url.Values{"after": {after}, "limit": {strconv.Itoa(limit)}}
	if prefix != "" {
		query.Set("prefix", prefix)
	}
	var page []string
	err := b.wait(ctx, "list", store, func() error {
		page = nil
		_, err := b.m.request(ctx, store, "/v1/transfer/list?"+query.Encode(), nil, &page)
		return err
	})
	return page, err
}

func (b *syncLiveBackend) post(ctx context.Context, url string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-carbonapi-v3-pb")
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	b.bytes.Add(int64(len(data)))
	if resp.StatusCode != http.StatusOK {
		return nil, &syncHTTPError{resp.StatusCode, string(bytes.TrimSpace(data[:min(len(data), 300)]))}
	}
	return data, nil
}

// syncDigestWire mirrors buckyd's /v1/transfer/digest response.
type syncDigestWire struct {
	Name     string
	Found    bool
	Archives []syncDigest
}

// Digest asks buckyd for per-archive digests. 404/405 (an older build
// without the endpoint) is errSyncNoDigest so the caller reads points.
func (b *syncLiveBackend) Digest(ctx context.Context, store string, names []string, from, until int64) (map[string][]syncDigest, error) {
	request := struct {
		Names       []string
		From, Until int64
	}{names, from, until}
	var reply []syncDigestWire
	err := b.wait(ctx, "digest", store, func() error {
		reply = nil
		_, err := b.m.request(ctx, store, "/v1/transfer/digest", request, &reply)
		return err
	})
	var api *apiError
	if errors.As(err, &api) && (api.Status == http.StatusNotFound || api.Status == http.StatusMethodNotAllowed) {
		return nil, errSyncNoDigest
	}
	if err != nil {
		return nil, err
	}
	out := make(map[string][]syncDigest, len(reply))
	for _, r := range reply {
		if r.Found {
			out[r.Name] = r.Archives
		}
	}
	return out, nil
}

// Info returns retentions for the names carbonserver knows. One unknown name
// fails a whole v3 info request with 404, so a 404 batch is split in halves.
func (b *syncLiveBackend) Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	base, err := b.carbon(store)
	if err != nil {
		return nil, err
	}
	out := map[string][]syncArchive{}
	var walk func([]string) error
	walk = func(names []string) error {
		body, err := (&protov3.MultiGlobRequest{Metrics: names}).MarshalVT()
		if err != nil {
			return err
		}
		var data []byte
		err = b.wait(ctx, "info", store, func() error {
			data, err = b.post(ctx, base+"/info/?format=carbonapi_v3_pb", body)
			return err
		})
		var httpErr *syncHTTPError
		if errors.As(err, &httpErr) && httpErr.status == http.StatusNotFound {
			if len(names) == 1 {
				return nil
			}
			if err := walk(names[:len(names)/2]); err != nil {
				return err
			}
			return walk(names[len(names)/2:])
		}
		if err != nil {
			return err
		}
		var resp protov3.MultiMetricsInfoResponse
		if err := resp.UnmarshalVT(data); err != nil {
			return fmt.Errorf("decode info from %s: %w", base, err)
		}
		for _, m := range resp.Metrics {
			archives := make([]syncArchive, 0, len(m.Retentions))
			for _, r := range m.Retentions {
				archives = append(archives, syncArchive{Step: r.SecondsPerPoint, Points: r.NumberOfPoints})
			}
			out[m.Name] = archives
		}
		return nil
	}
	return out, walk(names)
}

func (b *syncLiveBackend) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	base, err := b.carbon(store)
	if err != nil {
		return nil, err
	}
	req := protov3.MultiFetchRequest{Metrics: make([]*protov3.FetchRequest, len(names))}
	for i, n := range names {
		req.Metrics[i] = &protov3.FetchRequest{Name: n, PathExpression: n, StartTime: from, StopTime: until}
	}
	body, err := req.MarshalVT()
	if err != nil {
		return nil, err
	}
	var data []byte
	err = b.wait(ctx, "fetch", store, func() error {
		data, err = b.post(ctx, base+"/render/?format=carbonapi_v3_pb", body)
		return err
	})
	var httpErr *syncHTTPError
	if errors.As(err, &httpErr) && httpErr.status == http.StatusNotFound {
		return map[string]syncSeries{}, nil
	}
	if err != nil {
		return nil, err
	}
	var resp protov3.MultiFetchResponse
	if err := resp.UnmarshalVT(data); err != nil {
		return nil, fmt.Errorf("decode render from %s: %w", base, err)
	}
	out := make(map[string]syncSeries, len(resp.Metrics))
	for _, m := range resp.Metrics {
		out[m.Name] = syncSeries{Start: m.StartTime, Step: m.StepTime, Values: m.Values}
	}
	return out, nil
}

func (b *syncLiveBackend) copyOptions(source, target, names, journal string, maxFailures int) copyOptions {
	return copyOptions{from: source, to: target, names: names, state: journal, engine: "metric", tokenEnv: b.o.tokenEnv,
		streams: b.o.copyStreams, maxBytes: 64 << 20, maxNames: 8192, maxFailures: maxFailures,
		tick: 2 * time.Second, maxBackoff: 30 * time.Second, transportRetries: 10, transferIdle: defaultTransferIdle}
}

// Copy runs the copy engine over a name list. A one-name probe first detects
// a target that refuses the source (403: not in its transfer-sources), so a
// refused source costs one request instead of a split per name. Engine
// aborts (a store restarting) are rerun from its journal up to
// --copy-attempts; names still failing are reported, not fatal.
func (b *syncLiveBackend) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	namesPath := journal[:len(journal)-len(".jsonl")] + ".names"
	probeNames := journal + ".probe.names"
	if err := os.WriteFile(probeNames, []byte(names[0]+"\n"), 0o644); err != nil {
		return syncCopyResult{}, err
	}
	probeJournal := journal + ".probe"
	_ = os.Remove(probeJournal)
	if err := b.copy(ctx, b.copyOptions(source, target, probeNames, probeJournal, 0)); err != nil {
		if ctx.Err() != nil {
			return syncCopyResult{}, ctx.Err()
		}
		var api *apiError
		if (errors.As(err, &api) && api.Status == http.StatusForbidden) || journalStatus(probeJournal, http.StatusForbidden) {
			return syncCopyResult{Forbidden: true}, nil
		}
		event("sync_copy_probe_failed", map[string]any{"level": "warn", "source": source, "error": err.Error()})
	}
	var err error
	for attempt := 1; attempt <= b.o.copyAttempts; attempt++ {
		err = b.copy(ctx, b.copyOptions(source, target, namesPath, journal, len(names)))
		if err == nil || ctx.Err() != nil {
			break
		}
		event("sync_copy_retry", map[string]any{"level": "warn", "source": source, "attempt": attempt, "error": err.Error()})
		if strings.Contains(err.Error(), "names failed") {
			if attempt >= 2 {
				break // per-name failures retried once: report them
			}
			continue
		}
		if attempt < b.o.copyAttempts {
			if err := sleepContext(ctx, time.Duration(attempt)*30*time.Second); err != nil {
				return syncCopyResult{}, err
			}
		}
	}
	if ctx.Err() != nil {
		return syncCopyResult{}, ctx.Err()
	}
	failed, err := copyJournalFailures(journal, names)
	if err != nil {
		return syncCopyResult{}, err
	}
	return syncCopyResult{Copied: len(names) - len(failed), Failed: failed}, nil
}

func journalStatus(path string, status int) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 256<<10), 4<<20)
	for scanner.Scan() {
		var row copyRow
		if json.Unmarshal(scanner.Bytes(), &row) == nil && row.Kind == "failed" && row.Status == status {
			return true
		}
	}
	return false
}

// copyJournalFailures returns the names not covered by a done range of the
// copy journal: failed names and names an aborted run never reached.
func copyJournalFailures(path string, names []string) ([]string, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return names, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	j := &copyJournal{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 256<<10), 4<<20)
	for scanner.Scan() {
		var row copyRow
		if json.Unmarshal(scanner.Bytes(), &row) == nil && row.Kind == "done" {
			j.ranges = append(j.ranges, copyRange{row.First, row.Last})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	cover := j.cover()
	var failed []string
	for _, n := range names { // names are byte-sorted (namesFile sorts)
		if !cover(n) {
			failed = append(failed, n)
		}
	}
	return failed, nil
}
