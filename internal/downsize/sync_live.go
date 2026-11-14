package downsize

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
)

// syncLiveBackend reaches real stores: catalogs through buckyd
// /v1/transfer/list, retentions and points through carbonserver v3 info and
// multi-fetch, and copies through the plan-free copy engine (planFreeCopy),
// which owns batching, retries and its own resume journal.
type syncLiveOptions struct {
	noCopy       bool
	copyTemplate *copyOptions
	carbonPort   int
	copyStreams  int
	tokenEnv     string
	hostWait     time.Duration
	copyAttempts int
	copyBytes    int64
	copyIdle     time.Duration
}

type syncLiveBackend struct {
	o             syncLiveOptions
	m             *mover
	client        *http.Client
	copy          func(context.Context, copyOptions) error
	listLimit     atomic.Int64 // learned catalog page cap after a truncated JSON response
	infoLimits    sync.Map     // store -> *atomic.Int64, learned metadata request cap
	bytes         atomic.Int64 // response bytes read for verification
	waits         atomic.Int64 // retries while a store was unavailable
	busy          atomic.Int64 // retries after 429 (all transfer slots taken)
	timings       sync.Map     // phase + store -> *syncIOTime
	windowMu      sync.RWMutex
	windowSources *nativeController // scoped to the missing-window scan
	windowPinned  time.Time
}

// syncIOTime reports cumulative request time, including retries. Concurrent
// calls overlap, so these seconds measure work, not elapsed phase time.
type syncIOTime struct {
	calls atomic.Int64
	nanos atomic.Int64
}
type syncIOTiming struct {
	Operation string  `json:"operation"`
	Store     string  `json:"store"`
	Calls     int64   `json:"calls"`
	Seconds   float64 `json:"seconds"`
}

func (b *syncLiveBackend) recordTime(operation, store string) func() {
	started := time.Now()
	v, _ := b.timings.LoadOrStore([2]string{operation, store}, &syncIOTime{})
	t := v.(*syncIOTime)
	return func() { t.nanos.Add(int64(time.Since(started))); t.calls.Add(1) }
}
func (b *syncLiveBackend) syncTimings() []syncIOTiming {
	var out []syncIOTiming
	b.timings.Range(func(key, value any) bool {
		k, v := key.([2]string), value.(*syncIOTime)
		out = append(out, syncIOTiming{k[0], k[1], v.calls.Load(), float64(v.nanos.Load()) / float64(time.Second)})
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Operation != out[j].Operation {
			return out[i].Operation < out[j].Operation
		}
		return out[i].Store < out[j].Store
	})
	return out
}

func (b *syncLiveBackend) syncStats() (int64, int64, int64) {
	return b.bytes.Load(), b.waits.Load(), b.busy.Load()
}

// syncBusyDelay is the pause before retrying a 429: a store answers 429 when
// all its transfer slots are taken, and a slot frees within one request's
// time, so the retry comes soon (25 ms doubling to 1 s) with full jitter so
// parallel streams do not retry in step.
func syncBusyDelay(busy int) time.Duration {
	ceiling := min(25*time.Millisecond<<min(busy, 6), time.Second)
	return ceiling/2 + rand.N(ceiling/2+1)
}

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
// waited out inside the tool. A 429 is retried after syncBusyDelay; other
// failures back off from 2 s to 30 s.
func (b *syncLiveBackend) wait(ctx context.Context, what, store string, call func() error) error {
	defer b.recordTime(what, store)()
	delay := 2 * time.Second
	var since time.Time
	busy := 0
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
		if status == http.StatusTooManyRequests {
			b.busy.Add(1)
			if err := sleepContext(ctx, syncBusyDelay(busy)); err != nil {
				return err
			}
			busy++
			continue
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
	if learned := b.listLimit.Load(); learned > 0 {
		limit = min(limit, int(learned))
	}
	query := url.Values{"after": {after}, "limit": {strconv.Itoa(limit)}}
	if prefix != "" {
		query.Set("prefix", prefix)
	}
	var page []string
	err := b.wait(ctx, "list", store, func() error {
		for {
			page = nil
			_, err := b.m.requestOnce(ctx, store, "/v1/transfer/list?"+query.Encode(), nil, &page)
			if !errors.Is(err, io.ErrUnexpectedEOF) || limit <= 1 {
				return err
			}
			// A count-bounded long-name page can exceed the decoder byte cap.
			// Retry the same cursor and remember the smaller page size.
			limit /= 2
			b.listLimit.Store(int64(limit))
			query.Set("limit", strconv.Itoa(limit))
			event("sync_list_page_reduced", map[string]any{"store": store, "limit": limit, "error": err.Error()})
		}
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
	Mode     string
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
		_, err := b.m.requestOnce(ctx, store, "/v1/transfer/digest", request, &reply)
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

// SumDigest requires an explicit mode echo: older endpoints can ignore Mode
// and return SHA digests with a superficially identical wire shape.
func (b *syncLiveBackend) SumDigest(ctx context.Context, store string, names []string, step, from, until int64) (map[string][]syncDigest, error) {
	request := struct {
		Names       []string
		Mode        string
		Step        int64
		From, Until int64
	}{names, "sum-v1", step, from, until}
	var reply []syncDigestWire
	err := b.wait(ctx, "digest", store, func() error {
		reply = nil
		_, err := b.m.requestOnce(ctx, store, "/v1/transfer/digest", request, &reply)
		return err
	})
	var api *apiError
	if errors.As(err, &api) && (api.Status == http.StatusNotFound || api.Status == http.StatusMethodNotAllowed || api.Status == http.StatusBadRequest) {
		return nil, errSyncNoDigest
	}
	if err != nil {
		return nil, err
	}
	if len(reply) != len(names) {
		return nil, errSyncNoDigest
	}
	out := make(map[string][]syncDigest, len(reply))
	for i, r := range reply {
		if r.Mode != "sum-v1" || r.Name != names[i] {
			return nil, errSyncNoDigest
		}
		if r.Found {
			out[r.Name] = r.Archives
		}
	}
	return out, nil
}

// Info returns retentions for the names carbonserver knows. A batch 404 does
// not identify the missing name. Recursively split failing batches,
// preserving healthy sub-batches and omitting only individual absent names.
func (b *syncLiveBackend) Info(ctx context.Context, store string, names []string) (map[string][]syncArchive, error) {
	base, err := b.carbon(store)
	if err != nil {
		return nil, err
	}
	value, _ := b.infoLimits.LoadOrStore(store, &atomic.Int64{})
	cap := value.(*atomic.Int64)
	out := map[string][]syncArchive{}
	var walk func([]string) error
	walk = func(names []string) error {
		for cursor := 0; cursor < len(names); {
			count := len(names) - cursor
			if learned := cap.Load(); learned > 0 {
				count = min(count, int(learned))
			}
			var data []byte
			err := b.wait(ctx, "info", store, func() error {
				for {
					body, err := (&protov3.MultiGlobRequest{Metrics: names[cursor : cursor+count]}).MarshalVT()
					if err != nil {
						return err
					}
					data, err = b.post(ctx, base+"/info/?format=carbonapi_v3_pb", body)
					if ctx.Err() != nil || count <= 1 || !syncInfoTooLarge(err) {
						return err
					}
					// A slow batch can outlive the server's write deadline. Retry
					// this cursor with fewer names, retaining every unprocessed name.
					count /= 2
					for old := cap.Load(); old == 0 || int64(count) < old; old = cap.Load() {
						if cap.CompareAndSwap(old, int64(count)) {
							break
						}
					}
					event("sync_info_batch_reduced", map[string]any{"store": store, "limit": count, "error": err.Error()})
				}
			})
			var httpErr *syncHTTPError
			if errors.As(err, &httpErr) && httpErr.status == http.StatusNotFound {
				if count > 1 {
					mid := cursor + count/2
					for _, half := range [][]string{names[cursor:mid], names[mid : cursor+count]} {
						if err := walk(half); err != nil {
							return err
						}
					}
				}
			} else {
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
			}
			cursor += count
		}
		return nil
	}
	return out, walk(names)
}

func syncInfoTooLarge(err error) bool {
	var timeout net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		(errors.As(err, &timeout) && timeout.Timeout())
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
	maxBytes, idle := b.o.copyBytes, b.o.copyIdle
	if maxBytes == 0 {
		maxBytes = copyDefaultBatchBytes
	}
	if idle == 0 {
		idle = defaultTransferIdle
	}
	o := copyOptions{froms: []string{source}, to: target, names: names, state: journal, engine: "metric", tokenEnv: b.o.tokenEnv,
		streams: b.o.copyStreams, maxBytes: maxBytes, maxNames: 8192, maxFailures: maxFailures,
		tick: 2 * time.Second, maxBackoff: 30 * time.Second, transferIdle: idle, stallTimeout: 30 * time.Minute}
	if b.o.copyTemplate != nil {
		template := b.o.copyTemplate
		o.maxNames, o.tick, o.maxBackoff, o.stallTimeout = template.maxNames, template.tick, template.maxBackoff, template.stallTimeout
		o.progressInterval, o.debtGuard, o.policy = template.progressInterval, template.debtGuard, template.policy
	}
	return o
}

// Copy runs the copy engine over a name list. A one-name probe first detects
// a target that refuses the source (403: not in its transfer-sources), so a
// refused source costs one request instead of a split per name. Engine
// aborts (a store restarting) are rerun from its journal up to
// --copy-attempts; names still failing are reported, not fatal.
func (b *syncLiveBackend) Copy(ctx context.Context, source, target string, names []string, journal string) (syncCopyResult, error) {
	if b.o.noCopy {
		return syncCopyResult{Failed: append([]string(nil), names...)}, nil
	}
	defer b.recordTime("copy", source)()
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
		var skipped *copySkippedError
		if errors.As(err, &skipped) {
			// The engine has already retried individual failures once. Keep
			// its completed ranges and report the residual names immediately.
			event("sync_copy_skipped", map[string]any{"level": "warn", "source": source, "names": skipped.count, "error": err.Error()})
			break
		}
		event("sync_copy_retry", map[string]any{"level": "warn", "source": source, "attempt": attempt, "error": err.Error()})

		if attempt < b.o.copyAttempts {
			if err := sleepContext(ctx, time.Duration(attempt)*30*time.Second); err != nil {
				return syncCopyResult{}, err
			}
		}
	}
	if ctx.Err() != nil {
		return syncCopyResult{}, ctx.Err()
	}
	failed, err := copyJournalFailures(journal, source, names)
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
func copyJournalFailures(path, source string, names []string) ([]string, error) {
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
			j.ranges = append(j.ranges, copyRange{Source: row.Source, First: row.First, Last: row.Last})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	cover := j.cover(source)
	var failed []string
	for _, n := range names { // names are byte-sorted (namesFile sorts)
		if !cover(n) {
			failed = append(failed, n)
		}
	}
	return failed, nil
}
