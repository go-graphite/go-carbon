package downsize

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

// Plan-free copy: discover names (source catalog or --names), optionally keep
// only names the target owns under --ring, and copy them through the existing
// metric importer (/v1/transfer/pull + verify) in batches cut by real exported
// bytes. Resume is an append-only journal of completed name ranges. Target
// points win in the importer, so redoing a batch is harmless.

type copyOptions struct {
	froms                                            []string
	to, names, prefix, ring, state, engine, tokenEnv string
	streams, maxNames, maxFailures                   int
	maxBytes                                         int64
	tick, transferIdle, maxBackoff, stallTimeout     time.Duration
	progressInterval                                 time.Duration
	transportRetries                                 int
	debtGuard                                        bool
}

const (
	copyInitialBytesPerName = 16 << 10
	copyFill                = 0.75
	copyListPage            = 10000
	// A pull answers only after the target exported and imported the whole
	// batch. Busy targets import about 2-3 s per MiB, so a 64 MiB batch ran
	// 2-3 min and every pull past the header deadline was discarded and redone
	// at full size. 16 MiB batches finish in under 80 s there and cost nothing
	// on fast copies, whose batches stay below 16 MiB anyway.
	copyDefaultBatchBytes = 16 << 20
	copyHeaderTimeoutMin  = 90 * time.Second
	copySettleAttempts    = 4
)

// copyHeaderTimeout keeps the response header deadline behind the transfer
// idle bound. Pull and verify send no bytes until the target finishes the
// batch, so a header deadline shorter than the idle bound turned slow batches
// into transport errors that retried at full size (and aborted the run after
// --transport-retries) instead of idle timeouts that split the batch.
func copyHeaderTimeout(idle time.Duration) time.Duration {
	if idle <= 0 {
		return copyHeaderTimeoutMin
	}
	return max(copyHeaderTimeoutMin, idle+30*time.Second)
}

// originList is a repeatable, comma-separable --from.
type originList []string

func (l *originList) String() string { return strings.Join(*l, ",") }

func (l *originList) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*l = append(*l, part)
		}
	}
	return nil
}

func PlanFreeCopyMain(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("store-copy copy", flag.ContinueOnError)
	var o copyOptions
	flags.Var((*originList)(&o.froms), "from", "source buckyd origin, e.g. http://donor:4242; repeat (or comma-separate) to copy every donor of a block into --to in one run")
	flags.StringVar(&o.to, "to", "", "target buckyd origin; the target pulls from --from")
	flags.StringVar(&o.names, "names", "", "byte-sorted unique name list (LC_ALL=C sort -u); default discovers the source catalog")
	flags.StringVar(&o.prefix, "prefix", "", "discover only source names with this prefix")
	flags.StringVar(&o.ring, "ring", "", "optional ring file (resize spec: its new ring; or a bare topology); names the target does not own are skipped")
	flags.StringVar(&o.state, "state", "", "append-only journal of completed name ranges; rerun the same command to resume")
	flags.StringVar(&o.engine, "engine", "metric", "metric (importer). bulk is not wired to plan-free copy yet")
	flags.StringVar(&o.tokenEnv, "token-env", "BUCKYD_TOKEN", "environment variable holding the buckyd JWT")
	flags.BoolVar(&o.debtGuard, "debt-guard", false, "let the target pause imports on a configured transfer-max-compaction-debt (429); off by default, which keeps only its hard limits (write stalls, L0 stop margin)")
	flags.IntVar(&o.streams, "streams", 0, "parallel transfers; 0 uses the smaller of the source and target max_transfers (each stream holds one transfer slot on both)")
	flags.Int64Var(&o.maxBytes, "max-batch-bytes", copyDefaultBatchBytes, "batch byte cap (real exported bytes); capped by the target max_body_bytes. A single name may use the whole max_body_bytes. Larger batches take longer per pull on a busy target")
	flags.IntVar(&o.maxNames, "max-batch-names", 8192, "names per batch cap (at most 10000)")
	flags.IntVar(&o.maxFailures, "max-failures", 0, "0 (default): a name that keeps failing is skipped, retried once at the end and reported (exit non-zero), never stopping the run; N>0 aborts above N skipped names")
	flags.DurationVar(&o.tick, "retry-interval", 2*time.Second, "first backoff after a transient guard response (429/502/503/504) or transport error")
	flags.DurationVar(&o.maxBackoff, "max-backoff", 30*time.Second, "backoff ceiling for transient retries")
	flags.IntVar(&o.transportRetries, "transport-retries", 0, "deprecated and ignored: transport errors, refused connections and 502/503/504 retry until --stall-timeout")
	flags.DurationVar(&o.progressInterval, "progress-interval", 30*time.Second, "print one progress line (done/total, names/s, MB/s, ETA, limiter, retries, waits, skipped) per interval")
	flags.DurationVar(&o.stallTimeout, "stall-timeout", 30*time.Minute, "abort with an error once a source or target has been unreachable (or answered only transient errors) this long without progress")
	flags.DurationVar(&o.transferIdle, "transfer-idle-timeout", defaultTransferIdle, "abort a pull/verify after this long without bytes; a multi-name batch splits, one name gets 3 attempts")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return planFreeCopy(ctx, o)
}

// defaultCopyStreams fills every transfer slot both ends advertise: each
// stream holds one slot on the source (export) and one on the target
// (import), so the smaller cap is the most that can run at once. Guessing
// lower left copies throttled on idle hosts; guessing higher only queues
// on 429s. Builds that advertise no cap get 64.
func defaultCopyStreams(source, target int) int {
	switch {
	case source > 0 && target > 0:
		return min(source, target)
	case source > 0:
		return source
	case target > 0:
		return target
	}
	return 64
}

type copyStatus struct {
	MaxTransfers int
	MaxBodyBytes int64
}

type copyStats struct {
	discovered, resumed, notOwned, copied, failed, batches atomic.Int64
	bytes                                                  atomic.Int64
}

type planFreeCopier struct {
	*hostWaiter
	o         copyOptions
	progress  *copyProgress
	skipMu    sync.Mutex
	skipped   []copySkip
	journal   *copyJournal
	sizer     *batchSizer
	serverMax int64
	stats     copyStats
	started   time.Time
	multi     bool
}

func planFreeCopy(ctx context.Context, o copyOptions) error {
	var err error
	if o.engine != "metric" {
		return errors.New("--engine bulk is not wired to plan-free copy yet; use --engine metric (or the legacy plan/run for native ranges)")
	}
	if o.to, err = origin(o.to); err != nil {
		return fmt.Errorf("--to: %w", err)
	}
	if len(o.froms) == 0 {
		return errors.New("--from required")
	}
	seen := map[string]bool{}
	var froms []string
	for _, raw := range o.froms {
		from, err := origin(raw)
		if err != nil {
			return fmt.Errorf("--from %s: %w", raw, err)
		}
		if from == o.to {
			return fmt.Errorf("--from %s and --to are the same origin", from)
		}
		if !seen[from] {
			seen[from] = true
			froms = append(froms, from)
		}
	}
	o.froms = froms
	if o.names != "" && len(o.froms) > 1 {
		return errors.New("--names takes a single --from; with several donors use catalog discovery (optionally --prefix) and --ring")
	}
	if o.state == "" {
		return errors.New("--state journal required (rerun the same command with it to resume)")
	}
	if o.names != "" && o.prefix != "" {
		return errors.New("--names and --prefix are exclusive")
	}
	if o.maxNames < 1 || o.maxNames > chunkstore.MaxTransferMetrics {
		return fmt.Errorf("--max-batch-names must be 1..%d", chunkstore.MaxTransferMetrics)
	}
	if o.maxBytes < 65536 {
		return errors.New("--max-batch-bytes must be at least 65536 (source export minimum)")
	}
	if o.streams < 0 || o.maxFailures < 0 || o.transportRetries < 0 || o.tick <= 0 || o.maxBackoff < o.tick || o.stallTimeout <= 0 {
		return errors.New("invalid streams/max-failures/transport-retries/retry-interval/max-backoff/stall-timeout")
	}
	if o.transportRetries > 0 {
		event("deprecated_flag", map[string]any{"flag": "--transport-retries", "value": o.transportRetries, "note": "ignored; transient errors retry until --stall-timeout"})
	}
	var owner func(string) bool
	if o.ring != "" {
		if owner, err = ringplan.TargetOwner(o.ring, o.to); err != nil {
			return err
		}
	}
	identity, err := copyIdentity(o)
	if err != nil {
		return err
	}
	m := &mover{options: options{engine: "metric", tokenEnv: o.tokenEnv, tick: o.tick, transferIdle: o.transferIdle, debtGuardOff: !o.debtGuard},
		client: &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{MaxIdleConnsPerHost: 128, MaxIdleConns: 256, ResponseHeaderTimeout: copyHeaderTimeout(o.transferIdle)}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	progress := newCopyProgress()
	c := &planFreeCopier{hostWaiter: &hostWaiter{m: m, tick: o.tick, maxBackoff: o.maxBackoff, stallTimeout: o.stallTimeout, progress: progress}, progress: progress, o: o, started: time.Now(), multi: len(o.froms) > 1}
	var target copyStatus
	sourceTransfers := 0
	for _, from := range o.froms {
		var source copyStatus
		if err := c.status(ctx, from, &source); err != nil {
			return fmt.Errorf("source %s status: %w", from, err)
		}
		if source.MaxTransfers > 0 {
			sourceTransfers += source.MaxTransfers
		}
	}
	if err := c.status(ctx, o.to, &target); err != nil {
		return fmt.Errorf("target status: %w", err)
	}
	if target.MaxBodyBytes < 65536 {
		return fmt.Errorf("target max_body_bytes %d is below the export minimum", target.MaxBodyBytes)
	}
	if o.streams == 0 {
		o.streams = defaultCopyStreams(sourceTransfers, target.MaxTransfers)
	}
	journal, err := openCopyJournal(o.state, identity)
	if err != nil {
		return err
	}
	defer journal.close()
	c.o, c.journal, c.serverMax = o, journal, target.MaxBodyBytes
	c.sizer = &batchSizer{budget: min(o.maxBytes, target.MaxBodyBytes), maxNames: o.maxNames, perName: copyInitialBytesPerName}
	event("copy_start", map[string]any{"from": o.froms, "to": o.to, "names": o.names, "prefix": o.prefix, "ring": o.ring, "state": o.state, "streams": o.streams, "batch_bytes": c.sizer.budget, "max_body_bytes": target.MaxBodyBytes, "max_batch_names": o.maxNames, "resumed_ranges": len(journal.ranges), "source_max_transfers": sourceTransfers, "target_max_transfers": target.MaxTransfers, "stall_timeout": o.stallTimeout.String()})
	sources := make([]copySource, 0, len(o.froms))
	for _, from := range o.froms {
		next, closeNames, err := c.nameSource(ctx, from)
		if err != nil {
			return err
		}
		defer closeNames()
		sources = append(sources, copySource{origin: from, next: next})
	}
	ctx, stall := context.WithCancelCause(ctx)
	defer stall(nil)
	var background sync.WaitGroup
	reportCtx, stopReport := context.WithCancel(ctx)
	background.Add(2)
	go func() { defer background.Done(); c.report(reportCtx, stall) }()
	go func() { defer background.Done(); c.countTotal(reportCtx, owner) }()
	err = c.run(ctx, sources, owner)
	if err == nil {
		err = c.retrySkipped(ctx)
	}
	stopReport()
	background.Wait()
	if cause := context.Cause(ctx); cause != nil && errors.Is(cause, errCopyStall) {
		err = cause
	}
	skippedFile, werr := c.writeSkipped()
	if err == nil {
		err = werr
	}
	c.summary(err, skippedFile)
	if err != nil {
		return err
	}
	if n := c.skippedCount(); n > 0 {
		return &copySkippedError{count: n, path: skippedFile}
	}
	return nil
}

// copySkippedError means the engine completed its final per-name retry.
// Callers can report the remaining names without restarting the whole engine.
type copySkippedError struct {
	count int
	path  string
}

func (e *copySkippedError) Error() string {
	return fmt.Sprintf("%d names still skipped after a final retry (listed in %s); rerun the same command to retry them", e.count, e.path)
}

func copyIdentity(o copyOptions) (string, error) {
	digest := func(path string) (string, error) {
		if path == "" {
			return "", nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:]), nil
	}
	names, err := digest(o.names)
	if err != nil {
		return "", err
	}
	ring, err := digest(o.ring)
	if err != nil {
		return "", err
	}
	// One donor keeps the original identity, so existing journals resume.
	from := o.froms[0]
	if len(o.froms) > 1 {
		sorted := append([]string(nil), o.froms...)
		sort.Strings(sorted)
		from = strings.Join(sorted, ",")
	}
	blob, err := json.Marshal(map[string]string{"from": from, "to": o.to, "prefix": o.prefix, "names_sha256": names, "ring_sha256": ring})
	return string(blob), err
}

// nameSource streams names in byte order: the --names file, or the source
// catalog through /v1/transfer/list pages (the planner's inventory endpoint).
func (c *planFreeCopier) nameSource(ctx context.Context, from string) (func() (string, error), func(), error) {
	if c.o.names != "" {
		f, err := os.Open(c.o.names)
		if err != nil {
			return nil, nil, err
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 256<<10), 1<<20)
		return func() (string, error) {
			for scanner.Scan() {
				if line := strings.TrimSpace(scanner.Text()); line != "" {
					return line, nil
				}
			}
			if err := scanner.Err(); err != nil {
				return "", err
			}
			return "", io.EOF
		}, func() { _ = f.Close() }, nil
	}
	var page []string
	var after string
	done := false
	return func() (string, error) {
		for len(page) == 0 {
			if done {
				return "", io.EOF
			}
			query := url.Values{"after": {after}, "limit": {strconv.Itoa(copyListPage)}}
			if c.o.prefix != "" {
				query.Set("prefix", c.o.prefix)
			}
			var next []string
			if err := c.retryTransient(ctx, from+"/v1/transfer/list", []string{from}, func() error {
				next = nil
				_, err := c.m.request(ctx, from, "/v1/transfer/list?"+query.Encode(), nil, &next)
				return err
			}); err != nil {
				return "", fmt.Errorf("source catalog: %w", err)
			}
			if len(next) > copyListPage {
				return "", errors.New("oversize source catalog page")
			}
			done = len(next) == 0
			page = next
			if len(next) > 0 {
				after = next[len(next)-1]
			}
		}
		name := page[0]
		page = page[1:]
		return name, nil
	}, func() {}, nil
}

type copySource struct {
	origin string
	next   func() (string, error)
}

type copyBatch struct {
	source string
	names  []string
}

// run feeds one shared worker pool from every donor's name stream at once.
func (c *planFreeCopier) run(ctx context.Context, sources []copySource, owner func(string) bool) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	batches := make(chan copyBatch, c.o.streams)
	var wg sync.WaitGroup
	for range c.o.streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batches {
				c.progress.inFlight.Add(1)
				err := c.send(ctx, batch.source, batch.names, false)
				c.progress.inFlight.Add(-1)
				if err != nil {
					cancel(err)
				}
			}
		}()
	}
	var producers sync.WaitGroup
	produceErrs := make([]error, len(sources))
	for i, source := range sources {
		producers.Add(1)
		go func() {
			defer producers.Done()
			if err := c.produce(ctx, source, owner, batches); err != nil {
				produceErrs[i] = err
				if !errors.Is(err, context.Canceled) {
					cancel(err)
				}
			}
		}()
	}
	producers.Wait()
	close(batches)
	wg.Wait()
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	if err := errors.Join(produceErrs...); err != nil {
		return err
	}
	return ctx.Err()
}

func (c *planFreeCopier) produce(ctx context.Context, source copySource, owner func(string) bool, out chan<- copyBatch) error {
	cover := c.journal.cover(source.origin)
	var batch []string
	var rejected []string
	flushRejected := func() error {
		if len(rejected) == 0 {
			return nil
		}
		err := c.rejectNames(source.origin, rejected, 0, chunkstore.ErrNameTooLong)
		rejected = nil
		return err
	}
	previous, first := "", true
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		select {
		case out <- copyBatch{source: source.origin, names: batch}:
			batch = nil
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		name, err := source.next()
		if errors.Is(err, io.EOF) {
			if err := flushRejected(); err != nil {
				return err
			}
			return flush()
		}
		if err != nil {
			return err
		}
		if !first && name <= previous {
			return fmt.Errorf("%s: names are not byte-sorted unique at %q after %q (use LC_ALL=C sort -u)", source.origin, name, previous)
		}
		previous, first = name, false
		c.stats.discovered.Add(1)
		if cover(name) {
			c.stats.resumed.Add(1)
			continue
		}
		if owner != nil && !owner(name) {
			c.stats.notOwned.Add(1)
			continue
		}
		if err := chunkstore.ValidateMetricName(name); err != nil {
			// Catalogs can retain names written before the intake limit. Keep
			// failures explicit without poisoning every neighboring batch.
			if err := flush(); err != nil {
				return err
			}
			rejected = append(rejected, name)
			if len(rejected) >= 1000 {
				if err := flushRejected(); err != nil {
					return err
				}
			}
			continue
		}
		if err := flushRejected(); err != nil {
			return err
		}
		batch = append(batch, name)
		if len(batch) >= c.sizer.names() {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// send copies one contiguous batch. Too large (413), idle, absent (404) and
// other definitive rejections split a multi-name batch in half; a single name
// that still fails is journaled as a failure and skipped, so nothing stalls.
func (c *planFreeCopier) send(ctx context.Context, source string, names []string, settled bool) error {
	if len(names) == 1 {
		if err := chunkstore.ValidateMetricName(names[0]); err != nil {
			return c.rejectName(source, names[0], 0, err)
		}
	}
	budget := c.sizer.budget
	if len(names) == 1 {
		budget = c.serverMax
	}
	t := task{Stream: "copy", Source: source, Target: c.o.to, Names: names}
	hosts := []string{source, c.o.to}
	idleAttempts, settleAttempts := 0, 0
	settleDelay := c.o.tick
	for {
		start := time.Now()
		var proofs []chunkstore.TransferProof
		var headers http.Header
		err := c.retryTransient(ctx, "/v1/transfer/pull", hosts, func() error {
			proofs = nil
			var err error
			headers, err = c.m.pull(ctx, t, names, budget, &proofs)
			return err
		})
		if err == nil {
			err = c.checkProofs(names, proofs)
		}
		if err == nil {
			err = c.retryTransient(ctx, "/v1/transfer/verify", hosts, func() error { return c.m.verify(ctx, t, proofs) })
		}
		if err == nil {
			bytes, _ := strconv.ParseInt(headers.Get("X-Transfer-Bytes"), 10, 64)
			c.sizer.observe(len(names), bytes)
			if err := c.journal.done(c.rowSource(source), names[0], names[len(names)-1], len(names), bytes, time.Since(start)); err != nil {
				return err
			}
			c.stats.copied.Add(int64(len(names)))
			c.progress.success()
			c.stats.bytes.Add(bytes)
			batches := c.stats.batches.Add(1)
			event("batch", map[string]any{"source": source, "first": names[0], "last": names[len(names)-1], "names": len(names), "bytes": bytes, "seconds": time.Since(start).Seconds(), "batches": batches, "next_batch_names": c.sizer.names()})
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errCopyStall) {
			return err
		}
		var api *apiError
		var idle *transferIdleError
		retryable := errors.As(err, &api) || errors.As(err, &idle) || errors.Is(err, errCopyProof)
		if !retryable {
			return err
		}
		// A source or target restart surfaces as a definitive-looking error
		// (the target answers 500 when its source export is refused, a verify
		// after a target restart may miss the import). It says nothing about
		// these names: wait until both hosts answer, then re-pull the whole
		// batch. Existing target points win, so the redo is safe.
		waited, werr := c.awaitHosts(ctx, hosts, err)
		if werr != nil {
			return werr
		}
		if waited {
			event("batch_repull", map[string]any{"first": names[0], "last": names[len(names)-1], "names": len(names), "error": err.Error()})
			continue
		}
		// A donor in its restart window can answer export with 409/404/500
		// while its status endpoint already answers. Before an original
		// batch is split or a name journaled as failed, re-pull it whole a
		// few times with backoff (about 30s with the defaults). Split
		// children skip this, so one bad name costs one settle round.
		if !settled && idle == nil && (api == nil || api.Status != http.StatusRequestEntityTooLarge) && settleAttempts < copySettleAttempts {
			settleAttempts++
			event("batch_settle_retry", map[string]any{"source": source, "first": names[0], "last": names[len(names)-1], "names": len(names), "attempt": settleAttempts, "delay_seconds": settleDelay.Seconds(), "error": err.Error()})
			if err := sleepContext(ctx, settleDelay); err != nil {
				return err
			}
			settleDelay = min(2*settleDelay, c.o.maxBackoff)
			continue
		}
		if len(names) > 1 {
			if api != nil && api.Status == http.StatusRequestEntityTooLarge {
				c.sizer.tooLarge(len(names))
			}
			event("split", map[string]any{"first": names[0], "last": names[len(names)-1], "names": len(names), "error": err.Error()})
			middle := len(names) / 2
			if err := c.send(ctx, source, names[:middle], true); err != nil {
				return err
			}
			return c.send(ctx, source, names[middle:], true)
		}
		if idle != nil {
			idleAttempts++
			if idleAttempts < transferIdleAttempts {
				idleEvent(t, "/v1/transfer/pull", "retry", idleAttempts, err)
				continue
			}
		}
		status := 0
		if api != nil {
			status = api.Status
		}
		return c.rejectName(source, names[0], status, err)
	}
}

func (c *planFreeCopier) rejectName(source, name string, status int, reason error) error {
	return c.rejectNames(source, []string{name}, status, reason)
}

func (c *planFreeCopier) rejectNames(source string, names []string, status int, reason error) error {
	rows := make([]copyRow, len(names))
	for i, name := range names {
		rows[i] = copyRow{Kind: "failed", Source: c.rowSource(source), Name: name, Status: status, Error: reason.Error()}
	}
	if err := c.journal.appendRows(rows); err != nil {
		return err
	}
	for _, name := range names {
		c.stats.failed.Add(1)
		failed := c.skip(copySkip{Source: source, Name: name, Status: status, Reason: reason.Error()})
		if c.o.maxFailures > 0 && failed > c.o.maxFailures {
			return fmt.Errorf("%d names skipped, above --max-failures %d (last %s: %v)", failed, c.o.maxFailures, name, reason)
		}
	}
	event("names_skipped", map[string]any{"level": "warn", "source": source, "names": len(names), "status": status, "error": reason.Error(), "skipped": c.skippedCount(), "journal": c.o.state})
	return nil
}

var errCopyProof = errors.New("import proof mismatch")

func (c *planFreeCopier) checkProofs(names []string, proofs []chunkstore.TransferProof) error {
	if len(proofs) != len(names) {
		return fmt.Errorf("%w: %d proofs for %d names", errCopyProof, len(proofs), len(names))
	}
	for i, p := range proofs {
		if p.Source.Name != names[i] || p.Target.Name != names[i] || p.Digest == "" {
			return fmt.Errorf("%w at %q", errCopyProof, names[i])
		}
	}
	return nil
}

// rowSource tags journal rows with their donor when one run covers several.
func (c *planFreeCopier) rowSource(source string) string {
	if c.multi {
		return source
	}
	return ""
}

func (c *planFreeCopier) summary(err error, skippedFile string) {
	seconds := time.Since(c.started).Seconds()
	skipped := c.skippedCount()
	fields := map[string]any{"discovered": c.stats.discovered.Load(), "resumed_skipped": c.stats.resumed.Load(), "not_owned": c.stats.notOwned.Load(), "copied": c.stats.copied.Load(), "skip_events": c.stats.failed.Load(), "skipped": skipped, "batches": c.stats.batches.Load(), "bytes": c.stats.bytes.Load(), "seconds": seconds, "retries": c.progress.retries.Load(), "target_429s": c.m.backpressure.Load()}
	if skippedFile != "" {
		fields["skipped_file"] = skippedFile
		c.skipMu.Lock()
		var sample []string
		for _, s := range c.skipped[:min(20, len(c.skipped))] {
			sample = append(sample, s.Name)
		}
		c.skipMu.Unlock()
		fields["skipped_sample"] = sample
	}
	fields["line"] = fmt.Sprintf("done: %d copied, %d skipped, %d already done, %d not owned, %.1f MB in %s", c.stats.copied.Load(), skipped, c.stats.resumed.Load(), c.stats.notOwned.Load(), float64(c.stats.bytes.Load())/1e6, time.Since(c.started).Round(time.Second))
	if seconds > 0 {
		fields["names_per_second"] = float64(c.stats.copied.Load()) / seconds
		fields["mb_per_second"] = float64(c.stats.bytes.Load()) / seconds / 1e6
	}
	if b := c.stats.batches.Load(); b > 0 {
		fields["names_per_batch"] = float64(c.stats.copied.Load()) / float64(b)
	}
	if err != nil {
		fields["error"] = err.Error()
	}
	event("copy_done", fields)
}

// batchSizer turns observed exported bytes per name into a name count that
// fills copyFill of the byte budget, bounded by the name cap.
type batchSizer struct {
	mu       sync.Mutex
	perName  float64
	budget   int64
	maxNames int
}

func (s *batchSizer) names() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := int(copyFill * float64(s.budget) / max(s.perName, 1))
	return max(1, min(n, s.maxNames))
}

func (s *batchSizer) observe(names int, bytes int64) {
	if names < 1 || bytes < 1 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.perName = 0.6*s.perName + 0.4*float64(bytes)/float64(names)
}

// tooLarge: names exceeded the budget, so the mean is above budget/names; aim
// the next batch at about half of them.
func (s *batchSizer) tooLarge(names int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.perName = max(s.perName, 1.5*float64(s.budget)/float64(names))
}

// copyJournal is append-only JSON lines: one header with the copy identity,
// then done ranges (first/last name in source order) and per-name failures.
type copyJournal struct {
	mu     sync.Mutex
	file   *os.File
	ranges []copyRange
}

type copyRange struct{ Source, First, Last string }

type copyRow struct {
	Kind     string  `json:"kind"`
	Source   string  `json:"source,omitempty"`
	Identity string  `json:"identity,omitempty"`
	First    string  `json:"first,omitempty"`
	Last     string  `json:"last,omitempty"`
	Names    int     `json:"names,omitempty"`
	Bytes    int64   `json:"bytes,omitempty"`
	Seconds  float64 `json:"seconds,omitempty"`
	Name     string  `json:"name,omitempty"`
	Status   int     `json:"status,omitempty"`
	Error    string  `json:"error,omitempty"`
	Time     string  `json:"time"`
}

func openCopyJournal(path, identity string) (*copyJournal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	j := &copyJournal{file: f}
	reader := bufio.NewReaderSize(f, 1<<20)
	var good int64
	header := false
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			// A torn final line (killed mid-append) is dropped below.
			break
		}
		if err != nil {
			f.Close()
			return nil, err
		}
		var row copyRow
		if err := json.Unmarshal(line, &row); err != nil {
			f.Close()
			return nil, fmt.Errorf("journal %s: corrupt row at byte %d: %w", path, good, err)
		}
		good += int64(len(line))
		switch row.Kind {
		case "header":
			if row.Identity != identity {
				f.Close()
				return nil, fmt.Errorf("journal %s belongs to another copy (%s); use a new --state", path, row.Identity)
			}
			header = true
		case "done":
			j.ranges = append(j.ranges, copyRange{row.Source, row.First, row.Last})
		}
	}
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if !header {
		if good != 0 {
			f.Close()
			return nil, fmt.Errorf("journal %s has no header", path)
		}
		if err := j.append(copyRow{Kind: "header", Identity: identity}); err != nil {
			f.Close()
			return nil, err
		}
	}
	return j, nil
}

func (j *copyJournal) append(row copyRow) error {
	return j.appendRows([]copyRow{row})
}

// appendRows keeps local validation failures durable without one fsync per
// name. Each row still has its own newline, so interrupted writes resume.
func (j *copyJournal) appendRows(rows []copyRow) error {
	var data []byte
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	for _, row := range rows {
		row.Time = stamp
		encoded, err := json.Marshal(row)
		if err != nil {
			return err
		}
		data = append(data, encoded...)
		data = append(data, '\n')
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.file.Write(data); err != nil {
		return err
	}
	return j.file.Sync()
}

func (j *copyJournal) done(source, first, last string, names int, bytes int64, took time.Duration) error {
	return j.append(copyRow{Kind: "done", Source: source, First: first, Last: last, Names: names, Bytes: bytes, Seconds: took.Seconds()})
}

func (j *copyJournal) failed(source, name string, status int, message string) error {
	return j.append(copyRow{Kind: "failed", Source: source, Name: name, Status: status, Error: message})
}

func (j *copyJournal) close() { _ = j.file.Close() }

// cover returns a predicate for names arriving in increasing byte order: true
// when the name lies inside a completed range. Failed names are never inside
// a done range, so a rerun retries them.
func (j *copyJournal) cover(source string) func(string) bool {
	var ranges []copyRange
	for _, r := range j.ranges {
		// Single-donor journals carry no source; several-donor rows do.
		if r.Source == "" || r.Source == source {
			ranges = append(ranges, r)
		}
	}
	sort.Slice(ranges, func(a, b int) bool { return ranges[a].First < ranges[b].First })
	merged := ranges[:0]
	for _, r := range ranges {
		if n := len(merged); n > 0 && r.First <= merged[n-1].Last {
			if r.Last > merged[n-1].Last {
				merged[n-1].Last = r.Last
			}
			continue
		}
		merged = append(merged, r)
	}
	i := 0
	return func(name string) bool {
		for i < len(merged) && merged[i].Last < name {
			i++
		}
		return i < len(merged) && merged[i].First <= name
	}
}
