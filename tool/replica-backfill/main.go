// replica-backfill fills a go-carbon host's data gap, such as the cached
// points an unclean crash lost, from replica hosts that received the same
// points. It compares each metric with the local carbonserver at the first
// archive's step and sends only the points the local host lacks to its
// plaintext receiver, gently: batched multi-fetch, bounded concurrency, a
// request rate cap for the replicas and a point rate cap for local writes.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/go-graphite/go-carbon/helper/heartbeat"
)

type urlList []string

func (u *urlList) String() string { return strings.Join(*u, ",") }
func (u *urlList) Set(v string) error {
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimRight(strings.TrimSpace(s), "/"); s != "" {
			*u = append(*u, s)
		}
	}
	return nil
}

type options struct {
	local, ingest      string
	replicas           urlList
	from, until        string
	since, settle      time.Duration
	uncleanStart       bool
	heartbeatFile      string
	uncleanLoss        time.Duration
	uncleanMargin      time.Duration
	maxUncleanAge      time.Duration
	prefix, glob       string
	namesFile          string
	lookback           time.Duration
	batch, workers     int
	replicaRPS         float64
	localRPS           float64
	timeout            time.Duration
	retries            int
	backoff, maxBack   time.Duration
	writeRate, flushPt int
	writeRetries       int
	cachePause         float64
	cacheResume        float64
	checkpoint         string
	dryRun             bool
	printGaps          int
	progress           time.Duration
	otlpEndpoint       string
	otlpInterval       time.Duration
	carbonConfig       string
	otlpLabels         labelList
}

// labelList collects repeated -otlp-label k=v flags.
type labelList map[string]string

func (l *labelList) String() string {
	parts := make([]string, 0, len(*l))
	for k, v := range *l {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

func (l *labelList) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("label %q: want k=v", v)
	}
	if *l == nil {
		*l = labelList{}
	}
	(*l)[k] = val
	return nil
}

// otlpAttributes are the data point attributes: go-carbon's
// [prometheus.labels] (carbon_cluster, carbon_location, carbon_ring) with
// -otlp-label overrides.
func (o *options) otlpAttributes() map[string]string {
	attrs := map[string]string{}
	if o.carbonConfig != "" {
		labels, err := carbonLabels(o.carbonConfig)
		if err != nil {
			log.Printf("otlp labels: %v", err)
		}
		for k, v := range labels {
			attrs[k] = v
		}
	}
	for k, v := range o.otlpLabels {
		attrs[k] = v
	}
	return attrs
}

func parseFlags(args []string) (*options, error) {
	o := &options{}
	fs := flag.NewFlagSet("replica-backfill", flag.ContinueOnError)
	fs.StringVar(&o.local, "local", "http://127.0.0.1:8080", "local carbonserver URL")
	fs.StringVar(&o.ingest, "ingest", "127.0.0.1:2003", "local plaintext receiver address")
	fs.Var(&o.replicas, "replica", "replica carbonserver URL; repeat or comma-separate, tried in order")
	fs.StringVar(&o.from, "from", "", "gap start: unix seconds or RFC3339")
	fs.StringVar(&o.until, "until", "", "gap end: unix seconds or RFC3339 (default now minus -settle)")
	fs.DurationVar(&o.since, "since", 0, "gap start relative to now, instead of -from")
	fs.DurationVar(&o.settle, "settle", time.Minute, "default -until is this long before now, so in-flight points are not counted as missing")
	fs.BoolVar(&o.uncleanStart, "since-unclean-start", false, "take the window from the last unclean stop recorded in -heartbeat-file")
	fs.StringVar(&o.heartbeatFile, "heartbeat-file", "", "go-carbon common.heartbeat-file: unclean stop window and local cache fill")
	fs.DurationVar(&o.uncleanLoss, "unclean-loss", 16*time.Minute, "how long points may have been cached, when the heartbeat has no writeout-max-delay")
	fs.DurationVar(&o.uncleanMargin, "unclean-margin", 5*time.Minute, "widen the unclean window by this much on both sides (persister lag, clock skew)")
	fs.DurationVar(&o.maxUncleanAge, "max-unclean-age", 24*time.Hour, "refuse an unclean window whose restart is older than this")
	fs.StringVar(&o.prefix, "prefix", "", "only metrics starting with this prefix")
	fs.StringVar(&o.glob, "glob", "", "only metrics matching this graphite glob (*, ?, [..], {a,b})")
	fs.StringVar(&o.namesFile, "names-file", "", "metric names, one per line ('-' for stdin), instead of the local /metrics/list/")
	fs.DurationVar(&o.lookback, "lookback", 30*time.Minute, "skip metrics with no local point in this long before the gap or in it; 0 checks every metric")
	fs.IntVar(&o.batch, "batch", 500, "metrics per multi-fetch request")
	fs.IntVar(&o.workers, "concurrency", 4, "batches in flight (each: one local and up to one request per replica)")
	fs.Float64Var(&o.replicaRPS, "replica-rps", 10, "overall replica requests per second, across all replicas")
	fs.Float64Var(&o.localRPS, "local-rps", 0, "local carbonserver requests per second (0: bounded by -concurrency only)")
	fs.DurationVar(&o.timeout, "timeout", time.Minute, "HTTP request timeout")
	fs.IntVar(&o.retries, "retries", 5, "retries per request on errors, 5xx and 429")
	fs.DurationVar(&o.backoff, "backoff", time.Second, "first retry delay, doubled per retry")
	fs.DurationVar(&o.maxBack, "max-backoff", 30*time.Second, "retry delay cap")
	fs.IntVar(&o.writeRate, "write-rate", 100000, "points per second sent to -ingest (0: unlimited)")
	fs.IntVar(&o.flushPt, "flush-points", 5000, "points per write to -ingest")
	fs.IntVar(&o.writeRetries, "write-retries", 20, "reconnect attempts per write to -ingest before giving up (backoff as for requests)")
	fs.Float64Var(&o.cachePause, "cache-pause", 0.5, "pause writes while the local cache is above this fraction of max-size (needs -heartbeat-file; 0 disables)")
	fs.Float64Var(&o.cacheResume, "cache-resume", 0, "resume below this fraction (default 80% of -cache-pause)")
	fs.StringVar(&o.checkpoint, "checkpoint", "", "checkpoint file; a restarted run with the same window skips finished metrics")
	fs.BoolVar(&o.dryRun, "dry-run", false, "report gaps and fillable points without writing")
	fs.IntVar(&o.printGaps, "print-gaps", 0, "log details of up to this many metrics with gaps")
	fs.DurationVar(&o.progress, "progress", 10*time.Second, "progress log interval (0: off)")
	fs.StringVar(&o.otlpEndpoint, "otlp-endpoint", "", "push progress metrics as OTLP/HTTP JSON to this base URL, e.g. http://127.0.0.1:4318 (empty: off)")
	fs.DurationVar(&o.otlpInterval, "otlp-interval", 15*time.Second, "OTLP push interval; a final push follows the run")
	fs.StringVar(&o.carbonConfig, "carbon-config", "", "go-carbon config whose [prometheus.labels] become OTLP data point attributes")
	fs.Var(&o.otlpLabels, "otlp-label", "extra OTLP data point attribute k=v; repeatable, overrides -carbon-config labels")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	switch {
	case len(o.replicas) == 0:
		return nil, errors.New("at least one -replica is required")
	case o.batch <= 0 || o.workers <= 0 || o.replicaRPS <= 0:
		return nil, errors.New("-batch, -concurrency and -replica-rps must be positive")
	case o.uncleanStart && o.heartbeatFile == "":
		return nil, errors.New("-since-unclean-start needs -heartbeat-file")
	case o.uncleanStart && (o.from != "" || o.since > 0 || o.until != ""):
		return nil, errors.New("-since-unclean-start excludes -from, -since and -until")
	case !o.uncleanStart && (o.from == "") == (o.since <= 0):
		return nil, errors.New("give exactly one of -from, -since or -since-unclean-start")
	case o.cachePause < 0 || o.cachePause > 1 || o.cacheResume < 0 || o.cacheResume > 1:
		return nil, errors.New("-cache-pause and -cache-resume are fractions in [0, 1]")
	}
	return o, nil
}

func parseTime(v string) (int64, error) {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return 0, fmt.Errorf("time %q: want unix seconds or RFC3339", v)
	}
	return t.Unix(), nil
}

// window resolves the gap window as inclusive unix seconds. ok=false means
// there is nothing to do (no unclean stop recorded).
func (o *options) window(now time.Time) (from, until int64, ok bool, err error) {
	if o.uncleanStart {
		s, err := heartbeat.Read(o.heartbeatFile)
		if err != nil {
			return 0, 0, false, err
		}
		if s.Unclean == nil {
			return 0, 0, false, nil
		}
		if age := now.Sub(s.Unclean.Restarted); age > o.maxUncleanAge {
			return 0, 0, false, fmt.Errorf("unclean stop recorded %s ago (restart %s) exceeds -max-unclean-age; pass -from/-until to backfill it anyway",
				age.Round(time.Second), s.Unclean.Restarted.UTC().Format(time.RFC3339))
		}
		f, u := s.Unclean.Range(o.uncleanLoss, o.uncleanMargin, now.Add(-o.settle))
		log.Printf("unclean stop: last alive %s (pid %d), restarted %s, resumed %s",
			s.Unclean.LastAlive.UTC().Format(time.RFC3339), s.Unclean.PreviousPID,
			s.Unclean.Restarted.UTC().Format(time.RFC3339), s.Unclean.Resumed.UTC().Format(time.RFC3339))
		from, until = f.Unix(), u.Unix()
	} else {
		if o.since > 0 {
			from = now.Add(-o.since).Unix()
		} else if from, err = parseTime(o.from); err != nil {
			return 0, 0, false, err
		}
		until = now.Add(-o.settle).Unix()
		if o.until != "" {
			if until, err = parseTime(o.until); err != nil {
				return 0, 0, false, err
			}
		}
	}
	if from >= until {
		return 0, 0, false, fmt.Errorf("empty window %d..%d", from, until)
	}
	return from, until, true, nil
}

// globRegexp converts a graphite glob to an anchored regexp. Wildcards do
// not cross '.'.
func globRegexp(glob string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	depth := 0
	for i := 0; i < len(glob); i++ {
		switch c := glob[i]; c {
		case '*':
			b.WriteString(`[^.]*`)
		case '?':
			b.WriteString(`[^.]`)
		case '{':
			depth++
			b.WriteString("(?:")
		case '}':
			if depth == 0 {
				return nil, fmt.Errorf("glob %q: unbalanced }", glob)
			}
			depth--
			b.WriteString(")")
		case ',':
			if depth > 0 {
				b.WriteString("|")
			} else {
				b.WriteString(",")
			}
		case '[':
			j := strings.IndexByte(glob[i:], ']')
			if j < 0 {
				return nil, fmt.Errorf("glob %q: unbalanced [", glob)
			}
			b.WriteString(glob[i : i+j+1])
			i += j
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("glob %q: unbalanced {", glob)
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}

func (o *options) selectNames(ctx context.Context, local *client) ([]string, error) {
	var names []string
	switch o.namesFile {
	case "":
		var err error
		if names, err = local.list(ctx); err != nil {
			return nil, fmt.Errorf("local metric list: %w", err)
		}
	default:
		f := os.Stdin
		if o.namesFile != "-" {
			var err error
			if f, err = os.Open(o.namesFile); err != nil {
				return nil, err
			}
			defer f.Close()
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			if n := strings.TrimSpace(sc.Text()); n != "" {
				names = append(names, n)
			}
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	}
	var re *regexp.Regexp
	if o.glob != "" {
		var err error
		if re, err = globRegexp(o.glob); err != nil {
			return nil, err
		}
	}
	out := names[:0]
	for _, n := range names {
		if strings.HasPrefix(n, o.prefix) && (re == nil || re.MatchString(n)) {
			out = append(out, n)
		}
	}
	return out, nil
}

func newClient(base string, o *options, lim *rate.Limiter) *client {
	return &client{
		base: base,
		http: &http.Client{Timeout: o.timeout, Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConnsPerHost: o.workers,
			IdleConnTimeout:     90 * time.Second,
		}},
		limiter:    lim,
		retries:    o.retries,
		backoff:    o.backoff,
		maxBackoff: o.maxBack,
	}
}

type report struct {
	From, Until       int64
	FromUTC, UntilUTC string
	DryRun            bool
	Metrics           map[string]int64
	Replicas          map[string]map[string]int64
	Local             map[string]int64
	Write             map[string]int64 `json:",omitempty"`
	Seconds           float64
	Error             string `json:",omitempty"`
	NothingToDo       bool   `json:",omitempty"`
}

func clientReport(c *client) map[string]int64 {
	return map[string]int64{
		"requests": c.stats.requests.Load(), "retries": c.stats.retries.Load(), "failures": c.stats.failures.Load(),
		"bytes_out": c.stats.bytesOut.Load(), "bytes_in": c.stats.bytesIn.Load(),
	}
}

func (e *engine) report(from, until int64, began time.Time, err error) report {
	s := &e.stats
	r := report{
		From: from, Until: until,
		FromUTC:  time.Unix(from, 0).UTC().Format(time.RFC3339),
		UntilUTC: time.Unix(until, 0).UTC().Format(time.RFC3339),
		DryRun:   e.cfg.dryRun,
		Metrics: map[string]int64{
			"selected": s.names.Load(), "skipped_checkpoint": s.resumed.Load(), "scanned": s.scanned.Load(),
			"invalid_names": s.invalid.Load(), "inactive": s.inactive.Load(), "complete": s.complete.Load(),
			"with_gaps": s.withGaps.Load(), "missing_slots": s.missingSlots.Load(), "fillable_points": s.fillable.Load(),
			"unfilled_metrics": s.unfilledMetrics.Load(), "step_mismatch": s.stepMismatch.Load(),
			"fallback_metrics": s.fallbackMetrics.Load(), "batches": s.batches.Load(), "failed_batches": s.failedBatches.Load(),
		},
		Replicas: map[string]map[string]int64{},
		Local:    clientReport(e.local),
		Seconds:  time.Since(began).Seconds(),
	}
	for _, c := range e.replicas {
		r.Replicas[c.base] = clientReport(c)
	}
	if e.out != nil {
		w := &e.out.stats
		r.Write = map[string]int64{
			"points_sent": w.points.Load(), "bytes": w.bytes.Load(), "flushes": w.flushes.Load(),
			"reconnects": w.reconnects.Load(), "cache_pauses": w.pauses.Load(),
			"paused_seconds": int64(time.Duration(w.pausedNanos.Load()).Seconds()),
		}
	}
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

func run(ctx context.Context, o *options, now time.Time) (report, error) {
	began := time.Now()
	from, until, ok, err := o.window(now)
	if err != nil {
		return report{}, err
	}
	if !ok {
		log.Printf("no unclean stop recorded in %s", o.heartbeatFile)
		return report{NothingToDo: true}, nil
	}
	log.Printf("window %s .. %s (%s)", time.Unix(from, 0).UTC().Format(time.RFC3339),
		time.Unix(until, 0).UTC().Format(time.RFC3339), time.Duration(until-from)*time.Second)

	var localLim *rate.Limiter
	if o.localRPS > 0 {
		localLim = rate.NewLimiter(rate.Limit(o.localRPS), 1)
	}
	replicaLim := rate.NewLimiter(rate.Limit(o.replicaRPS), 1)
	e := &engine{
		cfg: engineConfig{
			from: from, until: until, lookback: int64(o.lookback / time.Second),
			batch: o.batch, workers: o.workers, dryRun: o.dryRun, printGaps: o.printGaps,
			checkpoint: o.checkpoint, progress: o.progress,
		},
		local: newClient(strings.TrimRight(o.local, "/"), o, localLim),
	}
	for _, r := range o.replicas {
		e.replicas = append(e.replicas, newClient(r, o, replicaLim))
	}
	if o.checkpoint != "" && !o.dryRun {
		if e.ckpt, err = loadCheckpoint(o.checkpoint, from, until); err != nil {
			return report{}, err
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	writerDone := make(chan error, 1)
	if !o.dryRun {
		var gate *cacheGate
		switch {
		case o.cachePause > 0 && o.heartbeatFile != "":
			if _, err := heartbeat.Read(o.heartbeatFile); err != nil {
				return report{}, fmt.Errorf("cache watch: %w (set go-carbon common.heartbeat-file, or -cache-pause=0)", err)
			}
			resume := o.cacheResume
			if resume == 0 {
				resume = 0.8 * o.cachePause
			}
			gate = &cacheGate{path: o.heartbeatFile, pauseAt: o.cachePause, resumeAt: min(resume, o.cachePause), poll: time.Second, now: time.Now}
		case o.cachePause > 0:
			log.Printf("no -heartbeat-file: local cache fill is not watched; only -write-rate limits writes")
		}
		e.out = newWriter(o.ingest, o.writeRate, o.flushPt, gate, o.writeRetries, o.backoff, o.maxBack)
		go func() {
			err := e.out.run(runCtx)
			writerDone <- err
			cancel() // a dead writer must not keep the replicas busy
		}()
	}
	if o.otlpEndpoint != "" {
		p := newOTLPPusher(o.otlpEndpoint, o.otlpInterval, o.otlpAttributes(), e.otlpMetrics)
		go p.run()
		defer p.close()
	}
	names, err := o.selectNames(runCtx, e.local)
	if err == nil {
		log.Printf("%d metrics selected", len(names))
		err = e.run(runCtx, names)
	}
	cancel()
	if !o.dryRun {
		if werr := <-writerDone; werr != nil && !errors.Is(werr, context.Canceled) {
			err = werr
		}
	}
	return e.report(from, until, began, err), err
}

func main() { os.Exit(realMain()) }

func realMain() int {
	log.SetFlags(log.LstdFlags | log.LUTC)
	o, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		log.Print(err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r, err := run(ctx, o, time.Now())
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(r)
	if err != nil {
		log.Print(err)
		return 1
	}
	return 0
}
