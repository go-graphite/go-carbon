package downsize

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Phases are serial: verification and copying can each use the entire budget.
// Native window copying currently processes one range at a time.
type effectiveSyncPolicy struct {
	Engine         string `json:"engine"`
	Streams        int    `json:"streams"`
	VerifyStreams  int    `json:"verify_streams"`
	CopyStreams    int    `json:"copy_streams"`
	Scheduling     string `json:"scheduling"`
	VerifyBatch    int    `json:"verify_batch"`
	CopyBatchNames int    `json:"copy_batch_names"`
	CopyBatchBytes int64  `json:"copy_batch_bytes"`
	Scope          string `json:"scope"`
	Windows        string `json:"windows"`
	SampleEvery    int    `json:"sample_every"`
	Phase          string `json:"phase"`
	NoCopy         bool   `json:"no_copy"`
	NoVerify       bool   `json:"no_verify"`
}

type syncCommand struct {
	health         targetHealthOptions
	freshReason    string
	ready          syncReadyOptions
	sync           syncOptions
	copy           copyOptions
	live           syncLiveOptions
	legacyJournal  bool
	explicit       map[string]bool
	resuming       bool
	config         savedSyncConfig
	jwtSecret      string
	goCarbonConfig string
	// jwtSecretConfig is the go-carbon config the default --jwt-secret came from.
	jwtSecretConfig string
	rate            int64
	cacheGuard      float64
	timeout         time.Duration
}

func parseSyncCommand(args []string) (syncCommand, error) {
	var c syncCommand
	var err error
	args, err = expandSyncBlock(args)
	if err != nil {
		return c, err
	}
	flags := flag.NewFlagSet("storectl sync", flag.ContinueOnError)
	registerCopyFlags(flags, &c.copy)
	flags.StringVar(&c.sync.fillPath, "fill-path", "auto", "fill holes through ingest when archive routing is safe; auto or transfer")
	flags.StringVar(&c.sync.fillIngest, "fill-ingest", "127.0.0.1:2003", "target plaintext receiver for automatic fill (run sync on the target, or override)")
	flags.BoolVar(&c.health.enabled, "target-health", true, "pause new sync batches while the target ingest cache is overloaded; monitoring failures log and continue")
	flags.Float64Var(&c.health.high, "target-health-high", 0.75, "pause at or above this fraction of the target cache limit")
	flags.Float64Var(&c.health.low, "target-health-low", 0.60, "resume below this fraction of the target cache limit")
	flags.DurationVar(&c.timeout, "timeout", 0, "overall sync deadline including store restart waits; 0 waits without limit")
	flags.Int64Var(&c.rate, "rate", 0, "aggregate copied bytes per second, paced between completed batches; 0 unlimited")
	flags.Float64Var(&c.cacheGuard, "cache-threshold", 0.75, "slow the copy while any source or target write cache is at least this full (0..1), and pause it at 95%; 0 disables")
	flags.StringVar(&c.freshReason, "allow-fresh-start", "", "reason for a new child state directory when --state already contains work; preserves prior state")
	resume := flags.String("resume", "", "resume from the saved configuration in this state directory; only operational scheduling, timeout, streams, rate, cache-threshold and READY inputs may override it")
	flags.StringVar(&c.jwtSecret, "jwt-secret", "", "buckyd JWT signing secret file; tokens refresh automatically before expiry (default: [buckyd] auth-jwt-secret-file from --config, else "+hostJWTSecretPath+")")
	flags.StringVar(&c.goCarbonConfig, "config", "", "local go-carbon config read for the default --jwt-secret (default: the -config of the running go-carbon, else "+defaultGoCarbonConfig+")")
	flags.Bool("ready", false, "run the final READY attestation (off by default)")
	c.copy.streams, c.copy.progressInterval = defaultSyncStreams, defaultSyncProgress
	flags.Lookup("streams").DefValue, flags.Lookup("progress-interval").DefValue = strconv.Itoa(defaultSyncStreams), defaultSyncProgress.String()
	flags.StringVar(&c.ready.scope, "ready-scope", "full-history", "READY coverage: full-history (default) or windows (requires --no-full-history --settle=0s)")
	flags.StringVar(&c.ready.carbonAPI, "carbonapi-url", "", "read API base URL for final READY read-back")
	flags.Var((*originList)(&c.ready.gapSelection), "gapfill-window", "with --ready-block: fill only these gap IDs (repeat or comma-separate); other missing receipts still block READY")
	flags.StringVar(&c.ready.proofBlock, "ready-block", "", "independent JSON block for exhaustive final READY proof; preserves the original copy scope")
	flags.IntVar(&c.ready.metricsPort, "process-metrics-port", 7007, "metrics port for target cache health and READY process_start_time_seconds on every store")
	flags.Var(&c.ready.period, "period", "FROM:UNTIL block copy/cutover period in Unix seconds")
	flags.BoolVar(&c.ready.gapsComplete, "gap-ledger-complete", false, "attest that --window includes every known input gap")
	flags.String("block", "", "generic JSON block file containing target, donors, peers, period, gaps and read-back URL")
	flags.Lookup("state").Usage = "state directory; existing copy journal FILE resumes first, with sync state in FILE.sync"
	flags.Lookup("streams").Usage = "total per-host transfer budget; the default 64 is lowered to the smallest max_transfers the source or target advertises, so it never causes 429s; an explicit value is used as given; 0 uses only the advertised capacity; serial verify and copy phases share this budget"
	flags.Var(&c.sync.windows, "window", "[label:]FROM:UNTIL in Unix seconds; repeat for ALL gap windows in this run; full history is also checked/copied unless --no-full-history")
	flags.IntVar(&c.sync.batch, "verify-batch", 500, "names per verification batch")
	flags.IntVar(&c.sync.maxRounds, "max-rounds", 2, "verification round limit; default verifies once, fills, then verifies repairs; 0 uses two rounds unless --rescan-all")
	flags.BoolVar(&c.sync.rescanAll, "rescan-all", false, "opt in to full-catalog convergence with mutation proofs; --max-rounds=0 converges while making progress")
	flags.DurationVar(&c.sync.settle, "settle", 10*time.Minute, "ignore the newest points while replicas are writing")
	flags.IntVar(&c.sync.sampleEvery, "sample-every", 1, "only with --window and full history enabled: check 1 in N full histories (1 all, 0 none)")
	flags.BoolVar(&c.sync.fullHistory, "full-history", true, "compatibility flag: full-history copy/fill is enabled by default; use --no-full-history for window-only work")
	noFullHistory := flags.Bool("no-full-history", false, "with --window: restrict all copying/filling to the selected windows")
	flags.BoolVar(&c.sync.noCopy, "no-copy", false, "verify without writing missing names or filling holes; residuals remain nonzero")
	flags.BoolVar(&c.sync.noVerify, "no-verify", false, "skip verification and fill after discovery/copy; does not establish readiness")
	flags.BoolVar(&c.sync.includeShort, "include-short-retention", false, "also verify/fill names whose archives all retain at most 3 days")
	flags.IntVar(&c.live.carbonPort, "carbonserver-port", 8080, "carbonserver port on every store for verification")
	flags.IntVar(&c.sync.verifyStreams, "verify-streams", 0, "deprecated compatibility flag: 0 or the same value as --streams; an independent verification budget is rejected")
	flags.IntVar(&c.live.copyStreams, "copy-streams", 0, "deprecated compatibility flag: use --streams for the total budget; a different positive value is rejected")
	flags.DurationVar(&c.live.hostWait, "host-wait", 30*time.Minute, "busy-store wait budget; restart failures are always retried until cancellation")
	flags.IntVar(&c.live.copyAttempts, "copy-attempts", 6, "copy engine runs per name list, resuming its journal")
	if err := flags.Parse(args); err != nil {
		return c, err
	}
	if flags.NArg() != 0 {
		return c, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	c.explicit = map[string]bool{}
	flags.Visit(func(f *flag.Flag) { c.explicit[f.Name] = true })
	if err := c.restoreConfig(flags, *resume); err != nil {
		return c, err
	}
	c.ready.noReady = flags.Lookup("ready").Value.String() != "true"
	if c.jwtSecret == "" {
		c.jwtSecret, c.jwtSecretConfig = defaultJWTSecretPath(c.goCarbonConfig, "/proc")
	}
	if math.IsNaN(c.health.low) || math.IsNaN(c.health.high) || c.health.low <= 0 || c.health.low >= c.health.high || c.health.high > 1 {
		return c, errors.New("target health thresholds must satisfy 0 < --target-health-low < --target-health-high <= 1")
	}
	if c.sync.fillPath != "auto" && c.sync.fillPath != "transfer" {
		return c, errors.New("--fill-path must be auto or transfer")
	}
	if c.timeout < 0 {
		return c, errors.New("--timeout must be nonnegative")
	}
	if !(c.cacheGuard >= 0 && c.cacheGuard <= 1) {
		return c, errors.New("--cache-threshold must be between 0 and 1")
	}
	if *noFullHistory {
		c.sync.fullHistory = false
	}
	if *noFullHistory && len(c.sync.windows) == 0 {
		return c, errors.New("--no-full-history requires at least one --window")
	}
	if c.ready.scope != "full-history" && c.ready.scope != "windows" {
		return c, errors.New("--ready-scope must be full-history or windows")
	}
	if c.ready.scope == "windows" && (c.sync.fullHistory || len(c.sync.windows) == 0 || c.sync.settle != 0 || len(c.ready.period) != 0) {
		return c, errors.New("--ready-scope=windows requires --window --no-full-history --settle=0s and no --period")
	}
	// Sampling is inactive outside full-history window verification. Retain its
	// historical identity value there so unchanged old journals still resume.
	if !c.explicit["sample-every"] && (len(c.sync.windows) == 0 || !c.sync.fullHistory) {
		c.sync.sampleEvery = 64
	}
	if c.rate < 0 || c.copy.streams < 0 || c.live.copyStreams < 0 || c.copy.maxBytes < 65536 || c.copy.transferIdle <= 0 || c.copy.maxNames < 1 || c.copy.maxNames > chunkstore.MaxTransferMetrics || c.copy.maxFailures < 0 || c.copy.tick <= 0 || c.copy.maxBackoff < c.copy.tick || c.copy.stallTimeout <= 0 || c.copy.progressInterval <= 0 || c.live.hostWait <= 0 || c.live.copyAttempts < 1 {
		return c, errors.New("invalid streams, batch limits, retry settings or timeouts")
	}
	if c.copy.engine != "metric" {
		return c, errors.New("--engine accepts metric; native window mode is selected by --window --no-full-history")
	}
	if c.copy.names != "" && c.copy.prefix != "" {
		return c, errors.New("--names and --prefix are exclusive")
	}
	if c.copy.to == "" {
		to, from := localBuckydEndpoint(c.goCarbonConfig, "/proc")
		if to == "" {
			return c, errors.New("--to required: no [buckyd] bind found in the local go-carbon config (--config)")
		}
		c.copy.to = to
		event("sync_default_target", map[string]any{"target": to, "from_config": from})
	}
	c.sync.to, c.sync.from = c.copy.to, append([]string(nil), c.copy.froms...)
	c.sync.ring, c.sync.state, c.sync.prefix = c.copy.ring, c.copy.state, c.copy.prefix
	c.sync.streams, c.sync.progress = max(1, c.copy.streams), c.copy.progressInterval
	if err := c.sync.normalize(); err != nil {
		return c, err
	}
	c.copy.to, c.copy.froms = c.sync.to, c.sync.from
	if c.live.copyStreams > 0 && c.live.copyStreams != c.copy.streams {
		return c, errors.New("--copy-streams no longer sets an independent pool; set --streams to the total budget and remove --copy-streams")
	}
	if c.sync.verifyStreams > 0 && c.sync.verifyStreams != c.copy.streams {
		return c, errors.New("--verify-streams no longer sets an independent budget; use --streams (keep old copy-wave state settings or use a fresh state directory)")
	}
	native := len(c.sync.windows) > 0 && !c.sync.fullHistory
	if native {
		for _, f := range []string{"max-batch-bytes", "copy-streams", "max-batch-names", "debt-guard", "retry-interval", "max-backoff", "stall-timeout", "max-failures", "engine", "copy-attempts"} {
			if c.explicit[f] {
				return c, fmt.Errorf("--%s has no effect in native window sync; remove it or remove --no-full-history", f)
			}
		}
	}
	if c.explicit["max-failures"] {
		return c, errors.New("--max-failures is managed per sync batch")
	}
	if c.copy.names != "" {
		var err error
		c.sync.namesHash, err = readSyncNames(c.copy.names)
		c.sync.namesFile = c.copy.names
		if err != nil {
			return c, err
		}
	}
	if info, err := os.Stat(c.copy.state); err == nil {
		c.legacyJournal = !info.IsDir()
	} else if errors.Is(err, os.ErrNotExist) {
		// Retain old launchers' file-shaped state for new copies too.
		c.legacyJournal = strings.HasSuffix(c.copy.state, ".jsonl")
	} else {
		return c, err
	}
	if c.legacyJournal {
		if native {
			return c, errors.New("a legacy copy journal imports full histories; --no-full-history requires a sync state directory")
		}
		c.sync.state = c.copy.state + ".sync"
	}
	c.live.noCopy = c.sync.noCopy
	c.live.tokenEnv, c.live.copyBytes, c.live.copyIdle = c.copy.tokenEnv, c.copy.maxBytes, c.copy.transferIdle
	if len(c.ready.gapSelection) > 0 && c.ready.proofBlock == "" {
		return c, errors.New("--gapfill-window requires --ready-block")
	}
	if c.ready.proofBlock != "" && !c.ready.noReady {
		if _, _, err := loadReadyProof(c.sync, c.ready); err != nil {
			return c, err
		}
	}
	c.captureConfig(flags)
	return c, nil
}

func readSyncNames(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("--names UTF-8 text file (one name per line, byte-sorted unique, not JSON): %w", err)
	}
	defer f.Close()
	hash := sha256.New()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 256<<10), 1<<20)
	previous := ""
	for scan.Scan() {
		n := strings.TrimSpace(scan.Text())
		if n == "" {
			continue
		}
		if strings.HasPrefix(n, "[") || strings.HasPrefix(n, "{") || n <= previous {
			return "", fmt.Errorf("--names: expected one metric name per line, byte-sorted unique (LC_ALL=C sort -u), not JSON; invalid name %q after %q", n, previous)
		}
		if err := chunkstore.ValidateMetricName(n); err != nil && len(n) <= chunkstore.MaxMetricNameBytes {
			return "", fmt.Errorf("--names: one metric name per line, byte-sorted unique, not JSON: %w", err)
		}
		fmt.Fprintln(hash, n)
		previous = n
	}
	if err := scan.Err(); err != nil {
		return "", fmt.Errorf("--names text file: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (c *syncCommand) policy(streams int) *effectiveSyncPolicy {
	p := &effectiveSyncPolicy{Engine: "metric", Streams: streams, VerifyStreams: streams, CopyStreams: streams, Scheduling: "serial phases; each uses the total budget", VerifyBatch: c.sync.batch, CopyBatchNames: c.copy.maxNames, CopyBatchBytes: c.copy.maxBytes, Scope: "full-history", Windows: c.sync.windows.String(), Phase: "all", NoCopy: c.sync.noCopy, NoVerify: c.sync.noVerify}
	if len(c.sync.windows) > 0 {
		p.Scope = "windows+full-history"
		p.SampleEvery = c.sync.sampleEvery
		if !c.sync.fullHistory {
			p.Engine = "native-window"
			p.Scope = "windows-only"
			p.CopyBatchNames = chunkstore.NativeMaxRangeNames
			p.CopyBatchBytes = 0
			p.SampleEvery = 0
		}
	}
	return p
}

func SyncMain(ctx context.Context, args []string) error {
	c, err := parseSyncCommand(args)
	if err != nil {
		return err
	}
	// Saved config keeps the typed hosts; requests use addresses, which is
	// how a target's ring nodes and transfer-sources list them.
	if c.sync.to, err = resolveEndpoint(ctx, c.sync.to); err != nil {
		return fmt.Errorf("--to: %w", err)
	}
	for i, f := range c.sync.from {
		if c.sync.from[i], err = resolveEndpoint(ctx, f); err != nil {
			return fmt.Errorf("--source: %w", err)
		}
	}
	c.copy.to, c.copy.froms = c.sync.to, append([]string(nil), c.sync.from...)
	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}
	if c.explicit["sample-every"] && (len(c.sync.windows) == 0 || !c.sync.fullHistory) {
		event("inactive_flag", map[string]any{"level": "warn", "flag": "--sample-every", "reason": "active only with --window --full-history; every name is verified in the selected scope"})
	}
	if c.explicit["full-history"] && len(c.sync.windows) == 0 {
		event("inactive_flag", map[string]any{"level": "warn", "flag": "--full-history", "reason": "without --window full history is already the default"})
	}
	if c.explicit["copy-streams"] {
		event("deprecated_flag", map[string]any{"level": "warn", "flag": "--copy-streams", "reason": "use only --streams; phases share one budget"})
	}
	if c.explicit["verify-streams"] {
		event("deprecated_flag", map[string]any{"level": "warn", "flag": "--verify-streams", "reason": "use only --streams; phases share one budget"})
	}
	if err := c.prepareStateIntent(); err != nil {
		event("sync_state_refused", map[string]any{"level": "error", "state": c.sync.state, "reason": err.Error()})
		return err
	}
	if err := os.MkdirAll(c.sync.state, 0700); err != nil {
		return err
	}
	owner, err := lockResumeState(filepath.Join(c.sync.state, "command"))
	if err != nil {
		return err
	}
	defer owner.Close()
	if err := c.saveConfig(); err != nil {
		return err
	}
	// The same cache serves discovery, native exports, verification and copy.
	auth := &hostTokenCache{readSecret: func() ([]byte, error) { return os.ReadFile(c.jwtSecret) }}
	if _, err := auth.get(c.jwtSecret); err != nil {
		return fmt.Errorf("JWT secret file %s (--jwt-secret, else auth-jwt-secret-file from --config or the running go-carbon config; found in %q): %w", c.jwtSecret, c.jwtSecretConfig, err)
	}
	event("sync_jwt_secret", map[string]any{"path": c.jwtSecret, "from_config": c.jwtSecretConfig})
	c.copy.auth, c.live.auth = auth, auth
	c.live.authOrigins = append(append([]string(nil), c.sync.from...), c.sync.to)
	if c.ready.proofBlock != "" && !c.ready.noReady {
		proof, _, err := loadReadyProof(c.sync, c.ready)
		if err != nil {
			return err
		}
		c.live.authOrigins = append(c.live.authOrigins, proof.from...)
	}
	limiter := &syncRateLimit{bytesPerSecond: c.rate}
	c.copy.rate, c.live.rate = limiter, limiter
	backend := newSyncLiveBackend(c.live)
	if c.health.enabled {
		backend.health = newTargetHealthGate(c.sync.to, c.health.high, c.health.low)
		c.copy.health = backend.health
		defer backend.health.monitor(ctx, targetHealthURL(c.sync.to, c.ready.metricsPort), 10*time.Second)()
	}
	defer backend.client.CloseIdleConnections()
	defer backend.m.client.CloseIdleConnections()
	if c.cacheGuard > 0 && !c.sync.noCopy {
		urls := map[string]string{}
		for _, store := range append(append([]string(nil), c.sync.from...), c.sync.to) {
			carbon, err := backend.carbon(store)
			if err != nil {
				return err
			}
			urls[store] = carbon + "/admin/info?scopes=cache"
		}
		governed, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			limiter.govern(governed, backend.client, urls, c.cacheGuard, 10*time.Second)
		}()
		defer func() {
			stop()
			<-done
		}()
	}
	err = runSyncReadyPrepared(ctx, &c.sync, c.ready, backend, func() error {
		streams := c.copy.streams
		var sourceCapacity int
		var target copyStatus
		for _, host := range c.live.authOrigins {
			if capped := streams == 0 || !c.explicit["streams"]; !capped && host != c.sync.to {
				continue
			}
			var status copyStatus
			err := backend.wait(ctx, "capacity", host, func() error {
				_, err := backend.m.requestOnce(ctx, host, "/v1/transfer/status", nil, &status)
				return err
			})
			if err != nil {
				return fmt.Errorf("resolve transfer budget on %s: %w", host, err)
			}
			if host == c.sync.to {
				target = status
			} else {
				if status.MaxTransfers > 0 {
					if sourceCapacity == 0 || status.MaxTransfers < sourceCapacity {
						sourceCapacity = status.MaxTransfers
					}
				}
			}
		}
		if streams == 0 {
			streams = defaultCopyStreams(sourceCapacity, target.MaxTransfers)
		} else if !c.explicit["streams"] {
			streams = min(streams, defaultCopyStreams(sourceCapacity, target.MaxTransfers))
		}
		c.sync.streams = streams
		c.live.copyStreams = streams
		c.copy.streams = streams
		c.sync.policy = c.policy(streams)
		if c.sync.policy.CopyBatchBytes > 0 && target.MaxBodyBytes > 0 {
			c.sync.policy.CopyBatchBytes = min(c.sync.policy.CopyBatchBytes, target.MaxBodyBytes)
		}
		c.config.Flags["streams"] = strconv.Itoa(streams)
		c.resuming = true
		if err := c.saveConfig(); err != nil {
			return err
		}
		c.copy.policy = c.sync.policy
		c.live.copyTemplate = &c.copy
		backend.o = c.live
		event("effective_config", map[string]any{"effective": c.sync.policy})
		return nil
	}, func() error { return c.run(ctx, backend) })
	event("sync_io", map[string]any{"timings": backend.syncTimings()})
	if errors.Is(err, ErrSyncQuarantined) {
		return err
	}
	if errors.Is(err, ErrSyncResidual) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitSyncResidual)
	}
	return err
}

// run preserves the legacy copy journal before entering the sync journal.
func (c *syncCommand) run(ctx context.Context, backend syncBackend) error {
	if c.legacyJournal && !c.sync.noCopy {
		event("legacy_copy_resume", map[string]any{"copy_journal": c.copy.state, "sync_state": c.sync.state})
		if err := planFreeCopy(ctx, c.copy); err != nil {
			return err
		}
		if c.sync.noVerify {
			return nil
		}
	}
	return runSync(ctx, c.sync, backend)
}
