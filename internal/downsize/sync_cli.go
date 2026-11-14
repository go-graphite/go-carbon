package downsize

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
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
	sync          syncOptions
	copy          copyOptions
	live          syncLiveOptions
	phase         string
	legacyJournal bool
	explicit      map[string]bool
}

func parseSyncCommand(args []string) (syncCommand, error) {
	var c syncCommand
	flags := flag.NewFlagSet("storectl sync", flag.ContinueOnError)
	registerCopyFlags(flags, &c.copy)
	flags.Lookup("state").Usage = "state directory; existing copy journal FILE resumes first, with sync state in FILE.sync; --phase copy uses a journal file"
	flags.Lookup("streams").Usage = "total per-host transfer budget; 0 uses source/target advertised capacity; serial verify and copy phases share this budget"
	flags.Var(&c.sync.windows, "window", "[label:]FROM:UNTIL in Unix seconds; repeat for ALL gap windows in this run; full history is also checked/copied unless --no-full-history")
	flags.StringVar(&c.phase, "phase", "all", "all (discover, copy, verify, fill, reverify) or copy (only metric copy, legacy journal format)")
	flags.IntVar(&c.sync.batch, "verify-batch", 500, "names per verification batch")
	flags.IntVar(&c.sync.maxRounds, "max-rounds", 4, "verify/fill rounds")
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
	flags.DurationVar(&c.live.hostWait, "host-wait", 30*time.Minute, "wait for a restarting store before failing reads")
	flags.IntVar(&c.live.copyAttempts, "copy-attempts", 6, "copy engine runs per name list, resuming its journal")
	if err := flags.Parse(args); err != nil {
		return c, err
	}
	if flags.NArg() != 0 {
		return c, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *noFullHistory {
		c.sync.fullHistory = false
	}
	if *noFullHistory && len(c.sync.windows) == 0 {
		return c, errors.New("--no-full-history requires at least one --window")
	}
	c.explicit = map[string]bool{}
	flags.Visit(func(f *flag.Flag) { c.explicit[f.Name] = true })
	// Sampling is inactive outside full-history window verification. Retain its
	// historical identity value there so unchanged old journals still resume.
	if !c.explicit["sample-every"] && (len(c.sync.windows) == 0 || !c.sync.fullHistory) {
		c.sync.sampleEvery = 64
	}
	if c.phase != "all" && c.phase != "copy" {
		return c, errors.New("--phase must be all or copy")
	}
	if c.copy.streams < 0 || c.live.copyStreams < 0 || c.copy.maxBytes < 65536 || c.copy.transferIdle <= 0 || c.copy.maxNames < 1 || c.copy.maxNames > chunkstore.MaxTransferMetrics || c.copy.maxFailures < 0 || c.copy.tick <= 0 || c.copy.maxBackoff < c.copy.tick || c.copy.stallTimeout <= 0 || c.copy.progressInterval <= 0 || c.live.hostWait <= 0 || c.live.copyAttempts < 1 {
		return c, errors.New("invalid streams, batch limits, retry settings or timeouts")
	}
	if c.copy.engine != "metric" {
		return c, errors.New("--engine accepts metric; native window mode is selected by --window --no-full-history")
	}
	if c.copy.names != "" && c.copy.prefix != "" {
		return c, errors.New("--names and --prefix are exclusive")
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
	if c.phase == "copy" {
		for _, f := range []string{"window", "full-history", "no-full-history", "no-copy", "no-verify", "verify-batch", "max-rounds", "settle", "sample-every", "include-short-retention", "carbonserver-port", "host-wait", "copy-attempts", "copy-streams", "verify-streams"} {
			if c.explicit[f] {
				return c, fmt.Errorf("--%s has no effect with --phase copy", f)
			}
		}
	} else if native {
		for _, f := range []string{"max-batch-bytes", "copy-streams", "max-batch-names", "debt-guard", "retry-interval", "max-backoff", "stall-timeout", "max-failures", "engine", "copy-attempts"} {
			if c.explicit[f] {
				return c, fmt.Errorf("--%s has no effect in native window sync; remove it or remove --no-full-history", f)
			}
		}
	}
	if c.phase == "all" && c.explicit["max-failures"] {
		return c, errors.New("--max-failures is managed per sync batch; use --phase copy to set it explicitly")
	}
	if c.copy.names != "" {
		var err error
		c.sync.namesHash, err = readSyncNames(c.copy.names)
		c.sync.namesFile = c.copy.names
		if err != nil {
			return c, err
		}
		if c.phase == "copy" && len(c.copy.froms) > 1 {
			return c, errors.New("--names with --phase copy takes one --source (or --from) URL")
		}
	}
	if info, err := os.Stat(c.copy.state); err == nil {
		c.legacyJournal = !info.IsDir()
	} else if !errors.Is(err, os.ErrNotExist) {
		return c, err
	}
	// Retain old launchers' file-shaped state for new copies too.
	c.legacyJournal = c.legacyJournal || strings.HasSuffix(c.copy.state, ".jsonl")
	if c.phase == "all" && c.legacyJournal {
		if len(c.sync.windows) > 0 {
			return c, errors.New("--window requires a sync state directory; a legacy copy journal imports full histories; resume it with --phase copy")
		}
		c.sync.state = c.copy.state + ".sync"
	}
	c.live.noCopy = c.sync.noCopy
	c.live.tokenEnv, c.live.copyBytes, c.live.copyIdle = c.copy.tokenEnv, c.copy.maxBytes, c.copy.transferIdle
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
		if err := chunkstore.ValidateMetricName(n); err != nil {
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
	p := &effectiveSyncPolicy{Engine: "metric", Streams: streams, VerifyStreams: streams, CopyStreams: streams, Scheduling: "serial phases; each uses the total budget", VerifyBatch: c.sync.batch, CopyBatchNames: c.copy.maxNames, CopyBatchBytes: c.copy.maxBytes, Scope: "full-history", Windows: c.sync.windows.String(), Phase: c.phase, NoCopy: c.sync.noCopy, NoVerify: c.sync.noVerify}
	if c.phase == "copy" {
		p.VerifyStreams = 0
		p.VerifyBatch = 0
	}
	if len(c.sync.windows) > 0 {
		p.Scope = "windows+full-history"
		p.SampleEvery = c.sync.sampleEvery
		if !c.sync.fullHistory {
			p.Engine = "native-window"
			p.Scope = "windows-only"
			p.CopyStreams = 1
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
	backend := newSyncLiveBackend(c.live)
	streams := c.copy.streams
	var sourceCapacity int
	var target copyStatus
	for _, host := range append(append([]string(nil), c.sync.from...), c.sync.to) {
		if streams != 0 && host != c.sync.to {
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
				if c.phase == "copy" {
					sourceCapacity += status.MaxTransfers
				} else if sourceCapacity == 0 || status.MaxTransfers < sourceCapacity {
					sourceCapacity = status.MaxTransfers
				}
			}
		}
	}
	if streams == 0 {
		streams = defaultCopyStreams(sourceCapacity, target.MaxTransfers)
	}
	c.sync.streams = streams
	c.live.copyStreams = streams
	c.copy.streams = streams
	c.sync.policy = c.policy(streams)
	if c.sync.policy.CopyBatchBytes > 0 && target.MaxBodyBytes > 0 {
		c.sync.policy.CopyBatchBytes = min(c.sync.policy.CopyBatchBytes, target.MaxBodyBytes)
	}
	c.copy.policy = c.sync.policy
	c.live.copyTemplate = &c.copy
	backend.o = c.live
	event("effective_config", map[string]any{"effective": c.sync.policy})
	if c.phase == "copy" {
		return planFreeCopy(ctx, c.copy)
	}
	if c.legacyJournal {
		event("legacy_copy_resume", map[string]any{"copy_journal": c.copy.state, "sync_state": c.sync.state})
		if err := planFreeCopy(ctx, c.copy); err != nil {
			return err
		}
	}
	err = runSync(ctx, c.sync, backend)
	event("sync_io", map[string]any{"timings": backend.syncTimings()})
	if errors.Is(err, ErrSyncResidual) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitSyncResidual)
	}
	return err
}
