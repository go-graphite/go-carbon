package downsize

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// progressObserver receives the exact snapshots published to progress JSON.
// Installed only by storectl before workers start; guarded by outputMu.
var progressObserver func(string, progressSnapshot)

// Retry observations also cover phases without a sync progress-file loop,
// including initial capacity probes and final READY read-back.
var storeWaitObserver func(int64, bool)

func observeStoreWait(delta int64, retry bool) {
	outputMu.Lock()
	defer outputMu.Unlock()
	if storeWaitObserver != nil {
		storeWaitObserver(delta, retry)
	}
}

// MainWithMetrics exports controller progress through a local Graphite receiver.
// Delivery is bounded and never changes the transfer result or retry policy.
func MainWithMetrics(args []string, version string) error {
	addr := os.Getenv("STORECTL_GRAPHITE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:2003"
	}
	host, _ := os.Hostname()
	prefix, state := progressMetricIdentity(host, args)
	if state == "" {
		return Main(args)
	}
	p := &progressMetrics{addr: addr, prefix: prefix, phase: "starting", active: 1, version: progressVersion(version), started: float64(time.Now().UnixNano()) / 1e9}
	if root, err := os.UserCacheDir(); err == nil {
		p.versionFile = filepath.Join(root, "storectl", "metric-versions", fmt.Sprintf("%x", sha256.Sum256([]byte(prefix))))
		if data, err := os.ReadFile(p.versionFile); err == nil {
			p.lastSentVersion = progressVersion(string(data))
		}
	}
	if args[0] == "sync" {
		// A legacy FILE resumes copying before its adjacent FILE.sync state.
		// Both are the same controller; nested batch journals are excluded.
		p.paths = map[string]bool{
			state + ".progress.json":                      true,
			filepath.Join(state, "progress.json"):         true,
			filepath.Join(state+".sync", "progress.json"): true,
		}
	}
	if args[0] == "sync" {
		p.redoEnabled = true
		p.restoreRedoMetrics(state)
	}
	outputMu.Lock()
	progressObserver = p.observe
	storeWaitObserver = p.observeStoreWait
	outputMu.Unlock()
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		p.send()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				p.send()
			}
		}
	}()
	defer func() {
		outputMu.Lock()
		progressObserver = nil
		storeWaitObserver = nil
		outputMu.Unlock()
		close(stop)
		<-stopped
		p.mu.Lock()
		p.active = 0
		p.mu.Unlock()
		p.send()
	}()
	return Main(args)
}

// The state path survives process restarts and binary/client replacements. Do
// not include PID, time, flags that tune concurrency, or journal contents that
// grow during copying. Commands without state do not publish copy progress.
func progressMetricIdentity(host string, args []string) (prefix, state string) {
	if len(args) == 0 {
		return "", ""
	}
	labelPath, blockLabel := "", ""
	for i := 1; i < len(args); i++ {
		if args[i] == "--" {
			break
		}
		key, value, equals := strings.Cut(args[i], "=")
		if key != "--state" && key != "-state" && key != "--resume" && key != "-resume" && key != "--block" && key != "-block" {
			continue
		}
		if !equals && i+1 < len(args) {
			i++
			value = args[i]
		}
		if key == "--block" || key == "-block" {
			blockLabel = strings.TrimSuffix(filepath.Base(value), filepath.Ext(value))
			if data, err := os.ReadFile(value); err == nil {
				var block syncBlock
				if json.Unmarshal(data, &block) == nil {
					state = block.State
				}
			}
			continue
		}
		state = value
		labelPath = filepath.Base(filepath.Clean(value))
		if key == "--resume" || key == "-resume" {
			if data, err := os.ReadFile(filepath.Join(value, "config.json")); err == nil {
				var saved savedSyncConfig
				if json.Unmarshal(data, &saved) == nil && saved.CopyState != "" {
					state = saved.CopyState
				}
			}
		}
	}
	if state == "" {
		return "", ""
	}
	state = metricStatePath(state)
	if blockLabel != "" {
		labelPath = blockLabel
	}
	id := progressRunLabel(host, labelPath, state)
	return "tools.storectl." + metricToken(host) + "." + id + "." + metricToken(args[0]), state
}

// Claims persist across client restarts. Atomic links publish complete owners so
// concurrent processes cannot both claim one label for different state paths.
func progressRunLabel(host, label, state string) (result string) {
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(state)))[:12]
	label = strings.Trim(strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		return '-'
	}, strings.ToLower(label)), "-")
	if label == "" {
		label = "state"
	}
	root, err := os.UserCacheDir()
	if err != nil {
		return hash
	}
	root = filepath.Join(root, "storectl", "metric-labels", metricToken(host))
	if err := os.MkdirAll(root, 0700); err != nil {
		return hash
	}
	identity := filepath.Join(root, ".state-"+hash)
	if saved, err := os.ReadFile(identity); err == nil && len(saved) > 0 {
		return string(saved)
	}
	defer func() {
		f, err := os.CreateTemp(root, ".identity-")
		if err != nil {
			return
		}
		defer os.Remove(f.Name())
		_, writeErr := f.WriteString(result)
		closeErr := f.Close()
		if writeErr == nil && closeErr == nil {
			_ = os.Link(f.Name(), identity)
			if saved, err := os.ReadFile(identity); err == nil {
				result = string(saved)
			}
		}
	}()
	f, err := os.CreateTemp(root, ".claim-")
	if err != nil {
		return hash
	}
	defer os.Remove(f.Name())
	_, writeErr := f.WriteString(state)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		return hash
	}
	for i := 0; i < 100; i++ {
		candidate := label
		if i == 1 {
			candidate += "-" + hash
		}
		if i > 1 {
			candidate += fmt.Sprintf("-%s-%d", hash, i)
		}
		claim := filepath.Join(root, candidate)
		if err := os.Link(f.Name(), claim); err == nil {
			return candidate
		}
		if owner, err := os.ReadFile(claim); err == nil {
			if string(owner) == state {
				return candidate
			}
		} else {
			return hash
		}
	}

	return hash
}

func metricStatePath(path string) string {
	if absolute, err := filepath.Abs(path); err == nil {
		return absolute
	}
	return filepath.Clean(path)
}

func metricToken(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' {
			return r
		}
		return '_'
	}, s)
}

type progressMetrics struct {
	ready                                 readyETAModel
	snapshot                              progressSnapshot
	mu                                    sync.Mutex
	sendMu                                sync.Mutex
	addr, prefix, phase                   string
	active, done, total, rate             float64
	quarantined, policySkipped            float64
	ownershipExcluded                     float64
	redoEnabled                           bool
	redoNames                             map[string]int64
	redoByPath                            map[string]map[string]int64
	repairFailedNames                     float64
	legacyRevalidated                     map[string]int64
	legacyAdopted, legacyReverified       map[string]int64
	phaseRegressions                      int64
	ambiguousReceiptNames                 map[string]int64
	receiptContradictions                 int64
	freshDone                             float64
	freshDoneAvailable                    float64
	storeRetries, storeWaiting            int64
	storeWaitSeconds                      float64
	storeWaitStarted                      time.Time
	started                               float64
	path, lastSentPhase                   string
	paths                                 map[string]bool
	warned                                bool
	code, round                           int
	samples                               []progressMetricSample
	version, lastSentVersion, versionFile string
}

func (p *progressMetrics) observeStoreWait(delta int64, retry bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if retry {
		p.storeRetries++
	}
	if p.storeWaiting == 0 && delta > 0 {
		p.storeWaitStarted = time.Now()
	}
	p.storeWaiting += delta
	if p.storeWaiting == 0 && delta < 0 {
		p.storeWaitSeconds += time.Since(p.storeWaitStarted).Seconds()
	}
}

func (p *progressMetrics) observe(path string, snapshot progressSnapshot) {
	p.mu.Lock()
	if p.paths != nil {
		if !p.accepts(path) {
			p.mu.Unlock()
			return
		}
	} else {
		// Other controllers publish their parent before nested copy starts.
		if p.path == "" {
			p.path = path
		}
		if p.path != path {
			p.mu.Unlock()
			return
		}
	}
	if p.redoEnabled || snapshot.RedoNames != nil {
		p.redoEnabled = true
		if p.redoByPath == nil {
			p.redoByPath = map[string]map[string]int64{}
		}
		p.redoByPath[path] = cloneRedoCounts(snapshot.RedoNames)
		p.redoNames = map[string]int64{}
		for _, counts := range p.redoByPath {
			for phase, n := range counts {
				p.redoNames[phase] += n
			}
		}

	}

	p.repairFailedNames = max(p.repairFailedNames, float64(snapshot.RepairFailedNames))
	if p.legacyRevalidated == nil {
		p.legacyRevalidated = map[string]int64{}
	}
	p.legacyRevalidated[path] = snapshot.LegacyRevalidated
	if p.legacyAdopted == nil {
		p.legacyAdopted = map[string]int64{}
		p.legacyReverified = map[string]int64{}
	}
	p.legacyAdopted[path] = snapshot.LegacyAdopted
	p.legacyReverified[path] = snapshot.LegacyReverified
	p.quarantined = max(p.quarantined, float64(snapshot.Quarantined))
	p.policySkipped = max(p.policySkipped, float64(snapshot.PolicySkipped))
	p.ownershipExcluded = float64(snapshot.OwnershipExcluded)
	p.snapshot = snapshot
	p.phaseRegressions = max(p.phaseRegressions, snapshot.PhaseRegressions)
	if p.ambiguousReceiptNames == nil {
		p.ambiguousReceiptNames = map[string]int64{}
	}
	p.ambiguousReceiptNames[path] = snapshot.AmbiguousReceiptNames
	p.receiptContradictions = max(p.receiptContradictions, snapshot.ReceiptContradictions)
	oldPhase, oldRound := p.phase, p.round
	p.phase = metricToken(snapshot.Phase)
	if snapshot.WaitingForStore {
		p.phase = "waiting_for_store"
	}
	if p.phase == "" {
		p.phase = "starting"
	}
	p.code, p.round = progressPhase(snapshot)
	if oldPhase != p.phase || oldRound != p.round || p.path != path || float64(snapshot.Done) < p.done {
		p.samples = nil
	}
	p.path = path
	p.done, p.total, p.rate = float64(snapshot.Done), float64(snapshot.Total), snapshot.Rate
	p.freshDone, p.freshDoneAvailable = float64(snapshot.FreshDone), 0
	if snapshot.FreshDoneAvailable {
		p.freshDoneAvailable = 1
	}
	p.active = 0
	if snapshot.Status == "running" {
		p.active = 1
	}
	terminal := p.active == 0
	p.mu.Unlock()
	if terminal {
		p.send()
	}
}

func (p *progressMetrics) send() {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	p.mu.Lock()
	phase, active, done, total, rate := p.phase, p.active, p.done, p.total, p.rate
	repairFailedNames := p.repairFailedNames
	var legacyRevalidated, ambiguousReceiptNames, legacyAdopted, legacyReverified int64
	for _, n := range p.legacyAdopted {
		legacyAdopted += n
	}
	for _, n := range p.legacyReverified {
		legacyReverified += n
	}
	for _, n := range p.ambiguousReceiptNames {
		ambiguousReceiptNames += n
	}
	receiptContradictions := p.receiptContradictions
	for _, n := range p.legacyRevalidated {
		legacyRevalidated += n
	}
	redoNames := cloneRedoCounts(p.redoNames)
	redoEnabled := p.redoEnabled || p.redoNames != nil
	quarantined, policySkipped := p.quarantined, p.policySkipped
	ownershipExcluded := p.ownershipExcluded
	at := time.Now()
	eta := p.eta(at)
	ready := p.ready.estimate(at, p.path, p.snapshot)
	code, round, phaseRegressions := p.code, p.round, p.phaseRegressions
	freshDone, freshAvailable, started := p.freshDone, p.freshDoneAvailable, p.started
	retries, waiting, waitSeconds := p.storeRetries, float64(0), p.storeWaitSeconds
	if p.storeWaiting > 0 {
		waiting = 1
		waitSeconds += at.Sub(p.storeWaitStarted).Seconds()
	}
	p.mu.Unlock()
	// Fixed metric names; the run identity distinguishes simultaneous controllers.
	// A timestamp heartbeat lets dashboards exclude stopped/abandoned runs.
	now := at.Unix()
	var b strings.Builder
	fmt.Fprintf(&b, "%s.legacy_observation_adopted %d %d\n%s.legacy_observation_reverified %d %d\n", p.prefix, legacyAdopted, now, p.prefix, legacyReverified, now)
	fmt.Fprintf(&b, "%s.legacy_revalidated_names %d %d\n", p.prefix, legacyRevalidated, now)
	fmt.Fprintf(&b, "%s.ambiguous_receipt_names %d %d\n%s.receipt_contradictions %d %d\n", p.prefix, ambiguousReceiptNames, now, p.prefix, receiptContradictions, now)
	fmt.Fprintf(&b, "%s.phase_regressions %d %d\n", p.prefix, phaseRegressions, now)
	fmt.Fprintf(&b, "%s.ownership_excluded %g %d\n", p.prefix, ownershipExcluded, now)
	if p.lastSentPhase != "" && p.lastSentPhase != phase {
		fmt.Fprintf(&b, "%s.%s.active 0 %d\n%s.%s.names_per_second 0 %d\n", p.prefix, p.lastSentPhase, now, p.prefix, p.lastSentPhase, now)
	}
	for _, v := range []struct {
		name  string
		value float64
	}{{"fresh_done", freshDone}, {"active", active}, {"done", done}, {"total", total}, {"names_per_second", rate}, {"heartbeat", float64(now)}} {
		fmt.Fprintf(&b, "%s.%s.%s %g %d\n", p.prefix, phase, v.name, v.value, now)
	}
	for _, v := range []struct {
		name  string
		value float64
	}{{"quarantined", quarantined}, {"policy_skipped", policySkipped}, {"repair_failed_names", repairFailedNames}, {"store_retries", float64(retries)}, {"store_wait_seconds", waitSeconds}, {"waiting_for_store", waiting}, {"phase_fresh_done", freshDone}, {"fresh_done_available", freshAvailable}, {"process_start_time_seconds", started}, {"phase", float64(code)}, {"round", float64(round)}, {"phase_done", done}, {"phase_total", total}, {"eta_seconds", eta}, {"eta_ready_seconds", ready.seconds}, {"eta_ready_known_seconds", ready.known}, {"eta_ready_unknown_parts", float64(ready.unknown)}, {"phases_left", float64(ready.phases)}, {"active", active}, {"heartbeat", float64(now)}} {
		fmt.Fprintf(&b, "%s.%s %g %d\n", p.prefix, v.name, v.value, now)
	}
	if redoEnabled {
		for _, phase := range []string{"verify", "fetch", "import"} {
			fmt.Fprintf(&b, "%s.%s.redo_names %d %d\n", p.prefix, phase, redoNames[phase], now)
		}
	}
	if p.version != "" {
		if p.lastSentVersion != "" && p.lastSentVersion != p.version {
			fmt.Fprintf(&b, "%s.client.%s.active 0 %d\n", p.prefix, p.lastSentVersion, now)
		}
		fmt.Fprintf(&b, "%s.client.%s.active %g %d\n", p.prefix, p.version, active, now)
	}
	conn, err := net.DialTimeout("tcp", p.addr, 500*time.Millisecond)
	if err == nil {
		_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		_, err = fmt.Fprint(conn, b.String())
		_ = conn.Close()
	}
	if err == nil {
		p.lastSentPhase = phase
		if p.version != "" && p.lastSentVersion != p.version {
			p.lastSentVersion = p.version
			if p.versionFile != "" {
				if os.MkdirAll(filepath.Dir(p.versionFile), 0700) == nil {
					// One controller owns each run; atomically preserve its last revision.
					temp := p.versionFile + ".tmp"
					if os.WriteFile(temp, []byte(p.version), 0600) == nil {
						_ = os.Rename(temp, p.versionFile)
					}
				}
			}
		}
	}
	if err != nil && !p.warned {
		p.warned = true
		fmt.Fprintln(os.Stderr, "storectl: progress metric delivery failed:", err)
	}
}

// Stable phase codes let a single series follow a controller across rounds.
// Unknown phases remain distinguishable from startup and terminal states.
func progressPhase(s progressSnapshot) (code, round int) {
	phase := metricToken(s.Phase)
	if s.Status == "interrupted" {
		return 7, 0
	}
	if s.Status == "failed" || s.Status == "incomplete" {
		return 6, 0
	}
	if s.WaitingForStore {
		return 9, 0
	}
	switch phase {
	case "", "starting":
		return 0, 0
	case "discover":
		return 1, 0
	case "copy", "window_missing", "window_retry":
		return 2, 0
	case "verify":
		return 3, s.Round
	case "fill":
		return 4, s.Round
	case "ready":
		return 5, 0
	}
	if value, ok := strings.CutPrefix(phase, "verify_round_"); ok {
		n, err := strconv.Atoi(value)
		if err == nil && n > 0 {
			return 3, n
		}
	}
	return -1, 0
}

type progressMetricSample struct {
	at   time.Time
	done float64
}

// eta uses the observed counter delta over the last three minutes. Keeping
// idle samples makes a stalled phase lose its old rate. Phase/round changes
// clear the window in observe; journal replay never becomes an initial rate.
// Caller holds mu.
func (p *progressMetrics) eta(now time.Time) float64 {
	if p.code == 5 {
		return 0
	}
	if p.active == 0 || p.code == 9 {
		return -1
	}
	p.samples = append(p.samples, progressMetricSample{now, p.done})
	cutoff := now.Add(-3 * time.Minute)
	for len(p.samples) > 1 && p.samples[0].at.Before(cutoff) {
		p.samples = p.samples[1:]
	}
	first := p.samples[0]
	elapsed, completed := now.Sub(first.at).Seconds(), p.done-first.done
	if elapsed <= 0 || completed <= 0 || p.total <= 0 {
		return -1
	}
	eta := max(0, p.total-p.done) * elapsed / completed
	if math.IsNaN(eta) || math.IsInf(eta, 0) {
		return -1
	}
	return eta
}

var progressRevisionPattern = regexp.MustCompile(`[0-9a-f]{12}`)

// Match the daemon's 12-hex revision label, including prefixed release builds.
// Only the revision and optional dirty marker become a Graphite path component.
func progressVersion(version string) string {
	version = strings.ToLower(version)
	dirty := strings.HasSuffix(version, "-dirty")
	version = progressRevisionPattern.FindString(version)
	if version == "" {
		version = "unknown"
	}
	if dirty {
		version += "-dirty"
	}
	return version
}

// READY may start a fresh sync after a store restart or final parity check.
// Its direct child is part of this run; per-batch child journals are not.
func (p *progressMetrics) accepts(path string) bool {
	path = metricStatePath(path)
	if p.paths[path] {
		return true
	}
	if filepath.Base(path) != "progress.json" {
		return false
	}
	parent := filepath.Dir(path)
	name := filepath.Base(parent)
	if !strings.HasPrefix(name, "fresh-") && !strings.HasPrefix(name, "ready-recovery-") && !strings.HasPrefix(name, "ready-fill-") && !strings.HasPrefix(name, "ready-proof-") && !strings.HasPrefix(name, "gapfill-") {
		return false
	}
	return p.accepts(filepath.Join(filepath.Dir(parent), "progress.json"))
}
