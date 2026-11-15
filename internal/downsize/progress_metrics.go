package downsize

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// progressObserver receives the exact snapshots published to progress JSON.
// Installed only by storectl before workers start; guarded by outputMu.
var progressObserver func(string, progressSnapshot)

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
	p := &progressMetrics{addr: addr, prefix: prefix, phase: "starting", active: 1, version: progressVersion(version)}
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
	outputMu.Lock()
	progressObserver = p.observe
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
	mu                                    sync.Mutex
	sendMu                                sync.Mutex
	addr, prefix, phase                   string
	active, done, total, rate             float64
	path, lastSentPhase                   string
	paths                                 map[string]bool
	warned                                bool
	code, round                           int
	samples                               []progressMetricSample
	version, lastSentVersion, versionFile string
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
	at := time.Now()
	eta := p.eta(at)
	code, round := p.code, p.round
	p.mu.Unlock()
	// Fixed metric names; the run identity distinguishes simultaneous controllers.
	// A timestamp heartbeat lets dashboards exclude stopped/abandoned runs.
	now := at.Unix()
	var b strings.Builder
	if p.lastSentPhase != "" && p.lastSentPhase != phase {
		fmt.Fprintf(&b, "%s.%s.active 0 %d\n%s.%s.names_per_second 0 %d\n", p.prefix, p.lastSentPhase, now, p.prefix, p.lastSentPhase, now)
	}
	for _, v := range []struct {
		name  string
		value float64
	}{{"active", active}, {"done", done}, {"total", total}, {"names_per_second", rate}, {"heartbeat", float64(now)}} {
		fmt.Fprintf(&b, "%s.%s.%s %g %d\n", p.prefix, phase, v.name, v.value, now)
	}
	for _, v := range []struct {
		name  string
		value float64
	}{{"phase", float64(code)}, {"round", float64(round)}, {"phase_done", done}, {"phase_total", total}, {"eta_seconds", eta}, {"active", active}, {"heartbeat", float64(now)}} {
		fmt.Fprintf(&b, "%s.%s %g %d\n", p.prefix, v.name, v.value, now)
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

// Public build revisions are bounded, safe Graphite path components.
func progressVersion(version string) string {
	dirty := strings.HasSuffix(version, "-dirty")
	version = strings.TrimSuffix(strings.ToLower(version), "-dirty")
	if len(version) > 12 {
		version = version[:12]
	}
	if len(version) != 12 || strings.Trim(version, "0123456789abcdef") != "" {
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
	if !strings.HasPrefix(name, "ready-recovery-") && !strings.HasPrefix(name, "ready-fill-") {
		return false
	}
	return p.paths[filepath.Join(filepath.Dir(parent), "progress.json")]
}
