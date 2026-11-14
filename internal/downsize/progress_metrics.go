package downsize

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// progressObserver receives the exact snapshots published to progress JSON.
// Installed only by storectl before workers start; guarded by outputMu.
var progressObserver func(string, progressSnapshot)

// MainWithMetrics exports controller progress through a local Graphite receiver.
// Delivery is bounded and never changes the transfer result or retry policy.
func MainWithMetrics(args []string) error {
	addr := os.Getenv("STORECTL_GRAPHITE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:2003"
	}
	host, _ := os.Hostname()
	prefix, state := progressMetricIdentity(host, args)
	if state == "" {
		return Main(args)
	}
	p := &progressMetrics{addr: addr, prefix: prefix, phase: "starting", active: 1}
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
	mu                        sync.Mutex
	sendMu                    sync.Mutex
	addr, prefix, phase       string
	active, done, total, rate float64
	path, lastSentPhase       string
	paths                     map[string]bool
	warned                    bool
}

func (p *progressMetrics) observe(path string, snapshot progressSnapshot) {
	p.mu.Lock()
	if p.paths != nil {
		if !p.paths[metricStatePath(path)] {
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
	p.phase = metricToken(snapshot.Phase)
	if p.phase == "" {
		p.phase = "starting"
	}
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
	p.mu.Unlock()
	// Fixed metric names; the run identity distinguishes simultaneous controllers.
	// A timestamp heartbeat lets dashboards exclude stopped/abandoned runs.
	now := time.Now().Unix()
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
	conn, err := net.DialTimeout("tcp", p.addr, 500*time.Millisecond)
	if err == nil {
		_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		_, err = fmt.Fprint(conn, b.String())
		_ = conn.Close()
	}
	if err == nil {
		p.lastSentPhase = phase
	}
	if err != nil && !p.warned {
		p.warned = true
		fmt.Fprintln(os.Stderr, "storectl: progress metric delivery failed:", err)
	}
}
