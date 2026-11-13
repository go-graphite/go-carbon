package downsize

import (
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// progressObserver receives the exact snapshots published to progress JSON.
// Installed only by store-copy before workers start; guarded by outputMu.
var progressObserver func(string, progressSnapshot)

// MainWithMetrics exports controller progress through a local Graphite receiver.
// Delivery is bounded and never changes the transfer result or retry policy.
func MainWithMetrics(args []string) error {
	addr := os.Getenv("STORE_COPY_GRAPHITE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:2003"
	}
	host, _ := os.Hostname()
	operation := "unknown"
	if len(args) > 0 {
		operation = args[0]
	}
	// One bounded identity per invocation, shared by nested sync copy operations.
	id := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", host, os.Getpid(), time.Now().UnixNano()))))[:12]
	p := &progressMetrics{addr: addr, prefix: "carbon.store_copy." + metricToken(host) + "." + id + "." + metricToken(operation), phase: "starting", active: 1}
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
	warned                    bool
}

func (p *progressMetrics) observe(path string, snapshot progressSnapshot) {
	p.mu.Lock()
	// The parent publishes before any nested copy starts. Keep its identity.
	if p.path == "" {
		p.path = path
	}
	if p.path != path {
		p.mu.Unlock()
		return
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
		fmt.Fprintln(os.Stderr, "store-copy: progress metric delivery failed:", err)
	}
}
