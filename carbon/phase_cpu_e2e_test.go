//go:build linux || darwin

package carbon

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"
	tcp "github.com/go-graphite/go-carbon/receiver/tcp"
)

// Opt-in so ordinary unit tests do not spend thirty seconds generating load.
// PHASE_CPU_E2E=on enables the profiler; off runs the identical control workload.
func TestPhaseCPUEndToEnd(t *testing.T) {
	mode := os.Getenv("PHASE_CPU_E2E")
	if mode == "" {
		t.Skip("set PHASE_CPU_E2E=on or off for local CPU accounting and overhead measurement")
	}
	path, cfg := sharedAppConfig(t)
	cfg.Common.MaxCPU = 2
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Carbonserver.ScanFrequency = &Duration{time.Minute}
	cfg.Whisper.Workers = 2
	cfg.Whisper.MaxUpdatesPerSecond = 0
	cfg.Whisper.StoreMetadataCacheSize = 4 << 20
	cfg.Whisper.StoreMemTableSize = 4 << 20
	cfg.Cache.MaxSize = 2_000_000
	cfg.Cache.WriteoutMinPoints = 128
	cfg.Cache.WriteoutMaxDelay = Duration{100 * time.Millisecond}
	if mode == "on" {
		cfg.Common.PhaseCPUProfileInterval = &Duration{30 * time.Second}
		cfg.Common.PhaseCPUProfileDuration = &Duration{500 * time.Millisecond}
	}
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	defer app.Stop()
	var addr string
	for _, r := range app.Receivers {
		if p, ok := r.Receiver.(*tcp.TCP); ok {
			addr = p.Addr().String()
		}
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// One prebuilt block, reused; both transport and cache memory stay bounded.
	var lines strings.Builder
	for i := range 4096 {
		fmt.Fprintf(&lines, "phase.metric.%d %d %d\n", i%256, i%13, time.Now().Unix())
	}
	block := []byte(lines.String())
	deadline := time.Now().Add(32 * time.Second)
	_ = conn.SetWriteDeadline(deadline)
	var before, after syscall.Rusage
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var count uint64
	for time.Now().Before(deadline) {
		n, writeErr := conn.Write(block)
		count += uint64(n/len(block)) * 4096
		if writeErr != nil {
			var e net.Error
			if errors.As(writeErr, &e) && e.Timeout() {
				break
			}
			t.Fatal(writeErr)
		}
	}
	if err = syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
		t.Fatal(err)
	}
	cpu := float64(after.Utime.Sec-before.Utime.Sec+after.Stime.Sec-before.Stime.Sec) + float64(after.Utime.Usec-before.Utime.Usec+after.Stime.Usec-before.Stime.Usec)/1e6
	var dropped float64
	app.Cache.Stat(func(name string, value float64) {
		if name == "overflow" {
			dropped = value
		}
	})
	t.Logf("cache_overflow_points=%.0f", dropped)
	families, err := app.PromRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started).Seconds()
	var estimated, observed, coverage, windows, accounted float64
	for _, f := range families {
		switch f.GetName() {
		case "go_carbon_phase_cpu_seconds_total":
			for _, m := range f.Metric {
				v := m.GetCounter().GetValue()
				estimated += v
				if v > 0 {
					t.Logf("phase=%s estimated_cpu_s=%.6f", m.Label[0].GetValue(), v)
				}
			}
		case "go_carbon_phase_sampled_cpu_seconds_total":
			for _, m := range f.Metric {
				observed += m.GetCounter().GetValue()
			}
		case "go_carbon_phase_cpu_profile_seconds_total":
			coverage = f.Metric[0].GetCounter().GetValue()
		case "go_carbon_phase_cpu_accounted_seconds_total":
			accounted = f.Metric[0].GetCounter().GetValue()
		case "go_carbon_phase_cpu_profile_windows_total":
			windows = f.Metric[0].GetCounter().GetValue()
		}
	}
	rateRatio := 0.0
	if accounted > 0 {
		rateRatio = (estimated / accounted) / (cpu / elapsed)
	}
	t.Logf("RESULT mode=%s wall_s=%.6f process_cpu_s=%.6f points=%d cpu_ns_per_point=%.3f estimated_cpu_s=%.6f sampled_cpu_s=%.6f coverage_s=%.6f accounted_s=%.6f windows=%.0f cpu_rate_ratio=%.4f", mode, elapsed, cpu, count, cpu*1e9/float64(count), estimated, observed, coverage, accounted, windows, rateRatio)
	if mode == "on" {
		if windows < 1 || coverage < .5*phasecpu.Window.Seconds() {
			t.Fatalf("insufficient profiling coverage: %.3fs %.0f windows", coverage, windows)
		}
		// Statistical estimate, not normalized to the independently measured CPU.
		if ratio := rateRatio; ratio < .65 || ratio > 1.35 {
			t.Fatalf("phase estimate / process CPU = %.3f", ratio)
		}
	}
}
