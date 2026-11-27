package carbon

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	"github.com/prometheus/client_golang/prometheus"
)

func v2FallbackTestApp(t *testing.T, root string) *App {
	a := v2TestApp(t, root)
	a.Config.Carbonserver.ScanFrequency = &Duration{20 * time.Millisecond}
	a.Config.Carbonserver.RealtimeIndex = 1024
	a.Config.Carbonserver.ConcurrentIndex = true
	a.Config.Carbonserver.QueryCacheEnabled = false
	a.Config.Carbonserver.QueryRangeCacheEnabled = false
	a.Config.Carbonserver.StreamingQueryCacheEnabled = false
	return a
}

func TestV2FallbackModes(t *testing.T) {
	for _, from := range []string{v2Shadow, v2ServingDual, v2Only} {
		for _, to := range []string{v2Shadow, v2ServingDual, v2Only} {
			t.Run(from+"/"+to, func(t *testing.T) {
				root := t.TempDir()
				a := v2FallbackTestApp(t, root)
				setV2Mode(a, from)
				if err := a.Start(); err != nil {
					t.Fatal(err)
				}
				wantDual := from != v2Only
				if (a.Persister != nil) != wantDual {
					t.Fatalf("classic persister mode %s", from)
				}
				if a.v2.mode != from {
					t.Fatal(a.v2.mode)
				}
				before := a.Config
				if err := a.ReloadConfig(); err == nil {
					t.Fatal("live reload unexpectedly accepted")
				}
				if a.Config != before || a.v2.mode != from {
					t.Fatal("failed reload changed active state")
				}
				a.Stop()
				a = v2FallbackTestApp(t, root)
				setV2Mode(a, to)
				err := a.Start()
				if from == v2Only && to != v2Only {
					if err == nil {
						a.Stop()
						t.Fatal("stale classic fallback accepted")
					}
				} else if err != nil {
					t.Fatal(err)
				}
				a.Stop()
			})
		}
	}
}

func TestV2FallbackReadsClassicDuringShadow(t *testing.T) {
	root := t.TempDir()
	a := v2FallbackTestApp(t, root)
	setV2Mode(a, v2Shadow)
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	defer a.Stop()
	now := time.Now().Unix()
	stamp := now - now%60 - 120
	conn, err := net.Dial("tcp", a.v2.server.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintf(conn, "fallback.metric 42 %d\n", stamp); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(a.Config.Whisper.DataDir, "fallback", "metric.wsp")); err == nil && a.Cache.IsEmpty() {
			time.Sleep(100 * time.Millisecond)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("classic never received journaled input")
		}
		time.Sleep(20 * time.Millisecond)
	}
	path := fmt.Sprintf("/render/?format=json&target=fallback.metric&from=%d&until=%d", stamp-60, stamp)
	if b := v2HTTP(t, a, path); !bytes.Contains(b, []byte("42")) {
		t.Fatalf("classic render: %s", b)
	}
	if _, err := a.v2.reader.Fetch(context.Background(), "fallback.metric", int(stamp-60), int(stamp)); err != nil {
		t.Fatal(err)
	}
	// Give classic a deliberately different value: shadow must actually serve it.
	committed := a.Persister.CommittedPoints()
	a.Cache.AddRestoredBeforeInput(points.OnePoint("fallback.metric", 99, stamp))
	deadline = time.Now().Add(5 * time.Second)
	for !a.Cache.IsEmpty() || a.Persister.CommittedPoints() <= committed {
		if time.Now().After(deadline) {
			t.Fatal("classic update did not drain")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if b := v2HTTP(t, a, path); !bytes.Contains(b, []byte("99")) {
		t.Fatalf("shadow served v2 instead of classic: %s", b)
	}
}

func TestV2FallbackMetrics(t *testing.T) {
	for _, mode := range []string{v2Shadow, v2ServingDual, v2Only} {
		r := prometheus.NewRegistry()
		r.MustRegister(&v2ModeMetrics{mode: mode})
		families, err := r.Gather()
		if err != nil {
			t.Fatal(err)
		}
		active := 0
		for _, family := range families {
			if family.GetName() == "go_carbon_storage_mode" {
				for _, m := range family.Metric {
					if m.GetGauge().GetValue() == 1 {
						active++
						if m.Label[0].GetValue() != mode {
							t.Fatal(m)
						}
					}
				}
			}
		}
		if active != 1 {
			t.Fatalf("%s active=%d", mode, active)
		}
	}
}

func TestV2FallbackTargetAndHistoryGuards(t *testing.T) {
	t.Run("target", func(t *testing.T) {
		root := t.TempDir()
		a := v2FallbackTestApp(t, root)
		setV2Mode(a, v2Shadow)
		if err := a.Start(); err != nil {
			t.Fatal(err)
		}
		a.Stop()
		a = v2FallbackTestApp(t, root)
		setV2Mode(a, v2Shadow)
		a.Config.Whisper.DataDir = filepath.Join(root, "different")
		if err := a.Start(); err == nil {
			a.Stop()
			t.Fatal("different classic target accepted")
		}
		a.Stop()
	})
	t.Run("history", func(t *testing.T) {
		root := t.TempDir()
		a := v2FallbackTestApp(t, root)
		setV2Mode(a, v2Shadow)
		if err := os.MkdirAll(a.Config.V2.Path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(a.Config.V2.Path, "old-history"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := a.Start(); err == nil {
			a.Stop()
			t.Fatal("untracked history accepted")
		}
		a.Stop()
	})
	t.Run("unknown-mode", func(t *testing.T) {
		a := v2FallbackTestApp(t, t.TempDir())
		setV2Mode(a, "typo")
		if err := a.Start(); err == nil || !strings.Contains(err.Error(), "unknown v2.mode") {
			a.Stop()
			t.Fatal(err)
		}
	})
}

func TestV2FallbackSIGKILL(t *testing.T) {
	for _, mode := range []string{v2Shadow, v2ServingDual} {
		t.Run(mode, func(t *testing.T) { testV2RuntimeSIGKILLWithActiveTCP(t, mode) })
	}
}

// Opt-in fixture used by the campaign's process CPU/RSS/disk-I/O sampler.
func TestV2FallbackCostFixture(t *testing.T) {
	mode := os.Getenv("V2_FALLBACK_MEASURE_MODE")
	if mode == "" {
		t.Skip("opt-in cost fixture")
	}
	root := t.TempDir()
	a := v2FallbackTestApp(t, root)
	setV2Mode(a, mode)
	a.Config.Carbonserver.ScanFrequency = &Duration{time.Hour}
	if mode == "classic" {
		a.Config.V2 = v2Config{}
	}
	a.Config.Common.MaxCPU = 2
	a.Config.Common.MetricInterval = &Duration{time.Hour}
	a.Config.Common.HeartbeatInterval = &Duration{0}
	a.Config.Cache.WriteStrategy = "max"
	a.Config.Cache.WriteoutMinPoints = 0
	a.Config.Cache.WriteoutMaxDelay = Duration{0}
	rets, err := persister.ParseRetentionDefs("60s:2h,3600s:1d")
	if err != nil {
		t.Fatal(err)
	}
	a.Config.Whisper.Schemas[0].Retentions = rets
	a.Config.Whisper.Workers = 2
	a.Config.Whisper.MaxUpdatesPerSecond = 0
	// A known loopback port works for either ordinary or v2 TCP receivers.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a.Config.Tcp.Listen = l.Addr().String()
	_ = l.Close()
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", a.Config.Tcp.Listen)
	if err != nil {
		a.Stop()
		t.Fatal(err)
	}
	now := time.Now().Unix()
	stamp := now - now%60 - 3600
	var input bytes.Buffer
	for i := 0; i < 128; i++ {
		for j := 0; j < 60; j++ {
			fmt.Fprintf(&input, "fixture.metric%04d %d %d\n", i, j, stamp+int64(j*60))
		}
	}
	if _, err := conn.Write(input.Bytes()); err != nil {
		_ = conn.Close()
		a.Stop()
		t.Fatal(err)
	}
	_ = conn.Close()
	deadline := time.Now().Add(60 * time.Second)
	for {
		accepted := false
		if mode == "classic" {
			accepted = a.Persister.CommittedPoints() >= 7680
		} else {
			accepted = a.v2.server.Stats().Accepted == 7680 && (mode == v2Only || a.Persister.CommittedPoints() >= 7680)
		}
		if accepted && a.Cache.IsEmpty() {
			break
		}
		if time.Now().After(deadline) {
			committed, cached := uint64(0), a.Cache.Size()
			if a.Persister != nil {
				committed = a.Persister.CommittedPoints()
			}
			a.Stop()
			t.Fatalf("fixture did not drain: committed=%d cache=%d", committed, cached)
		}
		time.Sleep(20 * time.Millisecond)
	}
	a.Stop()
	var disk int64
	if err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			disk += info.Size()
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("mode=%s points=7680 logical_bytes=%d", mode, disk)
	ready := os.Getenv("V2_FALLBACK_MEASURE_READY")
	if ready != "" {
		if err := os.WriteFile(ready, []byte(mode), 0600); err != nil {
			t.Fatal(err)
		}
		deadline = time.Now().Add(60 * time.Second)
		for {
			if _, err := os.Stat(ready + ".release"); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("sampler release timeout")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestV2FallbackRejectsUnseededRollupCutover(t *testing.T) {
	root := t.TempDir()
	a := v2FallbackTestApp(t, root)
	setV2Mode(a, v2Shadow)
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	stamp := now - now%60 - 120
	conn, err := net.Dial("tcp", a.v2.server.Addr().String())
	if err != nil {
		a.Stop()
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(conn, "unseeded.metric 5 %d\n", stamp)
	_ = conn.Close()
	if err != nil {
		a.Stop()
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for a.v2.server.Stats().Accepted == 0 || !a.Cache.IsEmpty() {
		if time.Now().After(deadline) {
			a.Stop()
			t.Fatal("shadow did not drain")
		}
		time.Sleep(20 * time.Millisecond)
	}
	a.Stop()
	a = v2FallbackTestApp(t, root)
	setV2Mode(a, v2ServingDual)
	err = a.Start()
	if err == nil {
		a.Stop()
		t.Fatal("unseeded multiarchive cutover accepted")
	}
	a.Stop()
	if !strings.Contains(err.Error(), "seeded rollup archives") {
		t.Fatal(err)
	}
}
