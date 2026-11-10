//go:build unix

package carbon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

func TestCheckpointPublicationFailureKeepsEarlyIntake(t *testing.T) {
	if isolatedTestProcessSerial(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	short, err := os.MkdirTemp(os.TempDir(), "h-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(short)
	cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, short, "compact"
	cfg.Dump.HandoffSupervisor, cfg.Dump.StoreInputHandoff, cfg.Dump.RequireStartupCapture = true, true, true
	cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "journal")
	cfg.Whisper.OutOfOrder = true
	cfg.Tcp.Enabled, cfg.Tcp.Listen, cfg.Common.MaxCPU = true, "127.0.0.1:0", 4
	if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	old := startIncrementalApp(t, path)
	if !old.Carbonserver.HasMappedIndex() {
		old.Stop()
		old = startIncrementalApp(t, path)
	}
	defer old.Stop()
	awaitCapture(t, func() bool {
		return old.Carbonserver.HasMappedIndex() && restartRecoveryReady(old.Cache, old.storeRestoreDone, old.metadataPrewarmGate, old.startupCaptureState.Load())
	})
	address := old.Receivers[0].Receiver.(*tcp.TCP).Addr().String()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	old.Persister.Stop()
	old.Persister = nil
	stamp := time.Now().Unix() - 3000
	before := make([]points.Point, 128)
	want := map[string]map[int64]float64{"before": {}, "after": {}}
	for i := range before {
		before[i] = points.Point{Timestamp: stamp + int64(i), Value: float64(i) + .125}
		want["before"][before[i].Timestamp] = before[i].Value
	}
	old.Cache.Add(&points.Points{Metric: "before", Data: before})
	probe := startupGenerationTestSuccessor(t)
	next := New(path)
	if err := next.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	next.Config.Tcp.Listen = address
	defer next.Stop()
	captured := make(chan *startupInput, 1)
	start := make(chan struct{})
	release := sync.OnceFunc(func() { close(start) })
	defer release()
	started := make(chan error, 1)
	old.SetReadHandoff(probe, func() {
		go func() {
			next.RegisterReadHandoff()
			next.beginRestart()
			if err := next.startStartupInput(); err != nil {
				started <- err
				return
			}
			captured <- next.startupInput
			<-start
			started <- next.Start()
		}()
	})
	saved := handoff.PeerExecutable
	handoff.PeerExecutable = func(*net.UnixConn) (string, error) { return probe, nil }
	defer func() { handoff.PeerExecutable = saved }()
	if err := os.Mkdir(filepath.Join(short, recovery.ManifestName), 0700); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- old.DumpStop() }()
	var spool *startupInput
	select {
	case spool = <-captured:
	case err := <-started:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("checkpoint publication failure blocked early intake", zapwriter.TestString())
	}
	if spool == nil || next.Cache != nil || next.MetricStore != nil {
		t.Fatal("fallback waited for storage/checkpoint restore")
	}
	const live = 1000
	var raw []byte
	for i := range live {
		metric, timestamp := "after", stamp+int64(i)
		if i%8 == 0 {
			// Captured corrections must follow the older cache dump, including
			// repeated corrections to the same metric/timestamp.
			metric, timestamp = "before", stamp+int64(i%len(before))
		}
		value := float64(i) + .25
		raw = fmt.Appendf(raw, "%s %g %d\n", metric, value, timestamp)
		want[metric][timestamp] = value
	}
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return spool.points.Load() == live })
	release()
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err, zapwriter.TestString())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ordered fallback did not complete", zapwriter.TestString())
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return next.Cache.IsEmpty() && spool.replayedPoints.Load() == live })
	for metric, expected := range want {
		awaitCapture(t, func() bool { return next.Carbonserver.MetricExists(metric) })
		got := readSeries(t, next.Carbonserver.HTTPListener().Addr().String(), metric, stamp-1, stamp+int64(live)+1)
		if len(got) != len(expected) {
			t.Fatalf("fallback lost %s points: %d/%d", metric, len(got), len(expected))
		}
		for timestamp, value := range expected {
			if got[timestamp] != value {
				t.Fatalf("fallback changed %s point at%d", metric, timestamp)
			}
		}
	}
	next.Stop()
	reopened := startIncrementalApp(t, path)
	awaitCapture(t, func() bool {
		// A cold capture owner starts finite older epochs asynchronously. An
		// initially empty cache is not proof that those sources were replayed.
		return (reopened.startupInput == nil || reopened.startupInput.historyReady.Load()) && reopened.Cache.IsEmpty()
	})
	for metric, expected := range want {
		snapshot, err := reopened.MetricStore.Snapshot(context.Background(), metric)
		if err != nil || len(snapshot.Archives) == 0 {
			t.Fatalf("closed-store readback %s: %v", metric, err)
		}
		data := snapshot.Archives[0].Points
		if len(data) != len(expected) {
			t.Fatalf("closed-store readback lost %s: %d/%d", metric, len(data), len(expected))
		}
		for i, point := range data {
			if value, ok := expected[point.Timestamp]; !ok || point.Value != value {
				t.Fatalf("closed-store readback changed %s point%d: %+v", metric, i, point)
			}
		}
	}
}

func startupGenerationTestSuccessor(t *testing.T) string {
	t.Helper()
	probe := filepath.Join(t.TempDir(), "successor")
	script := fmt.Sprintf(`#!/bin/sh
case "$1" in
-handoff-protocol) echo '%s';;
-input-handoff-protocol) echo '%s';;
-store-input-handoff-protocol) echo '%s';;
-check-startup-capture) echo '%s';;
-startup-generation-protocol) echo '%s';;
*) exit 2;;
esac
`, handoff.Protocol, handoff.InputProtocol, handoff.StoreInputProtocol, handoff.StartupCaptureProtocol, handoff.StartupGenerationProtocol)
	if err := os.WriteFile(probe, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return probe
}

func TestGenerationHintRefusalPreservesWriter(t *testing.T) {
	if isolatedTestProcessSerial(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, t.TempDir(), "compact"
	cfg.Dump.HandoffSupervisor, cfg.Dump.StoreInputHandoff, cfg.Dump.RequireStartupCapture = true, true, true
	cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "journal")
	cfg.Tcp.Enabled, cfg.Tcp.Listen, cfg.Common.MaxCPU = true, "127.0.0.1:0", 4
	writeSharedConfig(t, path, cfg)
	app := startIncrementalApp(t, path)
	if !app.Carbonserver.HasMappedIndex() {
		app.Stop()
		app = startIncrementalApp(t, path)
	}
	awaitCapture(t, func() bool {
		return app.Carbonserver.HasMappedIndex() && restartRecoveryReady(app.Cache, app.storeRestoreDone, app.metadataPrewarmGate, app.startupCaptureState.Load())
	})
	conn, err := net.Dial("tcp", app.Receivers[0].Receiver.(*tcp.TCP).Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writer, migration := app.Persister, app.incrementalMigration.Load()
	released := false
	app.SetReadHandoff(startupGenerationTestSuccessor(t), func() { released = true })
	if err := os.Mkdir(filepath.Join(cfg.Dump.Path, startupGenerationName), 0700); err != nil {
		t.Fatal(err)
	}
	err = app.DumpStop()
	if !errors.Is(err, ErrRestartRefused) || !strings.Contains(err.Error(), "prepare independent input generation") ||
		app.Persister != writer || app.incrementalMigration.Load() != migration || len(app.Receivers) != 1 || released {
		t.Fatalf("failed generation preparation changed incumbent ownership: %v", err)
	}
	stamp := time.Now().Unix() - 2
	if _, err := fmt.Fprintf(conn, "after.refusal 7.125 %d\n", stamp); err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool {
		snapshot, err := app.MetricStore.Snapshot(context.Background(), "after.refusal")
		if err != nil || len(snapshot.Archives) == 0 {
			return false
		}
		for _, p := range snapshot.Archives[0].Points {
			if p.Timestamp == stamp && p.Value == 7.125 {
				return true
			}
		}
		return false
	})
}
