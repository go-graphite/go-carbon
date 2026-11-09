//go:build unix

package carbon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/lomik/zapwriter"
)

// Delay all storage/index work after the real input-only offer. More than the
// default RAM read-ahead budget must still drain durably on each connection.
func TestStartupInputDrainsBeforeCheckpoint(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	short, err := os.MkdirTemp("/tmp", "hs-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(short)
	cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format, cfg.Dump.StoreInputHandoff = true, short, "compact", true
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Common.MaxCPU = 4
	writeSharedConfig(t, path, cfg)
	old := startIncrementalApp(t, path)
	if !old.Carbonserver.HasMappedIndex() {
		old.Stop()
		old = startIncrementalApp(t, path)
	}
	address := old.Receivers[0].Receiver.(*tcp.TCP).Addr().String()
	nextCfg := *cfg
	// This test measures durable input capture, not cache pressure. Leave
	// room for the conservative unpublished shard count during tail replay.
	nextCfg.Cache.MaxSize = handoffRecoveryCacheSize
	nextTCP := *cfg.Tcp
	nextTCP.Listen = address
	nextCfg.Tcp = &nextTCP
	next := New(path)
	next.Config = &nextCfg
	defer next.Stop()
	captured := make(chan *startupInput, 1)
	start := make(chan struct{})
	release := sync.OnceFunc(func() { close(start) })
	defer release()
	started := make(chan error, 1)
	old.SetReadHandoff(storeHandoffSuccessor(t, true), func() {
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
	handoff.PeerExecutable = func(*net.UnixConn) (string, error) { return storeHandoffSuccessor(t, true), nil }
	defer func() { handoff.PeerExecutable = saved }()
	stopped := make(chan error, 1)
	go func() { stopped <- old.DumpStop() }()
	var spool *startupInput
	select {
	case spool = <-captured:
	case err := <-started:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("no early capture")
	}
	if spool == nil || next.Cache != nil || next.MetricStore != nil {
		t.Fatal("capture waited for storage")
	}
	const clients, records = 2, 100000
	var writers sync.WaitGroup
	for client := range clients {
		writers.Go(func() {
			conn, err := net.Dial("tcp", address)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			var data []byte
			for seq := range records {
				data = fmt.Appendf(data, "client%d.%s %d 1700000000\n", client, strings.Repeat("longsegment.", 12)+"value", seq)
			}
			if len(data) <= 8<<20 {
				t.Error("fixture must exceed per-connection RAM budget")
			}
			if _, err := conn.Write(data); err != nil {
				t.Error(err)
			}
		})
	}
	writers.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for spool.points.Load() < clients*records {
		if time.Now().After(deadline) {
			t.Fatal("input did not drain before checkpoint", spool.points.Load())
		}
		time.Sleep(time.Millisecond)
	}
	counts := map[string]int{}
	if _, err := replayInputTailReader(io.NewSectionReader(spool.file, 0, spool.size.Load()), 1, func(p *points.Points) {
		for _, d := range p.Data {
			if int(d.Value) != counts[p.Metric] {
				t.Error("capture lost, duplicated, or reordered points")
			}
			counts[p.Metric]++
		}
	}); err != nil {
		t.Fatal(err)
	}
	if len(counts) != clients {
		t.Fatal(counts)
	}
	release()
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err, zapwriter.TestString())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("startup did not catch up")
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}

func startupPoints(stamp int64, phase int) []*points.Points {
	switch phase {
	case 0:
		return []*points.Points{points.OnePoint("handoff.metric", 1, stamp-3).Add(2, stamp-2)}
	case 1:
		return []*points.Points{points.OnePoint("handoff.metric", 3, stamp-2).Add(4, stamp-1), points.OnePoint("tail.only", 11, stamp-1)}
	default:
		return []*points.Points{points.OnePoint("handoff.metric", 5, stamp-2).Add(6, stamp-1).Add(7, stamp), points.OnePoint("tail.only", 12, stamp)}
	}
}

// Crash-only recovery uses names already in this incremental fixture's file
// index. Its Buckyd/catalog refresher is disabled; discovering brand-new catalog
// names is separate from journal durability. The successor oracle covers those.
func predecessorPoints(stamp int64, phase int) []*points.Points {
	ps := startupPoints(stamp, phase)
	for _, p := range ps {
		if p.Metric == "handoff.metric" {
			p.Metric = "first"
		} else {
			p.Metric = "second"
		}
	}
	return ps
}

// The child exits without running any teardown: this is a process crash, not a
// simulated clean Close. The parent then uses the real App.Start recovery.
func TestStartupInputCrashProcess(t *testing.T) {
	stage := os.Getenv("HANDOFF_CRASH_STAGE")
	if stage == "" {
		return
	}
	stamp, _ := strconv.ParseInt(os.Getenv("HANDOFF_CRASH_STAMP"), 10, 64)
	s, err := openStartupInput(os.Getenv("HANDOFF_CRASH_TAIL"))
	if err != nil {
		t.Fatal(err)
	}
	if stage == "before_append" {
		os.Exit(99)
	}
	var raw []byte
	n := 0
	for _, p := range startupPoints(stamp, 1) {
		raw = p.AppendBinary(raw)
		n += len(p.Data)
	}
	if stage == "during_append" {
		// Model SIGKILL during the final write: all earlier records are
		// complete; the last point's timestamp is torn.
		if _, err := s.file.Write(raw[:len(raw)-1]); err != nil {
			t.Fatal(err)
		}
		if err := s.file.Sync(); err != nil {
			t.Fatal(err)
		}
		os.Exit(99)
	}
	if err := s.Write(raw, n); err != nil {
		t.Fatal(err)
	}
	if stage == "after_write" {
		os.Exit(99)
	}
	if err := s.file.Sync(); err != nil {
		t.Fatal(err)
	}
	if stage == "after_sync" {
		os.Exit(99)
	}
	if stage == "during_replay" {
		core := cache.New()
		if _, err := replayInputTailReader(io.NewSectionReader(s.file, 0, s.size.Load()), 8, core.AddRecovered); err != nil {
			t.Fatal(err)
		}
		os.Exit(99)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if stage == "after_seal" {
		os.Exit(99)
	}
	app := New(os.Getenv("HANDOFF_CRASH_CONFIG"))
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	app.Persister.Stop()
	for _, p := range startupPoints(stamp, 2) {
		app.Cache.Add(p)
	}
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	os.Exit(99)
}

func TestPredecessorCrashProcess(t *testing.T) {
	stage := os.Getenv("HANDOFF_OLD_CRASH_STAGE")
	if stage == "" {
		return
	}
	stamp, _ := strconv.ParseInt(os.Getenv("HANDOFF_CRASH_STAMP"), 10, 64)
	app := startIncrementalApp(t, os.Getenv("HANDOFF_CRASH_CONFIG"))
	if !app.Carbonserver.HasMappedIndex() {
		app.Stop()
		app = startIncrementalApp(t, os.Getenv("HANDOFF_CRASH_CONFIG"))
	}
	app.Persister.Stop()
	app.Persister = nil
	for phase := range 2 {
		for _, p := range predecessorPoints(stamp, phase) {
			app.Cache.Add(p)
		}
	}
	if err := app.syncInputLog(); err != nil {
		t.Fatal(err)
	}
	if stage == "before_dump" {
		os.Exit(99)
	}
	if stage == "during_warm" {
		checkpointWarmHook = func() { os.Exit(99) }
	}
	if stage == "during_dump" {
		checkpointDumpHook = func() {
			fmt.Println("CRASH_READY")
			_ = syscall.Kill(os.Getpid(), syscall.SIGSTOP)
		}
	}
	if err := app.DumpStop(); err != nil {
		t.Fatal(err)
	}
	if stage == "during_dump" {
		t.Fatal("dump completed before crash injection")
	}
	os.Exit(99)
}

func TestPredecessorCrashKeepsLoggedInput(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, stage := range []string{"before_dump", "during_warm", "during_dump", "after_dump"} {
		t.Run(stage, func(t *testing.T) {
			defer zapwriter.Test()()
			path, cfg := incrementalAppFixture(t)
			cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, t.TempDir(), "legacy"
			cfg.Whisper.OutOfOrder = true
			cfg.Cache.MaxSize = 0
			// A crash without a checkpoint discovers log-only names through the
			// next ordinary scan; do not use the fixture's one-hour scan period.
			cfg.Carbonserver.ScanFrequency = &Duration{100 * time.Millisecond}
			cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "log")
			cfg.InputLog.SegmentSize = 16 << 20
			writeSharedConfig(t, path, cfg)
			stamp := time.Now().Unix() - 10
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPredecessorCrashProcess$")
			cmd.Env = append(os.Environ(), "HANDOFF_OLD_CRASH_STAGE="+stage, "HANDOFF_CRASH_STAMP="+strconv.FormatInt(stamp, 10), "HANDOFF_CRASH_CONFIG="+path)
			if stage == "during_dump" {
				output, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				defer cmd.Process.Kill()
				scanner := bufio.NewScanner(output)
				ready := false
				for scanner.Scan() {
					if scanner.Text() == "CRASH_READY" {
						ready = true
						break
					}
				}
				if !ready {
					t.Fatal("child never entered dump", scanner.Err())
				}
				if _, err := os.Stat(filepath.Join(cfg.Dump.Path, recovery.ManifestName)); !os.IsNotExist(err) {
					t.Fatal("checkpoint was already published", err)
				}
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				if err := cmd.Wait(); err == nil {
					t.Fatal("child did not crash")
				}
			} else {
				out, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 99 {
					t.Fatalf("child: %v\n%s", err, out)
				}
			}
			app := startIncrementalApp(t, path)
			want := map[string]map[int64]float64{}
			for phase := range 2 {
				for _, p := range predecessorPoints(stamp, phase) {
					if want[p.Metric] == nil {
						want[p.Metric] = map[int64]float64{}
					}
					for _, d := range p.Data {
						want[p.Metric][d.Timestamp] = d.Value
					}
				}
			}
			for name, values := range want {
				deadline := time.Now().Add(10 * time.Second)
				for {
					got := readSeriesOrEmpty(t, app.Carbonserver.HTTPListener().Addr().String(), name, stamp-4, stamp+1)
					if reflect.DeepEqual(got, values) {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("logged input lost after", stage, name, got, values)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		})
	}
}

func TestStartupInputCrashArchivesMatchUninterrupted(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	for _, stage := range []string{"before_append", "during_append", "after_write", "after_sync", "during_replay", "after_seal", "after_live"} {
		t.Run(stage, func(t *testing.T) {
			defer zapwriter.Test()()
			path, cfg := incrementalAppFixture(t)
			cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, t.TempDir(), "compact"
			cfg.Whisper.OutOfOrder = true
			if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:2m,5s:10m,30s:1h\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "log")
			cfg.InputLog.SegmentSize = 16 << 20
			writeSharedConfig(t, path, cfg)
			old := startIncrementalApp(t, path)
			if !old.Carbonserver.HasMappedIndex() {
				old.Stop()
				old = startIncrementalApp(t, path)
			}
			old.Persister.Stop()
			old.Persister = nil
			stamp := time.Now().Unix() - 10
			for _, p := range startupPoints(stamp, 0) {
				old.Cache.Add(p)
			}
			if err := old.DumpStop(); err != nil {
				t.Fatal(err)
			}
			old.Stop()
			raw, err := os.ReadFile(filepath.Join(cfg.Dump.Path, recovery.ManifestName))
			if err != nil {
				t.Fatal(err)
			}
			var manifest recovery.Manifest
			if err := json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			tail := filepath.Join(cfg.Dump.Path, recovery.TailName(manifest.WAL.Name))
			if err := os.WriteFile(tail, nil, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupInputCrashProcess$")
			cmd.Env = append(os.Environ(), "HANDOFF_CRASH_STAGE="+stage, "HANDOFF_CRASH_STAMP="+strconv.FormatInt(stamp, 10), "HANDOFF_CRASH_TAIL="+tail, "HANDOFF_CRASH_CONFIG="+path)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 99 {
				t.Fatalf("crash child: %v\n%s", err, out)
			}
			actual := New(path)
			if err := actual.ParseConfig(); err != nil {
				t.Fatal(err)
			}
			if err := actual.Start(); err != nil {
				t.Fatal(err, zapwriter.TestString())
			}
			defer actual.Stop()
			if stage != "after_live" {
				for _, p := range startupPoints(stamp, 2) {
					actual.Cache.Add(p)
				}
			}
			// The oracle writes the same logical sequence without any restart.
			refPath, refCfg := incrementalAppFixture(t)
			refCfg.Whisper.OutOfOrder = true
			if err := os.WriteFile(refCfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:2m,5s:10m,30s:1h\n"), 0600); err != nil {
				t.Fatal(err)
			}
			writeSharedConfig(t, refPath, refCfg)
			ref := startIncrementalApp(t, refPath)
			defer ref.Stop()
			for phase := range 3 {
				if phase == 1 && stage == "before_append" {
					continue
				}
				for _, p := range startupPoints(stamp, phase) {
					if phase == 1 && stage == "during_append" && p.Metric == "tail.only" {
						continue
					}
					ref.Cache.Add(p)
				}
			}
			for _, app := range []*App{actual, ref} {
				deadline := time.Now().Add(5 * time.Second)
				for !app.Cache.IsEmpty() || app.Cache.PendingOutstanding() != 0 {
					if time.Now().After(deadline) {
						t.Fatal("recovery did not drain", zapwriter.TestString())
					}
					time.Sleep(time.Millisecond)
				}
			}
			for _, name := range []string{"handoff.metric", "tail.only"} {
				want, err := ref.MetricStore.Snapshot(context.Background(), name)
				if err != nil {
					t.Fatal(err)
				}
				got, err := actual.MetricStore.Snapshot(context.Background(), name)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got.Archives, want.Archives) {
					t.Fatalf("%s archives differ after %s:\ngot %+v\nwant %+v", name, stage, got.Archives, want.Archives)
				}
				listen := actual.Carbonserver.HTTPListener().Addr().String()
				values := readSeries(t, listen, name, stamp-4, stamp+1)
				refValues := readSeries(t, ref.Carbonserver.HTTPListener().Addr().String(), name, stamp-4, stamp+1)
				if !reflect.DeepEqual(values, refValues) {
					t.Fatal("read oracle differs", values, refValues)
				}
			}
		})
	}
}
