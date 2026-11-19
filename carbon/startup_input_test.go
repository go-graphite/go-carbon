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
	"github.com/zeebo/xxh3"
)

// Delay all storage/index work after the real input-only offer. More than the
// default RAM read-ahead budget must still drain durably on each connection.
func TestStartupInputDrainsBeforeCheckpoint(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	path, cfg := incrementalAppFixture(t)
	short, err := os.MkdirTemp(os.TempDir(), "h-")
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
	next := New(path)
	if err := next.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	nextCfg := *next.Config
	// This test measures capture, leaving room for unpublished shards.
	nextCfg.Cache.MaxSize = handoffRecoveryCacheSize
	nextTCP := *next.Config.Tcp
	nextTCP.Listen = address
	nextCfg.Tcp = &nextTCP
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
	case <-t.Context().Done():
		t.Fatal("early capture cancelled")
	}
	if spool == nil || next.Cache != nil || next.MetricStore != nil {
		t.Fatal("capture waited for storage")
	}
	// Longer valid metric segments keep each stream above the default 8MiB
	// RAM allowance with fewer points and durable capture batches.
	const clients, records = 2, 12000
	// Keep the legacy lookup path below macOS PATH_MAX during incremental routing.
	metricPath := strings.Repeat(strings.Repeat("longsegment", 8)+".", 8)
	stamp := time.Now().Unix() - 5
	// Drive the actual plaintext receiver in process. The socket transfer is
	// exercised above; kernel loopback throughput must not decide whether the
	// capture sink drains before storage opens. Each reader still supplies more
	// than the default 8MiB read-ahead allowance, in bounded reusable batches.
	var writers sync.WaitGroup
	for client := range clients {
		writers.Go(func() {
			name := fmt.Sprintf("client%d.%svalue", client, metricPath)
			reader := &startupPointReader{name: name, stamp: stamp, records: records}
			next.startupInput.receiver.HandleConnection(&startupReaderConn{Reader: reader})
			if reader.bytes <= 8<<20 {
				t.Error("fixture must exceed per-connection RAM budget", reader.bytes)
			}
		})
	}
	writers.Wait() // HandleConnection joins its final capture flush before return.
	if got := spool.points.Load(); got != clients*records {
		t.Fatal("input did not drain before checkpoint", got)
	}
	counts := map[string]int{}
	if err := visitCapture(spool, func(p *points.Points) {
		for _, d := range p.Data {
			if int(d.Value) != counts[p.Metric] {
				t.Error("capture lost, duplicated, or reordered points")
			}
			counts[p.Metric]++
		}
	}); err != nil {
		t.Fatal(err)
	}

	for name, count := range counts {
		if count != records {
			t.Fatalf("%s: captured %d, want %d", name, count, records)
		}
	}
	if len(counts) != clients {
		t.Fatal(counts)
	}
	// This case restarts the continuous queue, even when replay finishes
	// quickly. Keep the automatic direct switch from changing that path.
	spool.stopDirectSwitch()
	release()
	if err := awaitTestResult(t, "startup", started, testLogProgress); err != nil {
		t.Fatal(err, zapwriter.TestString())
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	// Restart the continuous queue again after the initial checkpoint may have
	// retired. The third process must inherit input while older epoch files
	// replay, preserving a correction captured before storage opens.
	third := New(path)
	third.Config = &nextCfg
	defer third.Stop()
	thirdCaptured := make(chan *startupInput, 1)
	thirdStart := make(chan struct{})
	thirdRelease := sync.OnceFunc(func() { close(thirdStart) })
	defer thirdRelease()
	thirdReady := make(chan error, 1)
	thirdReleased := make(chan struct{})
	next.SetReadHandoff(storeHandoffSuccessor(t, true), func() {
		close(thirdReleased)
		go func() {
			third.RegisterReadHandoff()
			third.beginRestart()
			if err := third.startStartupInput(); err != nil {
				thirdReady <- err
				return
			}
			thirdCaptured <- third.startupInput
			<-thirdStart
			thirdReady <- third.Start()
		}()
	})
	stoppedAgain := make(chan error, 1)
	go func() {
		err := next.DumpStop()
		if err == nil {
			select {
			case <-thirdReleased:
			default:
				err = fmt.Errorf("second stop did not request input handoff")
			}
		}
		if err != nil {
			thirdReady <- err
		}
		stoppedAgain <- err
	}()
	var thirdSpool *startupInput
	select {
	case thirdSpool = <-thirdCaptured:
	case err := <-thirdReady:
		t.Fatal(err)
	case <-t.Context().Done():
		t.Fatal("streaming successor cancelled", zapwriter.TestString())
	}
	if thirdSpool == nil {
		t.Fatal("streaming restart fell back to closed input")
	}
	name := "client0." + metricPath + "value"
	thirdSpool.receiver.HandleConnection(&startupReaderConn{Reader: strings.NewReader(fmt.Sprintf("%s 100001 %d\n", name, stamp))})
	if thirdSpool.points.Load() != 1 {
		t.Fatal("correction did not drain before checkpoint")
	}
	thirdRelease()
	if err := awaitTestResult(t, "second streaming restart", thirdReady, testLogProgress); err != nil {
		t.Fatal(err, zapwriter.TestString())
	}
	if err := <-stoppedAgain; err != nil {
		t.Fatal(err, zapwriter.TestString())
	}
	awaitTestProgress(t, "third replay", func() bool {
		return thirdSpool.replayedPoints.Load() == 1 && third.Cache.IsEmpty()
	}, func() any { return [2]int64{thirdSpool.replayedPoints.Load(), third.Cache.Size()} })
	snap, err := third.MetricStore.Snapshot(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, archive := range snap.Archives {
		for _, p := range archive.Points {
			if p.Timestamp == stamp {
				found = true
				if p.Value != 100001 {
					t.Fatal("second restart reverted a correction", p)
				}
			}
		}
	}
	if !found {
		t.Fatal("second restart lost the captured correction")
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
	if strings.HasPrefix(stage, "direct_") {
		startupDirectCrashChild(t, stage, stamp)
		return
	}
	s, err := openStartupInput(os.Getenv("HANDOFF_CRASH_TAIL"))
	if err != nil {
		t.Fatal(err)
	}
	if stage == "before_append" {
		os.Exit(99)
	}
	for i, p := range startupPoints(stamp, 1) {
		shard := s.shards[xxh3.HashString(p.Metric)%uint64(len(s.shards))]
		raw := p.AppendBinary(nil)
		if stage == "during_append" && i == 1 {
			// Write the journal frame, then emulate a crash before its final
			// byte reached disk. No deferred seal or cursor publication runs.
			if err := shard.Write(raw, len(p.Data)); err != nil {
				t.Fatal(err)
			}
			if err := shard.file.Truncate(shard.current.Load().size.Load() - 1); err != nil {
				t.Fatal(err)
			}
			if err := shard.sync(); err != nil {
				t.Fatal(err)
			}
			os.Exit(99)
		}
		if err := shard.Write(raw, len(p.Data)); err != nil {
			t.Fatal(err)
		}
	}
	if stage == "after_write" {
		os.Exit(99)
	}
	for _, shard := range s.shards {
		if err := shard.sync(); err != nil {
			t.Fatal(err)
		}
	}
	if stage == "after_sync" {
		os.Exit(99)
	}
	if stage == "during_replay" {
		core := cache.New()
		if err := visitCapture(s, core.AddRecovered); err != nil {
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
	if stage == "after_pending_retirement" {
		// Force the handoff source to retire while periodic WAL sync is
		// deliberately far away. A real crash must still preserve its points.
		deadline := time.Now().Add(5 * time.Second)
		for app.Cache.HasPendingRecovery() {
			if time.Now().After(deadline) {
				t.Fatal("pending checkpoint did not retire")
			}
			time.Sleep(time.Millisecond)
		}
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
	parallelTestMatrix(t) // Crash stages own independent stores and process globals.
	for _, stage := range []string{"before_dump", "during_warm", "during_dump", "after_dump"} {
		t.Run(stage, func(t *testing.T) {
			if isolatedTestProcess(t) {
				return
			}
			defer zapwriter.Test()()
			path, cfg := incrementalAppFixture(t)
			cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, t.TempDir(), "legacy"
			cfg.Whisper.OutOfOrder = true
			cfg.Cache.MaxSize = 0
			// A crash without a checkpoint discovers log-only names through the
			// next ordinary scan; do not use the fixture's one-hour scan period.
			cfg.Carbonserver.ScanFrequency = &Duration{100 * time.Millisecond}
			// Reads open while the journal replays. The poll below repeats one
			// render URL, which the query cache would answer for 60 s with
			// the first result, possibly taken before the replay finished.
			cfg.Carbonserver.QueryCacheEnabled = false
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
				var processExit *exec.ExitError
				if !errors.As(err, &processExit) || processExit.ExitCode() != 99 {
					t.Fatalf("child: %v\n%s", err, out)
				}
			}
			// Only the successor enables capture; leave predecessor crash hooks
			// on their ordinary dump path.
			cfg.Dump.StoreInputHandoff = true
			cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
			writeSharedConfig(t, path, cfg)
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
	parallelTestMatrix(t) // Crash stages own independent stores and process globals.
	// Isolate stages individually so their store I/O can overlap within the
	// shared process limit, including the subprocess that actually crashes.
	for _, stage := range []string{"before_append", "during_append", "after_write", "after_sync", "during_replay", "after_seal", "after_live", "after_pending_retirement", "direct_before_switch", "direct_during_seal", "direct_after_fence", "direct_after_switch", "direct_partial_retirement", "direct_full_retirement", "direct_next_epoch"} {
		t.Run(stage, func(t *testing.T) {
			if isolatedTestProcess(t) {
				return
			}
			defer zapwriter.Test()()
			path, cfg := incrementalAppFixture(t)
			cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, t.TempDir(), "compact"
			cfg.Whisper.OutOfOrder = true
			if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[all]\npattern = .*\nretentions = 1s:2m,5s:10m,30s:1h\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg.InputLog.Enabled, cfg.InputLog.Path = true, filepath.Join(t.TempDir(), "log")
			cfg.InputLog.SegmentSize = 16 << 20
			if stage == "after_pending_retirement" {
				cfg.Whisper.StoreSyncInterval = &Duration{time.Hour}
			}
			if strings.HasPrefix(stage, "direct_") {
				cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
				cfg.Dump.StoreInputHandoff = true
				cfg.InputLog.TruncateLag = Duration{0}
				primeStartupDirectIndex(t, cfg)
			}
			writeSharedConfig(t, path, cfg)
			old := startIncrementalApp(t, path)
			if !old.Carbonserver.HasMappedIndex() {
				old.Stop()
				old = startIncrementalApp(t, path)
			}
			if strings.HasPrefix(stage, "direct_") {
				convertStartupDirectIndex(t, old)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStartupInputCrashProcess$")
			cmd.Env = append(os.Environ(), "HANDOFF_CRASH_STAGE="+stage, "HANDOFF_CRASH_STAMP="+strconv.FormatInt(stamp, 10), "HANDOFF_CRASH_TAIL="+tail, "HANDOFF_CRASH_CONFIG="+path)
			out, err := cmd.CombinedOutput()
			var processExit *exec.ExitError
			if !errors.As(err, &processExit) || processExit.ExitCode() != 99 {
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
			if strings.HasPrefix(stage, "direct_") && actual.startupInput != nil {
				awaitCapture(t, func() bool { return allCaptureLive(actual.startupInput) })
			}
			if stage != "after_live" && stage != "after_pending_retirement" && !directCrashHasInput(stage) {
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
			if strings.HasPrefix(stage, "direct_") {
				primeStartupDirectIndex(t, refCfg)
			}
			writeSharedConfig(t, refPath, refCfg)
			ref := startIncrementalApp(t, refPath)
			defer ref.Stop()
			if strings.HasPrefix(stage, "direct_") {
				convertStartupDirectIndex(t, ref)
			}
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
			if stage == "direct_next_epoch" {
				for _, p := range directNextEpochPoints(stamp) {
					ref.Cache.Add(p)
				}
			}
			for _, app := range []*App{actual, ref} {
				awaitTestProgress(t, "crash recovery drain", func() bool {
					return app.Cache.IsEmpty() && app.Cache.PendingOutstanding() == 0
				}, func() any {
					cycles := int64(0)
					if log := app.inputLog.Load(); log != nil {
						cycles = log.Metrics().Cycles
					}
					return [3]int64{app.Cache.Size(), int64(app.Cache.PendingOutstanding()), cycles}
				})
			}
			for _, name := range []string{"handoff.metric", "tail.only"} {
				// Store confirmation can precede asynchronous publication of a
				// newly created name to the read index. Join that boundary before
				// comparing HTTP readback, especially under race-suite load.
				awaitCapture(t, func() bool {
					return actual.Carbonserver.MetricExists(name) && ref.Carbonserver.MetricExists(name)
				})
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

// startupReaderConn lets the real receiver consume deterministic input without
// OS socket buffers or wall-clock I/O deadlines. The reader owns no resources.
type startupReaderConn struct{ io.Reader }

func (*startupReaderConn) Write([]byte) (int, error)        { return 0, io.ErrClosedPipe }
func (*startupReaderConn) Close() error                     { return nil }
func (*startupReaderConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (*startupReaderConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (*startupReaderConn) SetDeadline(time.Time) error      { return nil }
func (*startupReaderConn) SetReadDeadline(time.Time) error  { return nil }
func (*startupReaderConn) SetWriteDeadline(time.Time) error { return nil }

type startupPointReader struct {
	name                string
	stamp               int64
	records, seq, bytes int
	offset              int
	buf                 []byte
}

func (r *startupPointReader) Read(dst []byte) (int, error) {
	if r.offset == len(r.buf) {
		r.buf, r.offset = r.buf[:0], 0
		if r.seq == r.records {
			return 0, io.EOF
		}
		for end := min(r.seq+256, r.records); r.seq < end; r.seq++ {
			r.buf = fmt.Appendf(r.buf, "%s %d %d\n", r.name, r.seq, r.stamp)
		}
	}
	n := copy(dst, r.buf[r.offset:])
	r.offset += n
	r.bytes += n
	return n, nil
}
