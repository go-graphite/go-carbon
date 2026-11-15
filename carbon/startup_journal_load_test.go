package carbon

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

// This opt-in gate drives real sockets, startup, cache, journal and persistence.
// Qualification requires an isolated Linux host and a private representative
// journal/config. Smoke mode tests the driver but can NEVER qualify a rollout.
func TestStartupJournalLoadGate(t *testing.T) {
	if os.Getenv("STARTUP_LOAD_GATE") != "1" {
		t.Skip("opt in with STARTUP_LOAD_GATE=1; see doc/startup-input-log-load.md")
	}
	// Run alone via -run: the general integration subprocess has a two-minute
	// timeout, shorter than this deliberately twenty-minute differential.
	smoke := os.Getenv("STARTUP_LOAD_SMOKE") == "1"
	rate, seconds, connections := int64(25_000_000), 600, 280
	if smoke {
		rate, seconds, connections = 10_000, 3, 32
	} else if runtime.GOOS != "linux" || os.Getenv("STARTUP_LOAD_ISOLATED") != "1" {
		t.Fatal("qualification requires an explicitly isolated non-production Linux host")
	}
	fixture, config := os.Getenv("STARTUP_LOAD_JOURNAL"), os.Getenv("STARTUP_LOAD_CONFIG")
	if !smoke && (fixture == "" || config == "") {
		t.Fatal("qualification requires representative journal and configuration paths")
	}
	output := os.Getenv("STARTUP_LOAD_OUTPUT")
	if output == "" {
		t.Fatal("STARTUP_LOAD_OUTPUT must name a retained JSONL evidence file")
	}
	f, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Sync(); _ = f.Close() }()
	evidence := json.NewEncoder(f)
	stamp := time.Now().Unix() - 30
	var reference string
	completed := 0
	for _, appendMode := range []bool{false, true} {
		name := "capture"
		if appendMode {
			name = "inputlog"
		}
		t.Run(name, func(t *testing.T) {
			digest := runStartupLoad(t, fixture, config, appendMode, smoke, rate, seconds, connections, stamp, evidence)
			if !appendMode {
				reference = digest
			} else if reference != "" && digest != reference {
				t.Fatalf("stored archive bits differ: capture=%s inputlog=%s", reference, digest)
			}
			completed++
		})
	}
	verdict := "PASS"
	if smoke {
		verdict = "SMOKE_ONLY_NOT_QUALIFIED"
	}
	if t.Failed() {
		verdict = "FAIL"
	} else if completed != 2 {
		verdict = "INCOMPLETE_NOT_QUALIFIED"
	}
	if err := evidence.Encode(map[string]any{"verdict": verdict, "archive_sha256": reference, "offered_points_per_second": rate, "seconds_per_path": seconds, "connections": connections, "goos": runtime.GOOS, "cpus": runtime.NumCPU(), "go_version": runtime.Version()}); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

func runStartupLoad(t *testing.T, fixture, config string, appendMode, smoke bool, rate int64, seconds, connections int, stamp int64, evidence *json.Encoder) string {
	t.Helper()
	path, cfg := sharedAppConfig(t)
	if config != "" {
		var err error
		cfg, err = ReadConfig(config)
		if err != nil {
			t.Fatal(err)
		}
	}
	root := t.TempDir()
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Tcp.BufferSize, cfg.Tcp.Compression = 0, ""
	cfg.Udp.Enabled, cfg.Pickle.Enabled, cfg.Grpc.Enabled = false, false, false
	cfg.Carbonlink.Enabled, cfg.Carbonserver.Enabled = false, false
	cfg.Pprof.Enabled, cfg.Prometheus.Enabled, cfg.Buckyd.Enabled = false, false, false
	cfg.Common.MetricInterval = &Duration{time.Hour} // only this sampler consumes TCP interval counters
	// Legacy early capture requires tag support disabled; canonical names
	// keep this throughput differential on the common configuration. Alias
	// canonicalization has its own bit-exact journal regression.
	cfg.Tags.Enabled = false
	cfg.Receiver = nil
	cfg.Logging = nil
	cfg.Whisper.StorageBackend, cfg.Whisper.StoreEncoding = "pebble-chunk", "exact"
	cfg.Whisper.StoreMirrorFiles, cfg.Whisper.StoreIncrementalMigration = false, false
	cfg.Whisper.StoreDir, cfg.Whisper.DataDir = filepath.Join(root, "store"), filepath.Join(root, "files")
	cfg.Whisper.SchemasFilename = filepath.Join(root, "schemas.conf")
	// Both sequential trials retain the identical frozen timestamps. Expiry
	// must not turn an archive differential into a comparison of empty stores.
	if err := os.WriteFile(cfg.Whisper.SchemasFilename, []byte("[load]\npattern = .*\nretentions = 1s:1d,1m:7d\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Dump.Enabled, cfg.Dump.StoreInputHandoff = true, true
	cfg.Dump.Path = filepath.Join(root, "dump")
	cfg.InputLog.Enabled, cfg.InputLog.StartupAppend = true, appendMode
	cfg.InputLog.Path = filepath.Join(root, "journal")
	if smoke {
		cfg.InputLog.SegmentSize = 16 << 20
		cfg.Whisper.Workers = 4
		cfg.Common.MaxCPU = 4
	}
	if err := os.MkdirAll(cfg.Dump.Path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cfg.InputLog.Path, 0700); err != nil {
		t.Fatal(err)
	}
	var fixtureBytes int64
	if fixture == "" {
		l, err := inputlog.Open(cfg.InputLog.Path, inputlog.Options{SegmentSize: 1 << 20, SyncInterval: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		l.StartAppend()
		var batch inputlog.Batch
		batch.Append(inputlog.NewStreamRef("load.history"), math.Float64frombits(0x8000000000000000), stamp)
		if _, err = l.Append(0, &batch); err != nil {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		entries, err := filepath.Glob(filepath.Join(fixture, "*.log"))
		if err != nil {
			t.Fatal(err)
		}
		for _, source := range entries {
			in, err := os.Open(source)
			if err != nil {
				t.Fatal(err)
			}
			out, err := os.Create(filepath.Join(cfg.InputLog.Path, filepath.Base(source)))
			if err != nil {
				in.Close()
				t.Fatal(err)
			}
			n, err := io.Copy(out, in)
			in.Close()
			closeErr := out.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("copy journal: %v %v", err, closeErr)
			}
			fixtureBytes += n
		}
		if !smoke && fixtureBytes < 1_400_000_000 {
			t.Fatalf("journal fixture is too small: %d bytes", fixtureBytes)
		}
	}

	// Count the retained historical points before live appends can inflate the
	// replay counter. This copy is private; the source fixture is read-only.
	inventory, err := inputlog.Open(cfg.InputLog.Path, inputlog.Options{SegmentSize: cfg.InputLog.SegmentSize, SyncInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	historical, replayErr := inventory.Replay(func(*points.Points) {})
	closeErr := inventory.Close()
	if replayErr != nil || closeErr != nil || historical.Corrupt != 0 {
		t.Fatalf("invalid historical fixture: %+v %v %v", historical, replayErr, closeErr)
	}
	if !smoke && historical.Points < 100_000_000 {
		t.Fatalf("historical fixture too small: %d points", historical.Points)
	}
	if err := evidence.Encode(map[string]any{"append": appendMode, "phase": "inventory", "fixture_bytes": fixtureBytes, "historical_points": historical.Points}); err != nil {
		t.Fatal(err)
	}
	writeSharedConfig(t, path, cfg)
	app := New(path)
	if err := app.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	// Prebuild cyclic native frames before startup, with disjoint names per connection. This
	// preserves same-name correction order across the two independent trials.
	wires := make([][2][8][]byte, connections)
	for id := range connections {
		for extra := range 2 {
			n := int(rate/int64(connections)/10) + extra
			for phase := range 8 {
				e, err := codec.NewEncoder(codec.Options{})
				if err != nil {
					t.Fatal(err)
				}
				if err := e.Reset(); err != nil {
					t.Fatal(err)
				}
				wire, err := codec.AppendPreamble(nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				for i := range n {
					bits := []uint64{0, 1 << 63, 1, 0x3ff0000000000001, 0x7ff8000000000042, 0x7ff0000000000000, 0xfff0000000000000, 0x4000000000000001}
					name := fmt.Sprintf("load.c%03d.m%04d;a=first;z=last", id, i%256)
					if err := e.Add([]byte(name), math.Float64frombits(bits[(i+phase)%len(bits)]), stamp+int64(i/256%8)); err != nil {
						t.Fatal(err)
					}
				}
				wire, err = e.Flush(wire)
				e.Close()
				if err != nil {
					t.Fatal(err)
				}
				wires[id][extra][phase] = wire
			}
		}
	}

	started := make(chan error, 1)
	go func() { started <- app.Start() }()
	var ready bool
	defer func() {
		if !ready {
			select {
			case <-started:
			case <-time.After(30 * time.Second):
				t.Error("App.Start did not join; private test process must exit")
				return
			}
		}
		app.Stop()
	}()
	deadline := time.Now().Add(2 * time.Minute)
	var s *startupInput
	for s == nil && time.Now().Before(deadline) {
		s = app.startupCaptureState.Load()
		select {
		case err := <-started:
			ready = true
			if err != nil {
				t.Fatal(err)
			}
			if s == nil {
				t.Fatal("startup completed without early capture")
			}
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if s == nil {
		t.Fatal("startup did not expose durable capture before load")
	}
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for range connections {
		c, err := net.DialTimeout("tcp", s.receiver.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	if prefix := os.Getenv("STARTUP_LOAD_PROFILE"); prefix != "" {
		writeWaitProfile := func(suffix string) {
			out, err := os.Create(fmt.Sprintf("%s-%t.%s.block.pprof", prefix, appendMode, suffix))
			if err != nil {
				t.Error(err)
				return
			}
			defer out.Close()
			if err := pprof.Lookup("block").WriteTo(out, 0); err != nil {
				t.Error(err)
			}
		}
		writeWaitProfile("before")
		defer writeWaitProfile("after")
		profile, err := os.Create(fmt.Sprintf("%s-%t.cpu.pprof", prefix, appendMode))
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(profile); err != nil {
			profile.Close()
			t.Fatal(err)
		}
		defer func() { pprof.StopCPUProfile(); profile.Close() }()
	}

	var sent atomic.Int64
	stop := make(chan struct{})
	failures := make(chan error, connections)
	var wg sync.WaitGroup
	defer func() {
		close(stop)
		for _, c := range conns {
			c.Close()
		}
		wg.Wait()
	}()
	start := time.Now().Add(100 * time.Millisecond)
	for id, c := range conns {
		wg.Go(func() {
			for tick := range seconds * 10 {
				at := start.Add(time.Duration(tick) * 100 * time.Millisecond)
				select {
				case <-stop:
					return
				case <-time.After(time.Until(at)):
				}
				// Due counts come from the original clock, never from completion of
				// a previous write. A slow consumer cannot lower the offered load.
				n := rate / 10 / int64(connections)
				if int64(id) < (rate/10)%int64(connections) {
					n++
				}
				extra := int(n - rate/int64(connections)/10)
				wire := wires[id][extra][tick%8]
				if tick > 0 {
					wire = wire[codec.PreambleSize:]
				}
				writeBudget := 100 * time.Millisecond
				if smoke {
					writeBudget = 10 * time.Second
				}
				_ = c.SetWriteDeadline(at.Add(writeBudget))
				if written, err := c.Write(wire); err != nil || written != len(wire) {
					failures <- fmt.Errorf("socket write %d/%d: %v", written, len(wire), err)
					return
				}
				sent.Add(n)
			}
		})
	}
	var intake, receiverErrors int64
	sample := func(name string, value float64) {
		switch name {
		case "metricsReceived":
			intake += int64(value)
		case "errors", "parseErrors", "streamDecodeErrors":
			receiverErrors += int64(value)
		}
	}
	var previousIntake, previousCommitted uint64
	wasDirect := false
	for second := 1; second <= seconds; second++ {
		time.Sleep(time.Until(start.Add(time.Duration(second) * time.Second)))
		s.receiver.Stat(sample)
		select {
		case err := <-started:
			if err != nil {
				t.Fatal(err)
			}
			ready = true
		default:
		}
		committed := uint64(0)
		if ready {
			committed = app.Persister.CommittedPoints()
		}
		var m inputlog.Metrics
		if log := app.inputLog.Load(); log != nil {
			m = log.Metrics()
		}
		timings := s.ioTimings()
		row := map[string]any{"append": appendMode, "second": second, "offered": rate * int64(second), "sent": sent.Load(), "intake": intake, "committed": committed, "direct": s.direct.Load(), "relogged": m.Relogged, "write_errors": m.WriteErrors, "receiver_errors": receiverErrors, "startup_points": s.points.Load(), "fixture_bytes": fixtureBytes,
			"replayed": m.ReplayedPoints, "startup_replayed": s.replayedPoints.Load(), "journal_points": m.PointsWritten, "journal_bytes": m.BytesWritten, "journal_syncs": m.Syncs, "unlogged": m.Unlogged, "append_calls": timings[captureAppend].calls, "append_p99_upper_ns": timings[captureAppend].p99, "append_total_ns": timings[captureAppend].total, "append_max_ns": timings[captureAppend].maximum}
		if err := evidence.Encode(row); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-failures:
			t.Fatalf("producer backpressured: %v", err)
		default:
		}
		if !smoke && (sent.Load() < rate*int64(second) || intake < rate*int64(second)) {
			t.Fatalf("offered load missed at second %d: %v", second, row)
		}
		if m.Relogged != 0 || m.WriteErrors != 0 || receiverErrors != 0 || s.paused.Load() {
			t.Fatalf("durability/relog gate failed: %v", row)
		}
		if !smoke && wasDirect && committed-previousCommitted < uint64(intake)-previousIntake {
			t.Fatalf("commits behind intake after direct: %v", row)
		}
		previousCommitted, previousIntake, wasDirect = committed, uint64(intake), s.direct.Load()
	}
	wg.Wait()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	if sent.Load() != rate*int64(seconds) {
		t.Fatalf("producer lost points: sent=%d", sent.Load())
	}
	if !ready {
		if err := <-started; err != nil {
			t.Fatal(err)
		}
		ready = true
	}
	for _, c := range conns {
		c.Close()
	}
	deadline = time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		s.receiver.Stat(sample)
		if intake == sent.Load() && s.direct.Load() && app.Cache.IsEmpty() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if receiverErrors != 0 || intake != sent.Load() || !s.direct.Load() || !app.Cache.IsEmpty() || app.Persister.CommittedPoints() < uint64(intake) {
		t.Fatal("direct/persistence did not drain")
	}
	if appendMode && app.inputLog.Load().Metrics().PointsWritten < s.points.Load() {
		t.Fatal("startup points were not written to the input log")
	}
	if !smoke && s.points.Load() == 0 {
		t.Fatal("load did not exercise startup capture before direct")
	}
	if appendMode && s.replayedPoints.Load() != historical.Points+s.points.Load() {
		t.Fatalf("startup replay count differs from fixture plus captured intake: replayed=%d historical=%d captured=%d", s.replayedPoints.Load(), historical.Points, s.points.Load())
	}
	if appendMode {
		captures, err := filepath.Glob(filepath.Join(cfg.Dump.Path, "*.capture-*.tail.bin"))
		if err != nil || len(captures) != 0 {
			t.Fatalf("new mode created capture spools: %v %v", captures, err)
		}
	}
	if err := evidence.Encode(map[string]any{"append": appendMode, "phase": "drained", "direct": s.direct.Load(), "sent": sent.Load(), "intake": intake, "committed": app.Persister.CommittedPoints(), "startup_points": s.points.Load(), "replayed": app.inputLog.Load().Metrics().ReplayedPoints, "relogged": app.inputLog.Load().Metrics().Relogged, "max_cpu": cfg.Common.MaxCPU, "historical_replayed": historical.Points}); err != nil {
		t.Fatal(err)
	}
	if err := app.MetricStore.Sync(); err != nil {
		t.Fatal(err)
	}
	// Hash every stored metric/archive using timestamp and IEEE bits, never JSON
	// float formatting or a sample of names. Store identities may differ.
	h := sha256.New()
	after := ""
	var buf [8]byte
	var liveNames, livePoints int
	for {
		page, err := app.MetricStore.ListPage(context.Background(), "", after, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, metadata := range page {
			binary.LittleEndian.PutUint64(buf[:], uint64(len(metadata.Name)))
			h.Write(buf[:])
			io.WriteString(h, metadata.Name)
			snapshot, err := app.MetricStore.Snapshot(context.Background(), metadata.Name)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(metadata.Name, "load.c") {
				liveNames++
			}
			binary.LittleEndian.PutUint64(buf[:], uint64(len(snapshot.Archives)))
			h.Write(buf[:])
			for _, archive := range snapshot.Archives {
				for _, n := range []int{archive.Retention.Step, archive.Retention.Count, len(archive.Points)} {
					binary.LittleEndian.PutUint64(buf[:], uint64(n))
					h.Write(buf[:])
				}
				if strings.HasPrefix(metadata.Name, "load.c") {
					livePoints += len(archive.Points)
				}
				for _, point := range archive.Points {
					binary.LittleEndian.PutUint64(buf[:], uint64(point.Timestamp))
					h.Write(buf[:])
					binary.LittleEndian.PutUint64(buf[:], math.Float64bits(point.Value))
					h.Write(buf[:])
				}
			}
			after = metadata.Name
		}
	}
	if liveNames != int(min(rate/10, int64(connections)*256)) || livePoints == 0 {
		t.Fatalf("incomplete live archive differential: names=%d want=%d points=%d", liveNames, int(min(rate/10, int64(connections)*256)), livePoints)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if err := evidence.Encode(map[string]any{"append": appendMode, "phase": "archive", "archive_sha256": digest, "live_names": liveNames, "live_points": livePoints}); err != nil {
		t.Fatal(err)
	}
	return digest
}
