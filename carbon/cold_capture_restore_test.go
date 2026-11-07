//go:build unix

package carbon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
	"github.com/zeebo/xxh3"
)

func TestColdCaptureCrashProcess(t *testing.T) {
	tail := os.Getenv("REPLAY_FIX_COLD_CRASH_TAIL")
	if tail == "" {
		return
	}
	stamp, err := strconv.ParseInt(os.Getenv("REPLAY_FIX_COLD_CRASH_STAMP"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	s, err := openStartupInputWithPolicy(tail, chooseStartupPolicy(2, 256<<20, 1<<30))
	if err != nil {
		t.Fatal(err)
	}
	data := make([]points.Point, 10000)
	for i := range data {
		data[i] = points.Point{Value: math.Float64frombits(0x4000000000000000 + uint64(i)), Timestamp: stamp + int64(i%16)}
	}
	if err := s.shards[0].Write((&points.Points{Metric: "handoff.metric", Data: data}).AppendBinary(nil), len(data)); err != nil {
		t.Fatal(err)
	}
	if err := s.shards[0].sync(); err != nil {
		t.Fatal(err)
	}
	os.Exit(99) // no capture teardown; the OS releases the orphan epoch's locks
}

func TestColdStartupAdoptsCaptureBeforeStorage(t *testing.T) {
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
	cfg.Common.MaxCPU, cfg.Cache.MaxSize = 4, 0
	cfg.Whisper.OutOfOrder = true
	writeSharedConfig(t, path, cfg)
	old := startIncrementalApp(t, path)
	if !old.Carbonserver.HasMappedIndex() {
		old.Stop()
		old = startIncrementalApp(t, path)
	}
	old.Persister.Stop()
	old.Persister = nil
	const metric = "handoff.metric"
	stamp := time.Now().Unix() - 60
	data := make([]points.Point, 50000)
	for i := range data {
		data[i] = points.Point{Value: math.Float64frombits(0x3ff0000000000000 + uint64(i)), Timestamp: stamp + int64(i%32)}
	}
	old.Cache.Add(&points.Points{Metric: metric, Data: data})
	if err := old.DumpStop(); err != nil {
		t.Fatal(err)
	}
	old.Stop()
	next := New(path)
	if err := next.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	defer next.Stop()
	// A plain cold restart without orphaned capture keeps the ordinary path.
	if err := next.startStartupInput(); err != nil || next.startupInput != nil {
		t.Fatal("ordinary cold start unexpectedly enabled capture", err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(cfg.Dump.Path, recovery.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest recovery.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	previous, err := openStartupInputWithPolicy(filepath.Join(cfg.Dump.Path, recovery.TailName(manifest.WAL.Name)), chooseStartupPolicy(2, 256<<20, 1<<30))
	if err != nil {
		t.Fatal(err)
	}
	if err := previous.close(); err != nil {
		t.Fatal(err)
	}
	if err := next.startStartupInput(); err != nil || next.startupInput != nil {
		t.Fatal("empty orphaned segments unexpectedly enabled capture", err)
	}
	older := make([]points.Point, 10000)
	for i := range older {
		older[i] = points.Point{Value: math.Float64frombits(0x4000000000000000 + uint64(i)), Timestamp: stamp + int64(i%16)}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestColdCaptureCrashProcess$")
	child.Env = append(os.Environ(), "REPLAY_FIX_COLD_CRASH_TAIL="+filepath.Join(cfg.Dump.Path, recovery.TailName(manifest.WAL.Name)), "REPLAY_FIX_COLD_CRASH_STAMP="+strconv.FormatInt(stamp, 10))
	out, err := child.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 99 {
		t.Fatalf("capture crash child: %v\n%s", err, out)
	}
	// Refuse the optional index, exercising the production ordered fallback.
	index := filepath.Join(cfg.Dump.Path, manifest.Index.Name)
	bytes, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	bytes[len(bytes)/2] ^= 0x40
	if err := os.WriteFile(index, bytes, 0600); err != nil {
		t.Fatal(err)
	}
	next.Config.InputLog.Enabled = true
	next.Config.InputLog.Path = filepath.Join(t.TempDir(), "journal")
	next.Config.InputLog.SyncInterval, next.Config.InputLog.MaxAge = Duration{time.Hour}, Duration{4 * time.Hour}
	next.Config.InputLog.SegmentSize = 16 << 20
	if err := next.startStartupInput(); err != nil {
		t.Fatal(err)
	}
	s := next.startupInput
	if s == nil || next.Cache != nil || next.MetricStore != nil || next.handoffClaim != nil {
		t.Fatal("cold capture waited for storage or required an inherited claim")
	}
	if err := next.startStartupInput(); err != nil || next.startupInput != s {
		t.Fatal("early startup capture is not idempotent", err)
	}
	newer := make([]points.Point, 1000)
	for i := range newer {
		newer[i] = points.Point{Value: math.Float64frombits(0x4008000000000000 + uint64(i)), Timestamp: stamp + int64(i%16)}
	}
	shard := s.shards[xxh3.HashString(metric)%uint64(len(s.shards))]
	if err := shard.Write((&points.Points{Metric: metric, Data: newer}).AppendBinary(nil), len(newer)); err != nil {
		t.Fatal(err)
	}
	// Newly opened TCP intake also captures before any cache exists.
	conn, err := net.Dial("tcp", s.receiver.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(conn, "tcp.only 42 %d\n", stamp)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return s.points.Load() == int64(len(newer)+1) })
	if err := next.Start(); err != nil {
		t.Fatal(err, zapwriter.TestString())
	}
	awaitCapture(t, func() bool { return s.replayedPoints.Load() == int64(len(newer)+1) && next.Cache.IsEmpty() })
	refPath, refCfg := incrementalAppFixture(t)
	refCfg.Whisper.OutOfOrder = true
	writeSharedConfig(t, refPath, refCfg)
	ref := startIncrementalApp(t, refPath)
	defer ref.Stop()
	ref.Cache.Add(&points.Points{Metric: metric, Data: data})
	ref.Cache.Add(&points.Points{Metric: metric, Data: older})
	ref.Cache.Add(&points.Points{Metric: metric, Data: newer})
	ref.Cache.Add(points.OnePoint("tcp.only", 42, stamp))
	awaitCapture(t, ref.Cache.IsEmpty)
	for _, name := range []string{metric, "tcp.only"} {
		actual, err := next.MetricStore.Snapshot(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := ref.MetricStore.Snapshot(context.Background(), name)
		if err != nil || !reflect.DeepEqual(actual.Archives, expected.Archives) {
			t.Fatal("cold capture/replay changed archive bits", name, err)
		}
	}
	t.Log("cold orphaned epoch replayed in parallel before new epoch; fresh TCP captured before storage; archives match")
}
