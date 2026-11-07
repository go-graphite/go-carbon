//go:build unix

package carbon

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/tcp"
	"github.com/klauspost/compress/zstd"
	"github.com/lomik/zapwriter"
	"github.com/zeebo/xxh3"
)

// A retained capture epoch plus a refused pending index takes the exact path
// observed on the canary. Keep the log timer an hour away: bounded restore
// staging must request commit itself before the successor can start replay.
func TestOrderedRestoreWithRetainedCapturePressure(t *testing.T) {
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
	cfg.Dump.Enabled, cfg.Dump.Path, cfg.Dump.Format = true, short, "compact"
	cfg.Dump.StoreInputHandoff = true
	cfg.Tcp.Enabled, cfg.Tcp.Listen = true, "127.0.0.1:0"
	cfg.Common.MaxCPU = 4
	cfg.Whisper.OutOfOrder = true
	cfg.Cache.MaxSize = 0
	writeSharedConfig(t, path, cfg)
	old := startIncrementalApp(t, path)
	if !old.Carbonserver.HasMappedIndex() {
		old.Stop()
		old = startIncrementalApp(t, path)
	}
	defer old.Stop()
	old.Persister.Stop()
	old.Persister = nil
	const metric, restored, captured = "handoff.metric", 500000, 100000
	stamp := time.Now().Unix() - 30
	data := make([]points.Point, restored)
	for i := range data {
		data[i] = points.Point{Value: math.Float64frombits(0x3ff0000000000000 + uint64(i)), Timestamp: stamp + int64(i%16)}
	}
	old.Cache.Add(&points.Points{Metric: metric, Data: data})
	next := New(path)
	if err := next.ParseConfig(); err != nil {
		t.Fatal(err)
	}
	next.Config.Tcp.Listen = old.Receivers[0].Receiver.(*tcp.TCP).Addr().String()
	next.Config.InputLog.Enabled = true
	next.Config.InputLog.Path = filepath.Join(t.TempDir(), "journal")
	next.Config.InputLog.SyncInterval = Duration{time.Hour}
	next.Config.InputLog.MaxAge = Duration{4 * time.Hour}
	next.Config.InputLog.SegmentSize = 16 << 20
	defer next.Stop()
	claimed := make(chan *startupInput, 1)
	release := make(chan struct{})
	start := sync.OnceFunc(func() { close(release) })
	defer start()
	ready := make(chan error, 1)
	old.SetReadHandoff(storeHandoffSuccessor(t, true), func() {
		go func() {
			next.RegisterReadHandoff()
			next.beginRestart()
			if err := next.startStartupInput(); err != nil {
				ready <- err
				return
			}
			claimed <- next.startupInput
			<-release
			ready <- next.Start()
		}()
	})
	saved := handoff.PeerExecutable
	handoff.PeerExecutable = func(*net.UnixConn) (string, error) { return storeHandoffSuccessor(t, true), nil }
	defer func() { handoff.PeerExecutable = saved }()
	stopped := make(chan error, 1)
	go func() { stopped <- old.DumpStop() }()
	var spool *startupInput
	select {
	case spool = <-claimed:
	case err := <-ready:
		t.Fatal(err, zapwriter.TestString())
	case <-time.After(10 * time.Second):
		t.Fatal("input handoff did not reach capture", zapwriter.TestString())
	}
	if spool == nil {
		t.Fatal("handoff did not start early capture")
	}
	// A previous process's closed epoch is adopted ahead of current input.
	// It need not share the successor's partition count.
	batch := points.OnePoint(metric, 2, stamp)
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	older := fmt.Sprintf("%s.capture-epoch-%020d.%06d.%012d.tail.bin", spool.path, spool.epoch-1, 0, 0)
	frame := recovery.EncodeCaptureFrame(encoder, batch.AppendBinary(nil), nil, 1)
	encoder.Close()
	if err := os.WriteFile(older, frame, 0600); err != nil {
		t.Fatal(err)
	}
	for base := 0; base < captured; base += 1000 {
		var raw []byte
		for i := base; i < base+1000; i++ {
			batch.Data[0] = points.Point{Value: math.Float64frombits(0x4000000000000000 + uint64(i)), Timestamp: stamp + int64(i%16)}
			raw = batch.AppendBinary(raw)
		}
		sh := spool.shards[xxh3.HashString(metric)%uint64(len(spool.shards))]
		if err := sh.Write(raw, 1000); err != nil {
			t.Fatal(err)
		}
	}
	// Corrupt only the optional read accelerator; authoritative dump and
	// capture sources remain intact and ordinary ordered restore must win.
	manifestBytes, err := os.ReadFile(filepath.Join(cfg.Dump.Path, recovery.ManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest recovery.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(cfg.Dump.Path, manifest.Index.Name)
	bytes, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	bytes[len(bytes)/2] ^= 0x40
	if err := os.WriteFile(index, bytes, 0600); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	start()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err, zapwriter.TestString())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ordered restore waited on log timer", zapwriter.TestString())
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	awaitCapture(t, func() bool { return spool.replayedPoints.Load() == captured && next.Cache.IsEmpty() })
	if !strings.Contains(zapwriter.TestString(), "using ordered restore") {
		t.Fatal("fixture did not refuse the checkpoint accelerator")
	}
	actual, err := next.MetricStore.Snapshot(context.Background(), metric)
	if err != nil {
		t.Fatal(err)
	}
	refPath, refCfg := incrementalAppFixture(t)
	refCfg.Whisper.OutOfOrder = true
	writeSharedConfig(t, refPath, refCfg)
	ref := startIncrementalApp(t, refPath)
	defer ref.Stop()
	ref.Cache.Add(&points.Points{Metric: metric, Data: data})
	ref.Cache.Add(points.OnePoint(metric, 2, stamp))
	for i := range captured {
		ref.Cache.Add(points.OnePoint(metric, math.Float64frombits(0x4000000000000000+uint64(i)), stamp+int64(i%16)))
	}
	awaitCapture(t, ref.Cache.IsEmpty)
	expected, err := ref.MetricStore.Snapshot(context.Background(), metric)
	if err != nil || !reflect.DeepEqual(actual.Archives, expected.Archives) {
		t.Fatal("retained capture/ordered restore changed archives", err)
	}
	t.Logf("restored=%d older_capture=1 current_capture=%d total_restore_replay=%v", restored, captured, time.Since(started))
}
