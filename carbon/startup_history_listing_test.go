//go:build unix

package carbon

import (
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/handoff"
	"github.com/go-graphite/go-carbon/internal/recovery"
	"github.com/go-graphite/go-carbon/points"
	"github.com/klauspost/compress/zstd"
	"github.com/lomik/zapwriter"
)

func TestStartupHistoryListsEveryLockedOlderPartition(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, _ := captureFixture(t)
	core := cache.New()
	core.SetMaxSize(0)
	app := New("")
	app.Config = NewConfig()
	app.startupInput = s
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	var unlocks []func() error
	release := sync.OnceFunc(func() {
		for _, unlock := range unlocks {
			if err := unlock(); err != nil {
				t.Error(err)
			}
		}
	})
	defer release()
	const partitions = 256
	var historyBytes int64
	for i := range partitions {
		path := fmt.Sprintf("%s.capture-epoch-%020d.%06d.%012d.tail.bin", s.path, s.epoch-1, i, 2)
		p := points.OnePoint(fmt.Sprintf("history.%d", i), math.Float64frombits(0x8000000000000000+uint64(i)), int64(1700000000+i))
		frame := recovery.EncodeCaptureFrame(encoder, p.AppendBinary(nil), nil, 1)
		historyBytes += int64(len(frame))
		if err := os.WriteFile(path, frame, 0600); err != nil {
			t.Fatal(err)
		}
		if i == 52 || i == 133 || i == 160 {
			unlock, err := handoff.LockExclusive(path)
			if err != nil {
				t.Fatal(err)
			}
			unlocks = append(unlocks, unlock)
		}
	}
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return s.size.Load() > 0 || s.historyReady.Load() })
	early := false
	select {
	case <-s.legacyDone:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	if got := s.historyPendingBytes.Load(); got != historyBytes {
		t.Errorf("pending history bytes %d, want %d", got, historyBytes)
	}
	if got := s.historyPendingSegments.Load(); got != partitions {
		t.Errorf("pending history segments %d, want %d", got, partitions)
	}
	release() // model the predecessor finishing its source-close loop
	awaitCapture(t, s.historyReady.Load)
	if got := s.historyPendingBytes.Load(); got != 0 {
		t.Fatal("completed history bytes", got)
	}
	if got := s.historyPendingSegments.Load(); got != 0 {
		t.Fatal("completed history segments", got)
	}
	for i := range partitions {
		got := core.Get(fmt.Sprintf("history.%d", i))
		if len(got) != 1 || got[0].Timestamp != int64(1700000000+i) || math.Float64bits(got[0].Value) != 0x8000000000000000+uint64(i) {
			t.Fatalf("history partition %d skipped or changed: %v", i, got)
		}
	}
	if early {
		t.Fatal("history declared complete while an older partition remained locked")
	}
	t.Log("all 256 historical partitions loaded bit-exactly after waiting for locks on 52, 133, 160")
}

func TestStartupHistoryBacklogTracksAppliedFrames(t *testing.T) {
	if isolatedTestProcess(t) {
		return
	}
	defer zapwriter.Test()()
	s, _ := captureFixture(t)
	core := cache.New()
	core.SetMaxSize(2) // Legacy admission allows the first point, then blocks.
	app := New("")
	app.Config = NewConfig()
	app.startupInput = s
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	first := recovery.EncodeCaptureFrame(encoder, points.OnePoint("first.history", 1, 1).AppendBinary(nil), nil, 1)
	second := recovery.EncodeCaptureFrame(encoder, points.OnePoint("second.history", 2, 2).AppendBinary(nil), nil, 1)
	path := fmt.Sprintf("%s.capture-epoch-%020d.%06d.%012d.tail.bin", s.path, s.epoch-1, 0, 0)
	if err := os.WriteFile(path, append(first, second...), 0600); err != nil {
		t.Fatal(err)
	}
	app.finishStartupInput(core)
	awaitCapture(t, func() bool { return s.historyPoints.Load() == 1 })
	awaitCapture(t, func() bool { return s.historyPendingBytes.Load() == int64(len(second)) })
	if s.historyPendingSegments.Load() != 1 {
		t.Fatal("partly applied segment no longer pending")
	}
	if s.historyReady.Load() {
		t.Fatal("history completed while admission blocked")
	}
	core.SetMaxSize(0)
	awaitCapture(t, s.historyReady.Load)
	if s.historyPendingBytes.Load() != 0 || s.historyPendingSegments.Load() != 0 {
		t.Fatal("completed history remains pending")
	}
}
