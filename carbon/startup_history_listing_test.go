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
	for i := range partitions {
		path := fmt.Sprintf("%s.capture-epoch-%020d.%06d.%012d.tail.bin", s.path, s.epoch-1, i, 2)
		p := points.OnePoint(fmt.Sprintf("history.%d", i), math.Float64frombits(0x8000000000000000+uint64(i)), int64(1700000000+i))
		if err := os.WriteFile(path, recovery.EncodeCaptureFrame(encoder, p.AppendBinary(nil), nil, 1), 0600); err != nil {
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
	release() // model the predecessor finishing its source-close loop
	awaitCapture(t, s.historyReady.Load)
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
