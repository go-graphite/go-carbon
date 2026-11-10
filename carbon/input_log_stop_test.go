package carbon

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
)

func TestInputLogOnlyConfig(t *testing.T) {
	if NewConfig().Dump.InputLogOnly {
		t.Fatal("experimental stop enabled by default")
	}
	valid := func() *Config {
		c := NewConfig()
		c.Dump.InputLogOnly, c.Dump.Enabled = true, false
		c.Dump.Path = t.TempDir()
		c.InputLog.Enabled, c.InputLog.BackgroundReplay = true, true
		c.Dump.HandoffSupervisor, c.Dump.StoreInputHandoff = true, true
		c.Whisper.StorageBackend = "pebble-chunk"
		c.Tags.Enabled, c.Udp.Enabled, c.Pickle.Enabled = false, false, false
		c.Carbonserver.Enabled, c.Tcp.Enabled = true, true
		return c
	}
	if err := validateInputLogOnlyConfig(valid()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"blocking replay":   func(c *Config) { c.InputLog.BackgroundReplay = false },
		"no log":            func(c *Config) { c.InputLog.Enabled = false },
		"no supervisor":     func(c *Config) { c.Dump.HandoffSupervisor = false },
		"no socket handoff": func(c *Config) { c.Dump.StoreInputHandoff = false },
		"mirrored store":    func(c *Config) { c.Whisper.StoreMirrorFiles = true },
		"tags":              func(c *Config) { c.Tags.Enabled = true },
		"mixed receivers":   func(c *Config) { c.Udp.Enabled = true },
	} {
		t.Run(name, func(t *testing.T) {
			c := valid()
			mutate(c)
			if err := validateInputLogOnlyConfig(c); err == nil {
				t.Fatal("unsafe journal-only configuration accepted")
			}
		})
	}
}

func TestInputLogOnlyRequiresRestart(t *testing.T) {
	old, next := NewConfig(), NewConfig()
	next.Dump.InputLogOnly = true
	if err := inputLogReloadError(old, next); err == nil {
		t.Fatal("changed recovery mode through reload")
	}
}

// A single hot metric can fill live staging after its initial snapshot. Socket
// detach must account for those cached-but-unlogged arrivals before transfer.
func TestInputLogOnlyHandoffRepairsStagingOverflow(t *testing.T) {
	core := cache.New()
	defer core.Stop()
	core.EnableInputLog()
	dir := t.TempDir()
	log, err := inputlog.Open(dir, inputlog.Options{SyncInterval: time.Hour, SegmentSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	log.Start(core, cache.ShardCount)
	defer log.Close()
	core.Add(points.OnePoint("hot", -1, -1))
	if err := log.Relog(); err != nil {
		t.Fatal(err)
	}
	startDrops := core.LogDropped()
	data := make([]points.Point, 1<<18)
	for i := range data {
		data[i] = points.Point{Value: float64(i) + .125, Timestamp: int64(i)}
	}
	core.Add(&points.Points{Metric: "hot", Data: data})
	if core.LogDropped() == startDrops {
		t.Fatal("fixture did not overflow live staging after the first snapshot")
	}
	h := &inputHandover{journal: log, logger: zap.NewNop(), logDropped: core.LogDropped, startDrops: startDrops}
	if _, err := h.completeTail(); err != nil {
		t.Fatal(err)
	}
	reader, err := inputlog.Open(dir, inputlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var got []points.Point
	var mu sync.Mutex
	stats, err := reader.Replay(func(p *points.Points) {
		mu.Lock()
		got = append(got, p.Data...)
		mu.Unlock()
	})
	want := append([]points.Point{{Value: -1, Timestamp: -1}}, data...)
	if err != nil || stats.Corrupt != 0 || stats.Points != int64(len(want)) || !slices.Equal(got, want) {
		t.Fatalf("overflow recovery: points=%d want=%d stats=%+v err=%v", len(got), len(want), stats, err)
	}
}
