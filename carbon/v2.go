package carbon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2index"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

// v2Config is restart-only. The experimental engine has its own mandatory log
// underneath Path, independent of the classic dump/input-log switches.
type v2Config struct {
	Enabled         bool   `toml:"enabled"`
	Path            string `toml:"path"`
	Producers       int    `toml:"producers"`
	Shards          int    `toml:"shards"`
	ReadBudgetBytes int64  `toml:"read-budget-bytes"`
}
type v2Runtime struct {
	store   *v2daemon.Store
	reader  *v2read.Reader
	catalog v2index.Catalog
	server  *v2recv.Server
	stop    chan struct{}
	wg      sync.WaitGroup
}

func emptyV2ClassicRoot(path string) error {
	if path == "" {
		return nil
	}
	entries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("v2 requires an empty classic root: %s; mixed-source reads are not implemented", path)
	}
	return nil
}
func (app *App) startV2() error {
	c := app.Config
	if c.Common.MaxCPU > 0 {
		runtime.GOMAXPROCS(c.Common.MaxCPU)
	}
	if c.V2.Path == "" || !filepath.IsAbs(c.V2.Path) {
		return errors.New("v2.path must be absolute")
	}
	if c.Tcp == nil || c.Udp == nil || c.Pickle == nil {
		return errors.New("v2 requires receiver settings")
	}
	if !c.Tcp.Enabled || !c.Carbonserver.Enabled || c.Udp.Enabled || c.Pickle.Enabled || c.Carbonlink.Enabled || c.Grpc.Enabled || len(c.Receiver) > 0 || c.Buckyd.Enabled || c.Dump.Enabled || c.InputLog.Enabled {
		return errors.New("v2 currently requires TCP and carbonserver only; disable other receivers, buckyd, dump and classic input-log")
	}
	if len(c.Whisper.Quotas) > 0 || c.Whisper.StoreExpiration.Value() > 0 || c.Whisper.StoreExpirationFilename != "" {
		return errors.New("v2 lifecycle activation requires portable incarnation/reset and host quota publication; not implemented")
	}
	for _, path := range []string{c.Whisper.DataDir, c.Whisper.StoreDir} {
		if path != "" && (filepath.Clean(path) == filepath.Clean(c.V2.Path) || stringsOverlap(path, c.V2.Path)) {
			return errors.New("v2 path overlaps classic root")
		}
		if err := emptyV2ClassicRoot(path); err != nil {
			return err
		}
	}
	if c.Whisper.Aggregation == nil || len(c.Whisper.Schemas) == 0 {
		return errors.New("v2 requires loaded schema and aggregation policies")
	}
	n := c.V2.Producers
	if n == 0 {
		n = runtime.GOMAXPROCS(0)
	}
	shards := c.V2.Shards
	if shards == 0 {
		shards = n
	}
	budget := c.V2.ReadBudgetBytes
	if budget == 0 {
		budget = 256 << 20
	}
	r, err := v2read.New(shards, budget, min(budget, 16<<20))
	if err != nil {
		return err
	}
	v := &v2Runtime{reader: r, stop: make(chan struct{})}
	app.v2 = v
	v.store, err = v2daemon.Open(c.V2.Path, v2daemon.Config{Layer: 3, Producers: n, Shards: shards, GroupInterval: time.Second, Hooks: v2core.Hooks{PublishBatches: r.Publish}})
	if err != nil {
		return err
	}
	refresh := func() error {
		names, err := r.Names(context.Background())
		if err != nil {
			return err
		}
		old := v.catalog.Snapshot()
		if old.Revision() > 0 && slices.Equal(slices.Collect(old.Names()), names) {
			return nil
		}
		return v.catalog.Publish(old.Revision()+1, names)
	}
	if err = refresh(); err != nil {
		return err
	}
	core := cache.New()
	app.Cache = core
	queue, err := app.configureCarbonserver(core)
	if err != nil {
		return err
	}
	app.Carbonserver.SetV2Reader(r)
	if err = app.Carbonserver.SetV2Catalog(&v.catalog); err != nil {
		return err
	}
	if err = app.listenCarbonserver(core, queue); err != nil {
		return err
	}
	l, err := net.Listen("tcp", c.Tcp.Listen)
	if err != nil {
		return err
	}
	resolve := func(name string) ([]byte, error) {
		schema, ok := c.Whisper.Schemas.Match(name)
		if !ok {
			return nil, fmt.Errorf("no v2 schema for %q", name)
		}
		agg := c.Whisper.Aggregation.Match(name)
		cfg := sealrollup.Config{Method: sealrollup.Method(agg.AggregationMethod()), XFilesFactor: float32(agg.XFilesFactor())}
		for _, a := range schema.Retentions {
			cfg.Archives = append(cfg.Archives, sealrollup.Archive{Step: int64(a.SecondsPerPoint()), Count: int64(a.NumberOfPoints())})
		}
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: cfg})
	}
	v.server, err = v.store.ReceiveApplicationsTCP(l, v2recv.Config{}, resolve)
	if err != nil {
		_ = l.Close()
		return err
	}
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-v.stop:
				return
			case <-ticker.C:
				if err := refresh(); err != nil {
					zapwriter.Logger("v2").Error("v2 discovery publication failed", zap.Error(err))
					v.server.Abort(err)
					return
				}
			}
		}
	}()
	return nil
}
func stringsOverlap(a, b string) bool {
	a = filepath.Clean(a)
	b = filepath.Clean(b)
	rel, err := filepath.Rel(a, b)
	if err == nil && rel != ".." && !stringsHasParent(rel) {
		return true
	}
	rel, err = filepath.Rel(b, a)
	return err == nil && rel != ".." && !stringsHasParent(rel)
}
func stringsHasParent(s string) bool { return len(s) >= 3 && s[:3] == ".."+string(filepath.Separator) }
func (v *v2Runtime) stopIntake() {
	if v.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := v.server.Wait(ctx); err != nil {
			zapwriter.Logger("v2").Error("v2 receiver shutdown failed", zap.Error(err))
		}
	}
}
func (v *v2Runtime) close() {
	v.stopIntake()
	close(v.stop)
	v.wg.Wait()
	v.reader.Close()
	if v.store != nil {
		if err := v.store.Close(); err != nil {
			zapwriter.Logger("v2").Error("v2 store shutdown failed", zap.Error(err))
		}
	}
}
