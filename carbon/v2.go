package carbon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2index"
	"github.com/go-graphite/go-carbon/internal/v2part"
	"github.com/go-graphite/go-carbon/internal/v2read"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
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
	quotas  *carbonserver.V2Quotas
	store   *v2daemon.Store
	reader  *v2read.Reader
	catalog v2index.Catalog
	server  *v2recv.Server
	stop    chan struct{}
	wg      sync.WaitGroup
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

	for _, path := range []string{c.Whisper.DataDir, c.Whisper.StoreDir} {
		if path == "" {
			continue
		}
		physical, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		if err := validateMirrorPaths(physical, c.V2.Path); err != nil {
			return fmt.Errorf("v2 root isolation: %w", err)
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
	r.Identities = &v2core.PolicyCatalog{}
	v := &v2Runtime{reader: r, stop: make(chan struct{})}
	app.v2 = v
	// Replay must complete before wall-clock expiry may create new reset barriers.
	var recovered atomic.Bool
	var intake atomic.Pointer[v2recv.Server]
	hooks := v2core.Hooks{Identities: r.Identities, PublishBatches: r.Publish, DenseRollups: true}
	if c.Whisper.StoreExpiration.Value() > 0 || len(c.Whisper.ExpirationRules) > 0 {
		hooks.Expire = func(p v2core.RollupPolicy, m v2core.RollupMetadata, now time.Time) bool {
			if !recovered.Load() || c.Whisper.StoreExpirationDryRun || m.LastUpdate == 0 {
				return false
			}
			if server := intake.Load(); server != nil && server.Pending(string(p.Name)) {
				return false
			}
			ttl := c.Whisper.StoreExpiration.Value()
			if i := c.Whisper.ExpirationRules.MatchIndex(string(p.Name), now); i >= 0 {
				ttl = c.Whisper.ExpirationRules[i].TTL()
			}
			return ttl > 0 && now.Sub(time.Unix(0, m.LastUpdate)) >= ttl
		}
	}
	v.store, err = v2daemon.Open(c.V2.Path, v2daemon.Config{Layer: 3, Producers: n, Shards: shards, GroupInterval: time.Second, Hooks: hooks})
	if err != nil {
		return err
	}
	classicNames, err := app.openV2Classic()
	if err != nil {
		return err
	}
	quotas, historicalQuota, err := app.newV2Quotas(classicNames)
	if err != nil {
		return err
	}
	v.quotas = quotas
	refresh := func() error {
		var quotaFence uint64
		if v.quotas != nil {
			quotaFence = v.quotas.Fence()
		}
		names, masks, err := r.NamesWithMasks(context.Background())
		if err != nil {
			return err
		}
		for _, name := range classicNames {
			if !masks[name] {
				names = append(names, name)
			}
		}
		slices.Sort(names)
		names = slices.Compact(names)
		if err := publishV2Quotas(v.quotas, quotaFence, r, names, historicalQuota); err != nil {
			return err
		}
		old := v.catalog.Snapshot()
		if old.Revision() > 0 && old.EqualNames(names) {
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
	app.Carbonserver.SetV2UnionReader(r)
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
		policy := v2read.Metric{AggregationMethod: agg.AggregationMethod().String(), XFilesFactor: cfg.XFilesFactor}
		for _, archive := range cfg.Archives {
			policy.Archives = append(policy.Archives, v2read.Archive{Step: archive.Step, Points: archive.Count})
		}
		if err := app.Carbonserver.ValidateV2UnionPolicy(name, policy); err != nil {
			return nil, err
		}
		return v2core.EncodeRollupPolicy(v2core.RollupPolicy{Name: v2part.ID(name), Group: "local", Config: cfg})
	}
	var throttle func(*points.Points, bool) bool
	if v.quotas != nil {
		throttle = v.quotas.ShouldThrottleMetric
	}
	v.server, err = v.store.ReceiveApplicationsTCPWithAdmission(l, v2recv.Config{}, resolve, throttle)
	if err != nil {
		_ = l.Close()
		return err
	}
	intake.Store(v.server)
	if c.Prometheus.Enabled {
		app.v2CollectorOnce.Do(func() { app.PromRegisterer.MustRegister(&v2Collector{app: app}) })
	}
	recovered.Store(true)
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
