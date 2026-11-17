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
	BaselinePath string    `toml:"baseline-path"`
	SealInterval *Duration `toml:"seal-interval"`
	SealMaxRows  int       `toml:"seal-max-rows"`

	Enabled         bool   `toml:"enabled"`
	Mode            string `toml:"mode"`
	Path            string `toml:"path"`
	Producers       int    `toml:"producers"`
	Shards          int    `toml:"shards"`
	ReadBudgetBytes int64  `toml:"read-budget-bytes"`
}
type v2Runtime struct {
	mode    string
	metrics *v2ModeMetrics
	quotas  *carbonserver.V2Quotas
	store   *v2daemon.Store
	moves   *v2daemon.MoveAdmin
	reader  *v2read.Reader
	catalog v2LazyCatalog
	server  *v2recv.Server
	stop    chan struct{}
	wg      sync.WaitGroup
}

func (app *App) startV2() error {
	c := app.Config
	mode, err := v2StorageMode(c.V2)
	if err != nil {
		return err
	}
	dual := mode != v2Only
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
	var core *cache.Cache
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
	v := &v2Runtime{mode: mode, reader: r, stop: make(chan struct{}), metrics: &v2ModeMetrics{mode: mode}}
	app.v2 = v
	// Replay must complete before wall-clock expiry may create new reset barriers.
	var recovered atomic.Bool
	var expiryNeeded, expiryAllowed atomic.Bool
	var intake atomic.Pointer[v2recv.Server]
	telemetry := newV2WriteTelemetry(shards, n)
	hooks := v2core.Hooks{Identities: r.Identities, PublishBatchesWithInventory: r.PublishWithInventory, DenseRollups: true, SealTiming: telemetry.observeStage, Visible: telemetry.visible}
	if c.Whisper.StoreExpiration.Value() > 0 || len(c.Whisper.ExpirationRules) > 0 {
		hooks.Expire = func(p v2core.RollupPolicy, m v2core.RollupMetadata, now time.Time) bool {
			if !recovered.Load() || c.Whisper.StoreExpirationDryRun || m.LastUpdate == 0 {
				return false
			}
			ttl := c.Whisper.StoreExpiration.Value()
			if i := c.Whisper.ExpirationRules.MatchIndex(string(p.Name), now); i >= 0 {
				ttl = c.Whisper.ExpirationRules[i].TTL()
			}
			if ttl <= 0 || now.Sub(time.Unix(0, m.LastUpdate)) < ttl {
				return false
			}
			if expiryAllowed.Load() {
				return true
			}
			expiryNeeded.Store(true)
			return false
		}
	}
	var sealInterval time.Duration
	if c.V2.SealInterval != nil {
		sealInterval = c.V2.SealInterval.Value()
	}
	storeConfig := v2daemon.Config{BaselinePath: c.V2.BaselinePath, Layer: 3, Producers: n, Shards: shards, GroupInterval: time.Second, SealInterval: sealInterval, SealMaxRows: c.V2.SealMaxRows, Hooks: hooks}
	storeConfig.Prepare = func() error {
		if err := app.prepareV2Fallback(mode); err != nil {
			return err
		}
		if dual {
			if !c.Whisper.Enabled || c.Tags.Enabled || c.Whisper.StoreLiveMigration || c.Whisper.StoreIncrementalMigration || c.Whisper.StoreMetadataCachePrewarm || c.Whisper.StoreExpiration.Value() > 0 || len(c.Whisper.ExpirationRules) > 0 {
				return errors.New("v2 dual write requires an enabled stable classic engine; disable tags, migration, metadata prewarm and expiry")
			}
			core, err = app.startStorage()
			if err != nil {
				return err
			}
			if err = app.startPersister(); err != nil {
				return err
			}
			close(app.metadataPrewarmReady)
		} else {
			core = cache.New()
			app.Cache = core
		}

		return nil
	}
	if dual {
		storeConfig.DurableApplications = func(producer int, batches []v2core.Batch) error { return v.mirrorClassic(core)(producer, batches) }
	}
	v.store, err = v2daemon.Open(c.V2.Path, storeConfig)
	if err != nil {
		return err
	}
	if dual {
		deadline := time.Now().Add(5 * time.Minute)
		diagnosticAt := time.Now().Add(5 * time.Second)
		for !core.IsEmpty() {
			if !diagnosticAt.IsZero() && time.Now().After(diagnosticAt) {
				// One bounded diagnostic per startup, including an idle queue
				// with unconfirmed batches that cannot be offered again.
				pebbleWrites, whisperWrites := app.Persister.WriteStats()
				zapwriter.Logger("v2").Warn("waiting for classic replay drain",
					zap.Int64("cache-points", core.Size()), zap.Int32("unconfirmed", core.NotConfirmedLength()),
					zap.Any("writeout", core.WriteoutState()), zap.String("queue", core.WriteoutQueue().DebugState()),
					zap.Any("pebble-writes", pebbleWrites), zap.Any("whisper-writes", whisperWrites),
					zap.Uint64("committed-points", app.Persister.CommittedPoints()))
				diagnosticAt = time.Time{}
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("classic replay did not drain; refusing to serve: points=%d unconfirmed=%d writeout=%+v committed=%d", core.Size(), core.NotConfirmedLength(), core.WriteoutState(), app.Persister.CommittedPoints())
			}
			time.Sleep(10 * time.Millisecond)
		}
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
	var refreshMu sync.Mutex
	refresh := func() error {
		refreshMu.Lock()
		defer refreshMu.Unlock()
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
		old := v.catalog.Catalog.Snapshot()
		if old.Revision() > 0 && old.EqualNames(names) {
			return nil
		}
		return v.catalog.Publish(old.Revision()+1, names)
	}
	v.catalog.refresh = refresh
	v.catalog.fail = func(err error) {
		if server := intake.Load(); server != nil {
			server.Abort(err)
		}
	}
	if err = refresh(); err != nil {
		return err
	}
	queue, err := app.configureCarbonserver(core)
	if err != nil {
		return err
	}
	if mode != v2Shadow {
		app.Carbonserver.SetV2UnionReader(r)
	}
	app.Carbonserver.RegisterInternalInfoHandler("v2-write", func() map[string]interface{} {
		out := telemetry.report()
		out["part_gc"] = v.store.Core.PartGCStats()
		if server := intake.Load(); server != nil {
			out["receiver"] = server.Stats()
		}
		return out
	})
	app.Carbonserver.RegisterInternalInfoHandler("v2-storage", func() map[string]interface{} { return map[string]interface{}{"mode": mode, "classic-dual-write": dual} })
	if c.Prometheus.Enabled {
		if err := app.PromRegisterer.Register(v.metrics); err != nil {
			return err
		}
		v.metrics.unregister = func() { app.PromRegisterer.Unregister(v.metrics) }
	}
	if mode != v2Shadow {
		if err = app.Carbonserver.SetV2Catalog(&v.catalog); err != nil {
			return err
		}
	}
	if mode == v2ServingDual {
		for _, name := range classicNames {
			policy, err := r.Metadata(context.Background(), name)
			if errors.Is(err, v2read.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := app.Carbonserver.ValidateV2UnionPolicy(name, policy); err != nil {
				return err
			}
		}
	}
	v.moves = v2daemon.NewMoveAdmin(c.V2.Path, v.store, func(index int, run func() error) error {
		receiver := intake.Load()
		if receiver == nil {
			return errors.New("v2 intake not ready")
		}
		return receiver.OnOwner(index, run)
	})
	app.Carbonserver.SetV2MoveAdmin(v.moves)
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
		if mode != v2Shadow {
			if err := app.Carbonserver.ValidateV2UnionPolicy(name, policy); err != nil {
				return nil, err
			}
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
	app.Collector = NewCollector(app)
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
				maintain := func() error {
					if expiryNeeded.Swap(false) {
						if err := v.server.Fence(func() error {
							// Confirm the durable name frontier before retiring
							// quota reservations in the post-expiry projection.
							if v.quotas != nil {
								if err := refresh(); err != nil {
									return err
								}
							}
							expiryAllowed.Store(true)
							defer expiryAllowed.Store(false)
							for i, p := range v.store.Producers {
								if err := v.server.OnOwner(i, p.Flush); err != nil {
									return err
								}
							}
							if v.quotas != nil {
								return refresh()
							}
							return nil
						}); err != nil {
							return err
						}
					}

					return nil
				}
				if err := maintain(); err != nil {
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
	if v.metrics.unregister != nil {
		v.metrics.unregister()
	}
	close(v.stop)
	v.wg.Wait()
	v.reader.Close()
	if v.store != nil {
		if err := v.store.Close(); err != nil {
			zapwriter.Logger("v2").Error("v2 store shutdown failed", zap.Error(err))
		}
	}
}

// v2LazyCatalog keeps idle discovery off the write path. Catalog snapshots used
// by telemetry remain cheap; only a discovery consumer asks for a fresh view.
type v2LazyCatalog struct {
	v2index.Catalog
	refresh func() error
	fail    func(error)
}

func (s *v2LazyCatalog) Snapshot() *v2index.Snapshot {
	if s.refresh != nil {
		if err := s.refresh(); err != nil {
			s.fail(err)
		}
	}
	return s.Catalog.Snapshot()
}
