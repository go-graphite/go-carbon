package carbon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/carbonserver"
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
	// IDKeys makes seals write ID-keyed parts through the store name
	// dictionary. Vshard moves of ID-keyed parts are not supported yet.
	IDKeys bool `toml:"id-keys"`
}
type v2Runtime struct {
	mode    string
	metrics *v2ModeMetrics
	quotas  *carbonserver.V2Quotas
	store   *v2daemon.Store
	moves   *v2daemon.MoveAdmin
	reader  *v2read.Reader
	catalog v2PublishedCatalog
	server  *v2recv.Server
	stop    chan struct{}
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

const v2SteadyGCPercent = 50

// v2L0Drain bounds how long an accepted point waits in its producer before
// core takes it and writes it to an L0 file at the next publication.
const v2L0Drain = 100 * time.Millisecond

func (app *App) startV2() error {
	c := app.Config
	// v2 does not enter the classic startup path that applies these rates.
	app.applyRuntimeProfiling()
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
	if !c.Tcp.Enabled || !c.Carbonserver.Enabled || c.Udp.Enabled || c.Pickle.Enabled || c.Carbonlink.Enabled || c.Grpc.Enabled || len(c.Receiver) > 0 || c.Buckyd.Enabled || c.Dump.Enabled {
		return errors.New("v2 currently requires TCP and carbonserver only; disable other receivers, buckyd and dump")
	}
	// v2 recovers from its L0 run files; the classic engine of a dual mode
	// recovers from its own input log, so each loses at most its own window.
	if c.InputLog.Enabled != dual {
		return errors.New("v2 dual write and shadow require the classic input-log; v2-only must disable it")
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
	updates := newV2CatalogUpdates(shards)
	catalogTelemetry := &v2CatalogTelemetry{}
	hooks := v2core.Hooks{Identities: r.Identities, PublishPending: r.PublishPending, DenseRollups: true, SealTiming: telemetry.observeStage, VisiblePending: func(owner int, pending v2core.PendingView) {
		telemetry.visiblePending(owner, pending)
		updates.visiblePending(owner, pending)
	}}
	if app.v2Now != nil {
		hooks.Now = app.v2Now
	}
	if c.V2.IDKeys {
		v2part.IDKeys.Store(true)
		hooks.IDKeys = true
	}
	if c.Whisper.StoreExpiration.Value() > 0 || len(c.Whisper.ExpirationRules) > 0 {
		// v2core asks Expire only about names idle for their ExpiryTTL window.
		hooks.Expire = func(p v2core.RollupPolicy, m v2core.RollupMetadata, _ time.Time) bool {
			if !recovered.Load() {
				return false
			}
			if expiryAllowed.Load() {
				updates.expire(m.ID, string(p.Name))
				return true
			}
			expiryNeeded.Store(true)
			return false
		}
		// A name's window, resolved once per name; it may change only when a
		// rule's Until passes. A dry run expires nothing.
		hooks.ExpiryTTL = func(p v2core.RollupPolicy, now time.Time) (time.Duration, time.Time) {
			if c.Whisper.StoreExpirationDryRun {
				return 0, time.Time{}
			}
			ttl := c.Whisper.StoreExpiration.Value()
			if i := c.Whisper.ExpirationRules.MatchIndex(string(p.Name), now); i >= 0 {
				ttl = c.Whisper.ExpirationRules[i].TTL()
			}
			var until time.Time
			for _, rule := range c.Whisper.ExpirationRules {
				if !rule.Until.IsZero() && now.Before(rule.Until) && (until.IsZero() || rule.Until.Before(until)) {
					until = rule.Until
				}
			}
			return ttl, until
		}
	}
	var sealInterval time.Duration
	if c.V2.SealInterval != nil {
		sealInterval = c.V2.SealInterval.Value()
	}
	// v2 keeps no producer journal: core writes L0 run files before each
	// publication. With no fsync to batch, producers hand rows to core every
	// 100 ms, so the kill-loss window is about one publication.
	storeConfig := v2daemon.Config{BaselinePath: c.V2.BaselinePath, Layer: 3, Producers: n, Shards: shards, GroupInterval: v2L0Drain, SealInterval: sealInterval, SealMaxRows: c.V2.SealMaxRows, Hooks: hooks}
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
			// Classic replays its own input-log tail before v2 opens.
			if err = app.replayInputLog(core); err != nil {
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
		// Prepare starts classic inside Open: resolve core at the first call.
		storeConfig.Mirror = func(producer int, batches []v2core.Batch) error { return v.mirrorClassic(core)(producer, batches) }
	}
	v.store, err = v2daemon.Open(c.V2.Path, storeConfig)
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
	ctx, cancel := context.WithCancel(context.Background())
	v.cancel = cancel
	initialized := false
	// Only the maintenance owner refreshes. Requests load Catalog.Snapshot and
	// never call discovery, take a refresh mutex, or wait for this work.
	// Without quotas (whose projection needs every name at once) a backlog is
	// published in bounded chunks, back to back, so no publication is long.
	// The base level of the discovery index lives in files (page cache).
	if err := v.catalog.SetDir(filepath.Join(c.V2.Path, "index")); err != nil {
		return err
	}
	chunk := v2CatalogChunk
	if v.quotas != nil {
		chunk = 0
	}
	var moreQueued bool
	refreshOnce := func() (refreshErr error) {
		var quotaFence uint64
		if v.quotas != nil {
			quotaFence = v.quotas.Fence()
		}
		old := v.catalog.Snapshot()
		if !initialized {
			names, masks, err := r.NamesWithMasks(ctx)
			if err != nil {
				return err
			}
			// Owner names arrive sorted and unique; only classic names are sorted.
			var classic []string
			for _, name := range classicNames {
				if !masks[name] {
					classic = append(classic, name)
				}
			}
			slices.Sort(classic)
			names = v2read.MergeNames(names, slices.Compact(classic))
			if err := publishV2QuotasContext(ctx, v.quotas, quotaFence, r, names, historicalQuota); err != nil {
				return err
			}
			if err := v.catalog.Publish(old.Revision()+1, names); err != nil {
				return err
			}
			initialized = true
			return nil
		}
		var add []string
		var stamps []v2NameStamp
		var expired []string
		add, stamps, expired, moreQueued = updates.drain(chunk)
		defer func() {
			if refreshErr != nil {
				updates.retry(add, stamps, expired)
			}
		}()
		var remove []string
		// Expiry and copy candidates need committed reader confirmation: a failed
		// seal or a concurrent recreation must stay visible, and copies skip intake.
		for _, name := range expired {
			_, err := r.Metadata(ctx, name)
			switch {
			case errors.Is(err, v2read.ErrNotFound):
				remove = append(remove, name)
			case err != nil:
				return err
			default:
				add = append(add, name)
			}
		}
		if len(add) == 0 && len(remove) == 0 {
			return ctx.Err()
		}
		start := time.Now()
		if err := v.catalog.ApplyBeforePublishContext(ctx, old.Revision()+1, add, remove, func(next *v2index.Snapshot) error {
			if v.quotas == nil {
				return ctx.Err()
			}
			return publishV2QuotasContext(ctx, v.quotas, quotaFence, r, slices.Collect(next.Names()), historicalQuota)
		}); err != nil {
			return err
		}
		next := v.catalog.Snapshot()
		catalogTelemetry.published(start, add, stamps, next.Levels(), next.Len())
		return nil
	}
	refresh := func() error {
		for {
			moreQueued = false
			if err := refreshOnce(); err != nil || !moreQueued {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	// Quotas require recovered reservations before admission. Without quotas,
	// startup discovery belongs to the background worker (also compatible with
	// parallel restart recovery); the first HTTP request never performs it.
	if v.quotas != nil {
		if err = refresh(); err != nil {
			return err
		}
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
		s := v.store.Core.Observe()
		out["seal_size"] = map[string]uint64{"durable_seals": s.DurableSeals, "names": s.SealNames, "points": s.SealPoints}
		if server := intake.Load(); server != nil {
			out["receiver"] = server.Stats()
		}
		return out
	})
	app.Carbonserver.RegisterInternalInfoHandler("v2-catalog", catalogTelemetry.report)
	app.Carbonserver.RegisterInternalInfoHandler("v2-storage", func() map[string]interface{} { return map[string]interface{}{"mode": mode, "classic-dual-write": dual} })
	if c.Prometheus.Enabled {
		if err := app.PromRegisterer.Register(v.metrics); err != nil {
			return err
		}
		v.metrics.unregister = func() { app.PromRegisterer.Unregister(v.metrics) }
	}
	if mode != v2Shadow {
		if err = app.Carbonserver.SetV2Catalog(&v.catalog, r.AwaitHydration); err != nil {
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
	v.moves.SetNamesVisible(updates.changed)
	admin := http.NewServeMux()
	admin.Handle("/admin/v2/move/", v.moves)
	admin.HandleFunc("/admin/v2/merge", v.forceMerge)
	app.Carbonserver.SetV2Admin(admin)
	if err = app.listenCarbonserver(core, queue); err != nil {
		return err
	}
	l, err := net.Listen("tcp", c.Tcp.Listen)
	if err != nil {
		return err
	}
	resolve := app.v2PolicyResolver(mode, classicNames)
	var throttle func(*points.Points, bool) bool
	if v.quotas != nil {
		throttle = v.quotas.ShouldThrottleMetric
	}
	v.server, err = v.store.ReceiveApplicationsTCPWithAdmission(l, v2recv.Config{SyncInterval: storeConfig.GroupInterval}, resolve, throttle, func(name string, err error) { v.rejectName(core, name, err) })
	if err != nil {
		_ = l.Close()
		return err
	}
	intake.Store(v.server)
	app.Collector = NewCollector(app)
	if c.Prometheus.Enabled {
		app.v2CollectorOnce.Do(func() { app.PromRegisterer.MustRegister(&v2Collector{app: app}) })
	}
	// Bound the garbage allowance on large heaps while preserving operator
	// tuning. Recovery keeps the runtime's growth: it allocates in bursts and a
	// smaller allowance there only lengthens restart.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(v2SteadyGCPercent)
	}
	recovered.Store(true)
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
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
								if err := publishV2QuotasContext(ctx, v.quotas, v.quotas.Fence(), r, slices.Collect(v.catalog.Snapshot().Names()), historicalQuota); err != nil {
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

					return refresh()
				}
				if err := maintain(); err != nil {
					if ctx.Err() != nil {
						return
					}
					zapwriter.Logger("v2").Error("v2 discovery publication failed", zap.Error(err))
					v.server.Abort(err)
					return
				}
			}
		}
	}()
	return nil
}

var v2NameRejectLogged atomic.Int64

// rejectName counts one point refused by its name. Names classic cannot store
// either (length or Whisper path limits, including ENAMETOOLONG from the
// classic union probe) use classic's receiver_name_too_long counter; other
// per-name policy errors use v2_name_rejected_points_total. Intake continues;
// the warning is logged at most once a minute, as classic does.
func (v *v2Runtime) rejectName(core *cache.Cache, name string, err error) {
	if errors.Is(err, v2daemon.ErrNameTooLong) || errors.Is(err, syscall.ENAMETOOLONG) {
		core.AddNameTooLongRejected(1)
	} else {
		v.metrics.rejectedNames.Add(1)
	}
	now := time.Now().UnixNano()
	last := v2NameRejectLogged.Load()
	if now-last < int64(time.Minute) || !v2NameRejectLogged.CompareAndSwap(last, now) {
		return
	}
	zapwriter.Logger("v2").Warn("refused points: invalid metric name", zap.Int("name_bytes", len(name)),
		zap.String("name_prefix", name[:min(len(name), 200)]), zap.Error(err),
		zap.Uint64("name_too_long_rejected_points_total", core.NameTooLongRejectedPoints()),
		zap.Uint64("name_rejected_points_total", v.metrics.rejectedNames.Load()))
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
	if v.cancel != nil {
		v.cancel()
	}
	v.stopIntake()
	if v.metrics.unregister != nil {
		v.metrics.unregister()
	}
	close(v.stop)
	v.wg.Wait()
	v.catalog.Close()
	v.reader.Close()
	if v.store != nil {
		if err := v.store.Close(); err != nil {
			zapwriter.Logger("v2").Error("v2 store shutdown failed", zap.Error(err))
		}
	}
}

// v2PublishedCatalog exposes only the latest immutable publication to HTTP.
// Its single background owner performs refresh and quota fencing.
type v2PublishedCatalog struct{ v2index.Catalog }

// forceMerge (POST /admin/v2/merge) merges every owner's parts down to one per
// group and time partition and replies {"merges": n}. Repeat until n is 0.
func (v *v2Runtime) forceMerge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "merge requires POST", http.StatusMethodNotAllowed)
		return
	}
	n, err := v.store.Core.ForceMerge()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"merges\":%d}\n", n)
}
