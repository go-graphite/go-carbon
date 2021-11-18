package persister

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	whisper "github.com/go-graphite/go-whisper"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
	"github.com/lomik/zapwriter"
)

const storeMutexCount = 32768

type StoreFunc func(metric string)

// Whisper write data to *.wsp files
type Whisper struct {
	helper.Stoppable
	recv                    func(chan bool) string
	pop                     func(string) (*points.Points, bool)
	confirm                 func(*points.Points)
	popConfirm              func(string) (*points.Points, bool)
	tagsEnabled             bool
	taggedFn                func(string, bool)
	schemas                 WhisperSchemas
	aggregation             *WhisperAggregation
	workersCount            int
	rootPath                string
	created                 uint32 // counter
	throttledCreates        uint32 // counter
	updateOperations        uint32 // counter
	committedPoints         uint32 // counter
	extended                uint32 // counter
	sparse                  bool
	flock                   bool
	compressed              bool
	hashFilenames           bool
	removeEmptyFile         bool
	maxUpdatesPerSecond     int
	maxCreatesPerSecond     int
	hardMaxCreatesPerSecond bool
	throttleTicker          *ThrottleTicker
	maxCreatesTicker        *ThrottleTicker
	storeMutex              [storeMutexCount]sync.Mutex
	mockStore               func() (StoreFunc, func())
	logger                  *zap.Logger
	createLogger            *zap.Logger

	onlineMigration struct {
		enabled bool
		rate    int
		ticker  *ThrottleTicker

		schema            bool
		xff               bool
		aggregationMethod bool

		stat struct {
			total               uint32
			schema              uint32
			xff                 uint32
			aggregationMethod   uint32
			physicalSizeChanges int64
			logicalSizeChanges  int64
		}
	}

	// blockThrottleNs        uint64 // sum ns counter
	// blockQueueGetNs        uint64 // sum ns counter
	// blockAvoidConcurrentNs uint64 // sum ns counter
	// blockUpdateManyNs      uint64 // sum ns counter
}

// NewWhisper create instance of Whisper
func NewWhisper(
	rootPath string,
	schemas WhisperSchemas,
	aggregation *WhisperAggregation,
	recv func(chan bool) string,
	pop func(string) (*points.Points, bool),
	confirm func(*points.Points),
	popConfirm func(string) (*points.Points, bool)) *Whisper {

	return &Whisper{
		recv:                recv,
		pop:                 pop,
		confirm:             confirm,
		popConfirm:          popConfirm,
		schemas:             schemas,
		aggregation:         aggregation,
		workersCount:        1,
		rootPath:            rootPath,
		maxUpdatesPerSecond: 0,
		logger:              zapwriter.Logger("persister"),
		createLogger:        zapwriter.Logger("whisper:new"),
	}
}

// SetMaxUpdatesPerSecond enable throttling
func (p *Whisper) SetMaxUpdatesPerSecond(maxUpdatesPerSecond int) {
	p.maxUpdatesPerSecond = maxUpdatesPerSecond
}

// SetMaxCreatesPerSecond enable throttling
func (p *Whisper) SetMaxCreatesPerSecond(maxCreatesPerSecond int) {
	p.maxCreatesPerSecond = maxCreatesPerSecond
}

// SetHardMaxCreatesPerSecond enable throttling
func (p *Whisper) SetHardMaxCreatesPerSecond(hardMaxCreatesPerSecond bool) {
	p.hardMaxCreatesPerSecond = hardMaxCreatesPerSecond
}

// SetOnlineMigration enable online migration
func (p *Whisper) EnableOnlineMigration(rate int, scope []string) {
	p.onlineMigration.enabled = true
	p.onlineMigration.rate = rate

	if len(scope) > 0 {
		for _, s := range scope {
			switch s {
			case "xff":
				p.onlineMigration.xff = true
			case "aggregationMethod":
				p.onlineMigration.aggregationMethod = true
			case "schema":
				p.onlineMigration.schema = true
			}
		}
	} else {
		p.onlineMigration.xff = true
		p.onlineMigration.aggregationMethod = true
		p.onlineMigration.schema = true
	}
}

// GetMaxUpdatesPerSecond returns current throttling speed
func (p *Whisper) GetMaxUpdatesPerSecond() int {
	return p.maxUpdatesPerSecond
}

// SetWorkers count
func (p *Whisper) SetWorkers(count int) {
	if count >= 1 {
		p.workersCount = count
	} else {
		p.workersCount = 1
	}
}

// SetSparse creation
func (p *Whisper) SetSparse(sparse bool) {
	p.sparse = sparse
}

// SetFLock on create and open
func (p *Whisper) SetFLock(flock bool) {
	p.flock = flock
}

func (p *Whisper) SetCompressed(compressed bool) {
	p.compressed = compressed
}

func (p *Whisper) SetRemoveEmptyFile(remove bool) {
	p.removeEmptyFile = remove
}

func (p *Whisper) SetHashFilenames(v bool) {
	p.hashFilenames = v
}

func (p *Whisper) SetMockStore(fn func() (StoreFunc, func())) {
	p.mockStore = fn
}

func (p *Whisper) SetTagsEnabled(v bool) {
	p.tagsEnabled = v
}

func (p *Whisper) SetTaggedFn(fn func(string, bool)) {
	p.taggedFn = fn
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (p *Whisper) updateMany(w *whisper.Whisper, path string, points []*whisper.TimeSeriesPoint) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("UpdateMany panic recovered",
				zap.String("path", path),
				zap.String("traceback", fmt.Sprint(r)),
			)
		}
	}()

	// start = time.Now()
	if err := w.UpdateMany(points); err != nil {
		p.logger.Error("fail to update metric",
			zap.String("path", path),
			zap.Error(err),
		)
	}
	if w.Extended {
		atomic.AddUint32(&p.extended, 1)
		p.logger.Info("cwhisper file has extended", zap.String("path", path))
	}
}

func (p *Whisper) store(metric string) {
	// avoid concurrent store same metric
	// @TODO: may be flock?
	// start := time.Now()
	mutexIndex := fnv32(metric) % storeMutexCount
	p.storeMutex[mutexIndex].Lock()
	// atomic.AddUint64(&p.blockAvoidConcurrentNs, uint64(time.Since(start).Nanoseconds()))
	defer p.storeMutex[mutexIndex].Unlock()

	var path string
	if p.tagsEnabled && strings.IndexByte(metric, ';') >= 0 {
		path = tags.FilePath(p.rootPath, metric, p.hashFilenames) + ".wsp"
	} else {
		path = filepath.Join(p.rootPath, strings.ReplaceAll(metric, ".", "/")+".wsp")
	}

	var newFile bool
	w, err := whisper.OpenWithOptions(path, &whisper.Options{
		FLock:      p.flock,
		Compressed: p.compressed,
	})
	if err != nil {
		// create new whisper if file not exists
		if !os.IsNotExist(err) {
			// There are cases that new files are created but data are not saved
			// on disk due to edge issues like server panics/reboot. So it's
			// better to treat empty files as NotExist to work around this
			// problem.
			stat, err2 := os.Stat(path)
			if err2 != nil {
				p.logger.Error("failed to stat whisper file", zap.String("path", path), zap.Error(p.simplifyPathError(err2)))
			}
			if !p.removeEmptyFile || err2 != nil || stat.Size() > 0 {
				p.logger.Error("failed to open whisper file", zap.String("path", path), zap.Error(p.simplifyPathError(err)))
				if pathErr, isPathErr := err.(*os.PathError); isPathErr && pathErr.Err == syscall.ENAMETOOLONG {
					p.popConfirm(metric)
				}
				return
			}
			if err := os.Remove(path); err != nil {
				p.logger.Error("failed to delete empty whisper file", zap.String("path", path), zap.Error(p.simplifyPathError(err)))
				p.popConfirm(metric)
				return
			}
			p.logger.Warn("deleted empty whisper file", zap.String("path", path))
		}

		if t := p.maxCreatesThrottling(); t != throttlingOff {
			if t == throttlingHard {
				p.popConfirm(metric)
			}

			atomic.AddUint32(&p.throttledCreates, 1)
			p.logger.Error("metric creation throttled",
				zap.String("name", metric),
				zap.String("operation", "create"),
				zap.Bool("dropped", t == throttlingHard),
			)

			return
		}

		schema, ok := p.schemas.Match(metric)
		if !ok {
			p.logger.Error("no storage schema defined for metric", zap.String("metric", metric))
			return
		}

		aggr := p.aggregation.Match(metric)
		if aggr == nil {
			p.logger.Error("no storage aggregation defined for metric", zap.String("metric", metric))
			return
		}

		if err = os.MkdirAll(filepath.Dir(path), os.ModeDir|os.ModePerm); err != nil {
			p.logger.Error("mkdir failed",
				zap.String("dir", filepath.Dir(path)),
				zap.Error(p.simplifyPathError(err)),
				zap.String("path", path),
			)
			return
		}

		compressed := p.compressed
		if schema.Compressed != nil {
			compressed = *schema.Compressed
		}
		w, err = whisper.CreateWithOptions(path, schema.Retentions, aggr.aggregationMethod, float32(aggr.xFilesFactor), &whisper.Options{
			Sparse:     p.sparse,
			FLock:      p.flock,
			Compressed: compressed,
		})
		if err != nil {
			p.logger.Error("create new whisper file failed",
				zap.String("path", path),
				zap.Error(p.simplifyPathError(err)),
				zap.String("retention", schema.RetentionStr),
				zap.String("schema", schema.Name),
				zap.String("aggregation", aggr.name),
				zap.Float64("xFilesFactor", aggr.xFilesFactor),
				zap.String("method", aggr.aggregationMethodStr),
				zap.Bool("compressed", compressed),
			)
			return
		}

		if p.tagsEnabled && p.taggedFn != nil && strings.IndexByte(metric, ';') >= 0 {
			p.taggedFn(metric, true)
		}

		p.createLogger.Debug("created",
			zap.String("path", path),
			zap.String("retention", schema.RetentionStr),
			zap.String("schema", schema.Name),
			zap.String("aggregation", aggr.name),
			zap.Float64("xFilesFactor", aggr.xFilesFactor),
			zap.String("method", aggr.aggregationMethodStr),
			zap.Bool("compressed", compressed),
		)

		atomic.AddUint32(&p.created, 1)

		newFile = true
	}

	// Check if schema and aggregation is still up-to-date
	if !newFile && p.onlineMigration.enabled {
		// errors are logged already
		w, _, _ = p.checkAndUpdateSchemaAndAggregation(w, metric)
	}

	values, exists := p.pop(metric)
	if !exists {
		return
	}
	if p.confirm != nil {
		defer p.confirm(values)
	}

	points := make([]*whisper.TimeSeriesPoint, len(values.Data))
	for i, r := range values.Data {
		points[i] = &whisper.TimeSeriesPoint{Time: int(r.Timestamp), Value: r.Value}
	}

	atomic.AddUint32(&p.committedPoints, uint32(len(values.Data)))
	atomic.AddUint32(&p.updateOperations, 1)

	// start = time.Now()
	p.updateMany(w, path, points)

	// TODO: produce realtime size info and forward it to carbonserver.trieIndex
	// fi := w.File().Stat()

	w.Close()
	// atomic.AddUint64(&p.blockUpdateManyNs, uint64(time.Since(start).Nanoseconds()))

	if p.tagsEnabled && p.taggedFn != nil && strings.IndexByte(metric, ';') >= 0 {
		p.taggedFn(metric, false)
	}
}

type throttleMode int

const (
	throttlingOff  throttleMode = iota
	throttlingSoft throttleMode = iota
	throttlingHard throttleMode = iota
)

// For test error messages
func (t throttleMode) String() string {
	if t == throttlingHard {
		return "hard"
	}

	if t == throttlingSoft {
		return "soft"
	}

	return "off"
}

func (p *Whisper) maxCreatesThrottling() throttleMode {
	if p.hardMaxCreatesPerSecond {
		keep, open := <-p.maxCreatesTicker.C
		if !open {
			return throttlingHard
		}

		if keep {
			return throttlingOff
		}

		return throttlingHard
	} else {
		select {
		case <-p.maxCreatesTicker.C:
			return throttlingOff

		default:
			return throttlingSoft
		}
	}
}

func (p *Whisper) worker(exit chan bool) {
	storeFunc := p.store
	var doneCb func()
	if p.mockStore != nil {
		storeFunc, doneCb = p.mockStore()
	}

LOOP:
	for {
		// start := time.Now()
		select {
		case <-p.throttleTicker.C:
			// atomic.AddUint64(&p.blockThrottleNs, uint64(time.Since(start).Nanoseconds()))
			// pass
		case <-exit:
			return
		}

		// start = time.Now()
		metric := p.recv(exit)
		// atomic.AddUint64(&p.blockQueueGetNs, uint64(time.Since(start).Nanoseconds()))
		if metric == "" {
			// exit closed
			break LOOP
		}
		storeFunc(metric)
		if doneCb != nil {
			doneCb()
		}
	}
}

// Stat callback
func (p *Whisper) Stat(send helper.StatCallback) {
	updateOperations := atomic.LoadUint32(&p.updateOperations)
	committedPoints := atomic.LoadUint32(&p.committedPoints)
	atomic.AddUint32(&p.updateOperations, -updateOperations)
	atomic.AddUint32(&p.committedPoints, -committedPoints)

	created := atomic.LoadUint32(&p.created)
	atomic.AddUint32(&p.created, -created)

	extended := atomic.LoadUint32(&p.extended)
	atomic.AddUint32(&p.extended, -extended)

	throttledCreates := atomic.LoadUint32(&p.throttledCreates)
	atomic.AddUint32(&p.throttledCreates, -throttledCreates)

	send("updateOperations", float64(updateOperations))
	send("committedPoints", float64(committedPoints))
	if updateOperations > 0 {
		send("pointsPerUpdate", float64(committedPoints)/float64(updateOperations))
	} else {
		send("pointsPerUpdate", 0.0)
	}

	send("created", float64(created))
	send("throttledCreates", float64(throttledCreates))
	send("maxCreatesPerSecond", float64(p.maxCreatesPerSecond))

	send("maxUpdatesPerSecond", float64(p.maxUpdatesPerSecond))
	send("workers", float64(p.workersCount))
	send("extended", float64(extended))

	if p.onlineMigration.enabled {
		stat := &p.onlineMigration.stat

		total := atomic.LoadUint32(&stat.total)
		atomic.AddUint32(&stat.total, -total)
		send("onlineMigration.total", float64(total))

		schema := atomic.LoadUint32(&stat.schema)
		atomic.AddUint32(&stat.schema, -schema)
		send("onlineMigration.schema", float64(schema))

		xff := atomic.LoadUint32(&stat.xff)
		atomic.AddUint32(&stat.xff, -xff)
		send("onlineMigration.xff", float64(xff))

		aggregationMethod := atomic.LoadUint32(&stat.aggregationMethod)
		atomic.AddUint32(&stat.aggregationMethod, -aggregationMethod)
		send("onlineMigration.aggregationMethod", float64(aggregationMethod))

		physicalSizeChanges := atomic.LoadInt64(&stat.physicalSizeChanges)
		atomic.AddInt64(&stat.physicalSizeChanges, -physicalSizeChanges)
		send("onlineMigration.physicalSizeChanges", float64(physicalSizeChanges))

		logicalSizeChanges := atomic.LoadInt64(&stat.logicalSizeChanges)
		atomic.AddInt64(&stat.logicalSizeChanges, -logicalSizeChanges)
		send("onlineMigration.logicalSizeChanges", float64(logicalSizeChanges))
	}

	// helper.SendAndSubstractUint64("blockThrottleNs", &p.blockThrottleNs, send)
	// helper.SendAndSubstractUint64("blockQueueGetNs", &p.blockQueueGetNs, send)
	// helper.SendAndSubstractUint64("blockAvoidConcurrentNs", &p.blockAvoidConcurrentNs, send)
	// helper.SendAndSubstractUint64("blockUpdateManyNs", &p.blockUpdateManyNs, send)
}

// Start worker
func (p *Whisper) Start() error {
	return p.StartFunc(func() error {
		p.throttleTicker = NewThrottleTicker(p.maxUpdatesPerSecond)
		if p.hardMaxCreatesPerSecond {
			p.maxCreatesTicker = NewHardThrottleTicker(p.maxCreatesPerSecond)
		} else {
			p.maxCreatesTicker = NewThrottleTicker(p.maxCreatesPerSecond)
		}

		if p.onlineMigration.enabled {
			p.onlineMigration.ticker = NewHardThrottleTicker(p.onlineMigration.rate)
		}

		for i := 0; i < p.workersCount; i++ {
			p.Go(p.worker)
		}

		return nil
	})
}

func (p *Whisper) Stop() {
	p.StopFunc(func() {
		p.throttleTicker.Stop()
		p.maxCreatesTicker.Stop()

		if p.onlineMigration.ticker != nil {
			p.onlineMigration.ticker.Stop()
		}
	})
}

func (p *Whisper) GetRetentionPeriod(metric string) (int, bool) {
	schema, ok := p.schemas.Match(metric)
	if !ok {
		return 0, false
	}

	retentionStep := schema.Retentions[0].SecondsPerPoint()

	return retentionStep, true
}

func (p *Whisper) GetAggrConf(metric string) (string, float64, bool) {
	aggr := p.aggregation.Match(metric)
	if aggr == nil {
		return "", float64(0), false
	}

	return aggr.Name(), aggr.XFilesFactor(), true
}

// simplifyPathError retruns an error without path in the error message. This
// simpplies the log a bit as the path is usually printed separately and the
// new error message is easier to filter in elastic search and other tools.
func (*Whisper) simplifyPathError(err error) error {
	perr, ok := err.(*os.PathError)
	if !ok {
		return err
	}
	return fmt.Errorf("%s: %s", perr.Op, perr.Err)
}

func (p *Whisper) checkAndUpdateSchemaAndAggregation(w *whisper.Whisper, metric string) (*whisper.Whisper, bool, error) {
	// TODO: skip check and update if the config files has not been updated
	// for a long time and all the metrics have been validated? this only
	// help if the check is expensive, most likely it's not.

	logger := p.logger.Named("online_migration")
	schema, ok := p.schemas.Match(metric)
	if !ok {
		logger.Error("no storage schema defined for metric", zap.String("metric", metric))
		return w, false, nil
	}
	aggr := p.aggregation.Match(metric)
	if aggr == nil {
		logger.Error("no storage aggregation defined for metric", zap.String("metric", metric))
		return w, false, nil
	}

	compressed := p.compressed
	if schema.Compressed != nil {
		compressed = *schema.Compressed
	}
	options := &whisper.Options{
		Sparse:     p.sparse,
		FLock:      p.flock,
		Compressed: compressed,
	}

	retentions := whisper.NewRetentionsNoPointer(w.Retentions())
	aggregationmethod := w.AggregationMethod()
	xFilesFactor := w.XFilesFactor()
	var updateSchema, updateXff, updateAggrMethod bool
	if schema.canMigrate(p.onlineMigration.schema) && !retentions.Equal(schema.Retentions) {
		retentions = schema.Retentions
		updateSchema = true
	}
	if aggr.canMigrateXff(p.onlineMigration.xff) && xFilesFactor != float32(aggr.xFilesFactor) {
		xFilesFactor = float32(aggr.xFilesFactor)
		updateXff = true
	}
	if aggr.canMigrateAggregationMethod(p.onlineMigration.aggregationMethod) && aggregationmethod != aggr.aggregationMethod {
		aggregationmethod = aggr.aggregationMethod
		updateAggrMethod = true
	}

	// No config migration needed.
	if !updateSchema && !updateXff && !updateAggrMethod {
		return w, false, nil
	}

	select {
	case hasCapacity := <-p.onlineMigration.ticker.C:
		if !hasCapacity {
			return w, false, nil
		}
	default:
		// makes sure that it's a non-blocking process
		return w, false, nil
	}

	if updateSchema {
		atomic.AddUint32(&p.onlineMigration.stat.schema, 1)
	}
	if updateXff {
		atomic.AddUint32(&p.onlineMigration.stat.xff, 1)
	}
	if updateAggrMethod {
		atomic.AddUint32(&p.onlineMigration.stat.aggregationMethod, 1)
	}
	atomic.AddUint32(&p.onlineMigration.stat.total, 1)

	var virtualSize, physicalSize int64
	if fiOld, err := w.File().Stat(); err != nil {
		logger.Error("failed to retrieve file info before migration", zap.String("metric", metric))
	} else {
		virtualSize = fiOld.Size()
		physicalSize = virtualSize
		if stat, ok := fiOld.Sys().(*syscall.Stat_t); ok {
			physicalSize = stat.Blocks * 512
		}
	}

	// IMPORTANT: file has be re-opened after a call to Whisper.UpdateConfig
	if err := w.UpdateConfig(retentions, aggregationmethod, xFilesFactor, options); err != nil {
		logger.Error("failed to migrate/update configs (schema/aggregation/etc)", zap.String("metric", metric))
		return w, false, err
	}

	// reopen whisper file
	nw, err := whisper.OpenWithOptions(w.File().Name(), &whisper.Options{
		FLock:      p.flock,
		Compressed: p.compressed,
	})

	if fiNew, err := w.File().Stat(); err != nil {
		logger.Error("failed to retrieve file info before migration", zap.String("metric", metric))
	} else {
		virtualSizeNew := fiNew.Size()
		physicalSizeNew := virtualSize
		if stat, ok := fiNew.Sys().(*syscall.Stat_t); ok {
			physicalSizeNew = stat.Blocks * 512
		}

		atomic.AddInt64(&p.onlineMigration.stat.logicalSizeChanges, virtualSizeNew-virtualSize)
		atomic.AddInt64(&p.onlineMigration.stat.physicalSizeChanges, physicalSizeNew-physicalSize)
	}

	return nw, true, err
}
