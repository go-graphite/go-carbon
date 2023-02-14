package persister

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	whisper "github.com/go-graphite/go-whisper"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/tags"
	"github.com/lomik/zapwriter"
)

const storeMutexCount = 32768
const maxPathLength = 4095
const maxFilenameLength = 255

type StoreFunc func(metric string)

// Whisper write data to *.wsp files
type Whisper struct {
	helper.Stoppable
	recv                func(chan bool) string
	pop                 func(string) (*points.Points, bool)
	confirm             func(*points.Points)
	popConfirm          func(string) (*points.Points, bool)
	tagsEnabled         bool
	taggedFn            func(string, bool)
	schemas             WhisperSchemas
	aggregation         *WhisperAggregation
	workersCount        int
	rootPath            string
	created             uint32 // counter
	updateOperations    uint32 // counter
	committedPoints     uint32 // counter
	oooDiscardedPoints  uint32 // counter
	extended            uint32 // counter
	sparse              bool
	flock               bool
	compressed          bool
	hashFilenames       bool
	removeEmptyFile     bool
	maxUpdatesPerSecond int
	updateTicker        *helper.ThrottleTicker
	storeMutex          [storeMutexCount]sync.Mutex
	mockStore           func() (StoreFunc, func())
	logger              *zap.Logger
	createLogger        *zap.Logger

	onlineMigration struct {
		enabled bool
		rate    int
		ticker  *helper.ThrottleTicker

		schema            bool
		xff               bool
		aggregationMethod bool

		stat struct {
			total               uint32
			errors              uint32
			schema              uint32
			xff                 uint32
			aggregationMethod   uint32
			physicalSizeChanges int64
			logicalSizeChanges  int64
		}
	}
	prometheus whisperPrometheus
	// blockThrottleNs        uint64 // sum ns counter
	// blockQueueGetNs        uint64 // sum ns counter
	// blockAvoidConcurrentNs uint64 // sum ns counter
	// blockUpdateManyNs      uint64 // sum ns counter
}

// NewWhisper create instance of Whisper
type whisperPrometheus struct {
	enabled             bool
	outOfOrderWriteLags prometheus.Histogram
	outOfOrderWriteLag  func(time.Duration)
}

func NewWhisper(
	rootPath string,
	schemas WhisperSchemas,
	aggregation *WhisperAggregation,
	recv func(chan bool) string,
	pop func(string) (*points.Points, bool),
	confirm func(*points.Points),
	popConfirm func(string) (*points.Points, bool)) *Whisper {

	return &Whisper{
		recv:         recv,
		pop:          pop,
		confirm:      confirm,
		popConfirm:   popConfirm,
		schemas:      schemas,
		aggregation:  aggregation,
		workersCount: 1,
		rootPath:     rootPath,
		logger:       zapwriter.Logger("persister"),
		createLogger: zapwriter.Logger("whisper:new"),
	}
}

func (p *Whisper) InitPrometheus(reg prometheus.Registerer) {
	p.prometheus = whisperPrometheus{
		enabled: true,
		outOfOrderWriteLags: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "out_of_order_write_lag_exp",
				Help:    "Lag for incoming datapoints (exponential buckets)",
				Buckets: prometheus.ExponentialBuckets(time.Millisecond.Seconds(), 2.0, 30),
			},
		),
	}
	p.prometheus.outOfOrderWriteLag = func(t time.Duration) {
		p.prometheus.outOfOrderWriteLags.Observe(t.Seconds())
	}
	reg.MustRegister(p.prometheus.outOfOrderWriteLags)
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

// SetMaxUpdatesPerSecond
func (p *Whisper) SetMaxUpdatesPerSecond(maxUpdatesPerSecond int) {
	p.maxUpdatesPerSecond = maxUpdatesPerSecond
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
func (p *Whisper) registerOutOfOrderLags(points []*whisper.TimeSeriesPoint) {
	if !p.prometheus.enabled {
		return
	}
	now := time.Now()
	for _, point := range points {
		lag := now.Sub(time.Unix(int64(point.Time), 0))
		p.prometheus.outOfOrderWriteLag(lag)
	}
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
	p.registerOutOfOrderLags(points)
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

	// update oooDiscardedPoints counter
	discardedPointsSinceOpen := w.GetDiscardedPointsSinceOpen()
	if discardedPointsSinceOpen > 0 {
		atomic.AddUint32(&p.oooDiscardedPoints, discardedPointsSinceOpen)
		p.logger.Debug("cwhisper file has ooo-discarded points",
			zap.Int("w.GetDiscardedPointsSinceOpen()", int(discardedPointsSinceOpen)),
			zap.String("path", path),
			zap.Any("points", points))
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

	if len(path) > maxPathLength {
		p.logger.Debug("metric name exceeds limit",
			zap.String("path", path),
			zap.Error(errors.New("path too long")))
		p.popConfirm(metric)
		return
	}
	filenames := strings.Split(path, "/")
	for _, filename := range filenames {
		if len(filename) > maxFilenameLength {
			p.logger.Debug("metric name exceeds limit",
				zap.String("path", path),
				zap.String("filename", filename),
				zap.Error(errors.New("filename too long")))
			p.popConfirm(metric)
			return
		}
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
		w, _, err = p.checkAndUpdateSchemaAndAggregation(w, metric)
		if err != nil {
			w, err = whisper.OpenWithOptions(path, &whisper.Options{
				FLock:      p.flock,
				Compressed: p.compressed,
			})
			if err != nil {
				p.logger.Error("failed to reopen whisper file after schema migration", zap.String("path", path), zap.Error(p.simplifyPathError(err)))
				p.popConfirm(metric)
				return
			}
		}
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

func (p *Whisper) worker(exit chan bool) {
	storeFunc := p.store
	var doneCb func()
	if p.mockStore != nil {
		storeFunc, doneCb = p.mockStore()
	}

LOOP:
	for {
		select {
		case <-p.updateTicker.C:
		case <-exit:
			return
		}

		metric := p.recv(exit)
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

	oooDiscardedPoints := atomic.LoadUint32(&p.oooDiscardedPoints)
	atomic.AddUint32(&p.oooDiscardedPoints, -oooDiscardedPoints)

	send("updateOperations", float64(updateOperations))
	send("committedPoints", float64(committedPoints))
	if updateOperations > 0 {
		send("pointsPerUpdate", float64(committedPoints)/float64(updateOperations))
	} else {
		send("pointsPerUpdate", 0.0)
	}

	send("created", float64(created))
	send("oooDiscardedPoints", float64(oooDiscardedPoints))
	send("maxUpdatesPerSecond", float64(p.maxUpdatesPerSecond))
	send("workers", float64(p.workersCount))
	send("extended", float64(extended))

	if p.onlineMigration.enabled {
		stat := &p.onlineMigration.stat

		total := atomic.LoadUint32(&stat.total)
		atomic.AddUint32(&stat.total, -total)
		send("onlineMigration.total", float64(total))

		errors := atomic.LoadUint32(&stat.errors)
		atomic.AddUint32(&stat.errors, -errors)
		send("onlineMigration.errors", float64(errors))

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

}

// Start worker
func (p *Whisper) Start() error {
	return p.StartFunc(func() error {
		p.updateTicker = helper.NewThrottleTicker(p.maxUpdatesPerSecond)
		if p.onlineMigration.enabled {
			p.onlineMigration.ticker = helper.NewHardThrottleTicker(p.onlineMigration.rate)
		}

		for i := 0; i < p.workersCount; i++ {
			p.Go(p.worker)
		}

		return nil
	})
}

func (p *Whisper) Stop() {
	p.StopFunc(func() {
		p.updateTicker.Stop()

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
		logger.Error("failed to retrieve file info before migration", zap.String("metric", metric), zap.Error(err))
	} else {
		virtualSize = fiOld.Size()
		physicalSize = virtualSize
		if stat, ok := fiOld.Sys().(*syscall.Stat_t); ok {
			physicalSize = stat.Blocks * 512
		}
	}

	// IMPORTANT: file has be re-opened after a call to Whisper.UpdateConfig
	if err := w.UpdateConfig(retentions, aggregationmethod, xFilesFactor, options); err != nil {
		logger.Error("failed to migrate/update configs (schema/aggregation/xff)", zap.String("metric", metric), zap.Error(err))
		atomic.AddUint32(&p.onlineMigration.stat.errors, 1)

		return w, false, err
	}

	// reopen whisper file
	nw, err := whisper.OpenWithOptions(w.File().Name(), &whisper.Options{
		FLock:      p.flock,
		Compressed: p.compressed,
	})

	if fiNew, err := nw.File().Stat(); err != nil {
		logger.Error("failed to retrieve file info after migration", zap.String("metric", metric), zap.Error(err))
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

// skipcq: RVV-A0005
func (p *Whisper) CheckPolicyConsistencies(rate int, printInconsistentMetrics bool) error {
	var summary struct {
		total              int
		errors             int
		brokenAggConfig    int
		brokenSchemaConfig int
		inconsistencies    struct {
			total       int
			aggregation int
			retention   int
			xff         int
		}
	}

	rateTicker := time.NewTicker(time.Second / time.Duration(rate))
	log.SetOutput(os.Stdout)

	printSummary := func() {
		pct := func(v int) float64 { return float64(v) / float64(summary.total) * 100 }
		log.Printf(
			"stats: total=%d errors=%d (%.2f%%) brokenAggConfig=%d (%.2f%%) brokenSchemaConfig=%d (%.2f%%) inconsistencies.total=%d (%.2f%%) inconsistencies.aggregation=%d (%.2f%%) inconsistencies.retention=%d (%.2f%%) inconsistencies.xff=%d (%.2f%%)\n",
			summary.total,
			summary.errors, pct(summary.errors),
			summary.brokenAggConfig, pct(summary.brokenAggConfig),
			summary.brokenSchemaConfig, pct(summary.brokenSchemaConfig),
			summary.inconsistencies.total, pct(summary.inconsistencies.total),
			summary.inconsistencies.aggregation, pct(summary.inconsistencies.aggregation),
			summary.inconsistencies.retention, pct(summary.inconsistencies.retention),
			summary.inconsistencies.xff, pct(summary.inconsistencies.xff),
		)
	}

	err := filepath.WalkDir(p.rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !strings.HasSuffix(path, ".wsp") || d.IsDir() {
			return err
		}

		<-rateTicker.C

		summary.total++

		metric := strings.TrimPrefix(strings.ReplaceAll(strings.TrimPrefix(strings.TrimSuffix(path, ".wsp"), p.rootPath), "/", "."), ".")

		schema, ok := p.schemas.Match(metric)
		if !ok {
			summary.brokenSchemaConfig++
			log.Printf("no storage schema defined for metric: %s", metric)
			return nil
		}
		aggr := p.aggregation.Match(metric)
		if aggr == nil {
			summary.brokenAggConfig++
			log.Printf("no storage aggregation defined for metric: %s", metric)
			return nil
		}

		file, err := whisper.OpenWithOptions(path, &whisper.Options{})
		if err != nil {
			summary.errors++
			log.Printf("failed to open %s: %s\n", path, err)
			return nil
		}
		defer file.Close()

		retentions := whisper.NewRetentionsNoPointer(file.Retentions())
		aggregationmethod := file.AggregationMethod()
		xFilesFactor := file.XFilesFactor()
		var inconsistencies []string
		if !retentions.Equal(schema.Retentions) {
			summary.inconsistencies.retention++
			inconsistencies = append(inconsistencies, "retention")
		}
		if xFilesFactor != float32(aggr.xFilesFactor) {
			summary.inconsistencies.xff++
			inconsistencies = append(inconsistencies, "xff")
		}
		if aggregationmethod != aggr.aggregationMethod {
			summary.inconsistencies.aggregation++
			inconsistencies = append(inconsistencies, "aggregation")
		}
		if len(inconsistencies) > 0 {
			summary.inconsistencies.total++
			if printInconsistentMetrics {
				log.Printf("inconsistent_metric: %s %s", strings.Join(inconsistencies, ","), metric)
			}
		}

		if (summary.total % 5000) == 0 {
			printSummary()
		}

		return nil
	})

	printSummary()

	return err
}
