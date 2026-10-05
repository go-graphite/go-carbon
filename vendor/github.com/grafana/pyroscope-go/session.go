package pyroscope

import (
	"bytes"
	"math"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/grafana/pyroscope-go/godeltaprof"
	"github.com/grafana/pyroscope-go/internal/semconv"
	"github.com/grafana/pyroscope-go/upstream"
)

type Session struct {
	// configuration, doesn't change
	upstream      upstream.Upstream
	profileTypes  []ProfileType
	uploadRate    time.Duration
	disableGCRuns bool
	// Deprecated: the field will be removed in future releases.
	DisableAutomaticResets bool

	logger   Logger
	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
	flushCh  chan *flush

	// these things do change:
	memBuf *bytes.Buffer

	goroutinesBuf    *bytes.Buffer
	goroutineLeakBuf *bytes.Buffer
	mutexBuf         *bytes.Buffer
	blockBuf         *bytes.Buffer

	lastGCGeneration uint32
	appNames         semconv.AppNames
	startTime        time.Time

	deltaBlock *godeltaprof.BlockProfiler
	deltaMutex *godeltaprof.BlockProfiler
	deltaHeap  *godeltaprof.HeapProfiler
	cpu        *cpuProfileCollector
}

type SessionConfig struct {
	Upstream       upstream.Upstream
	Logger         Logger
	AppName        string
	Tags           map[string]string
	ProfilingTypes []ProfileType
	DisableGCRuns  bool
	UploadRate     time.Duration

	// Deprecated: the field will be removed in future releases.
	// Use UploadRate instead.
	DisableAutomaticResets bool
	// Deprecated: the field will be removed in future releases.
	// DisableCumulativeMerge is ignored.
	DisableCumulativeMerge bool
	// Deprecated: the field will be removed in future releases.
	// SampleRate is set to 100 and is not configurable.
	SampleRate uint32
}

type flush struct {
	wg   sync.WaitGroup
	wait bool
}

func NewSession(c SessionConfig) (*Session, error) {
	if c.UploadRate == 0 {
		// For backward compatibility.
		c.UploadRate = 15 * time.Second
	}

	c.Logger.Infof("starting profiling session:")
	c.Logger.Infof("  AppName:        %+v", c.AppName)
	c.Logger.Infof("  Tags:           %+v", c.Tags)
	c.Logger.Infof("  ProfilingTypes: %+v", c.ProfilingTypes)
	c.Logger.Infof("  DisableGCRuns:  %+v", c.DisableGCRuns)
	c.Logger.Infof("  UploadRate:     %+v", c.UploadRate)

	if c.DisableAutomaticResets {
		c.UploadRate = math.MaxInt64
	}

	appNames, err := semconv.MergeTagsWithAppName(c.AppName, newSessionID().String(), c.Tags)
	if err != nil {
		return nil, err
	}

	// Warn if goroutine leak profiling is requested but not available.
	// The goroutineleak profile requires Go 1.26+ with GOEXPERIMENT=goroutineleakprofile.
	for _, pt := range c.ProfilingTypes {
		if pt == ProfileGoroutineLeak {
			if pprof.Lookup("goroutineleak") == nil {
				c.Logger.Infof("goroutine leak profiling requested but not available: " +
					"build with GOEXPERIMENT=goroutineleakprofile (requires Go 1.26+)")
			}

			break
		}
	}

	ps := &Session{
		upstream:         c.Upstream,
		appNames:         appNames,
		profileTypes:     c.ProfilingTypes,
		disableGCRuns:    c.DisableGCRuns,
		uploadRate:       c.UploadRate,
		stopCh:           make(chan struct{}),
		flushCh:          make(chan *flush),
		logger:           c.Logger,
		memBuf:           &bytes.Buffer{},
		goroutinesBuf:    &bytes.Buffer{},
		goroutineLeakBuf: &bytes.Buffer{},
		mutexBuf:         &bytes.Buffer{},
		blockBuf:         &bytes.Buffer{},

		deltaBlock: godeltaprof.NewBlockProfiler(),
		deltaMutex: godeltaprof.NewMutexProfiler(),
		deltaHeap:  godeltaprof.NewHeapProfiler(),
		cpu:        newCPUProfileCollector(appNames.SDK, c.Upstream, c.Logger, c.UploadRate),
	}

	return ps, nil
}

// revive:disable-next-line:cognitive-complexity complexity is fine
func (ps *Session) takeSnapshots() {
	t := time.NewTicker(ps.uploadRate)
	defer t.Stop()
	for {
		select {
		case endTime := <-t.C:
			ps.reset(ps.startTime, endTime)

		case f := <-ps.flushCh:
			ps.reset(ps.startTime, ps.truncatedTime())
			_ = ps.cpu.Flush()
			ps.upstream.Flush()
			f.wg.Done()

		case <-ps.stopCh:
			if ps.isCPUEnabled() {
				ps.cpu.Stop()
			}

			return
		}
	}
}

func copyBuf(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)

	return r
}

func (ps *Session) Start() error {
	t := ps.truncatedTime()
	ps.reset(t, t)

	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		ps.takeSnapshots()
	}()

	if ps.isCPUEnabled() {
		ps.wg.Add(1)
		go func() {
			defer ps.wg.Done()
			ps.cpu.Start()
		}()
	}

	return nil
}

func (ps *Session) isCPUEnabled() bool {
	for _, t := range ps.profileTypes {
		if t == ProfileCPU {
			return true
		}
	}

	return false
}

func (ps *Session) isMemEnabled() bool {
	for _, t := range ps.profileTypes {
		if t == ProfileInuseObjects || t == ProfileAllocObjects || t == ProfileInuseSpace || t == ProfileAllocSpace {
			return true
		}
	}

	return false
}

func (ps *Session) isBlockEnabled() bool {
	for _, t := range ps.profileTypes {
		if t == ProfileBlockCount || t == ProfileBlockDuration {
			return true
		}
	}

	return false
}

func (ps *Session) isMutexEnabled() bool {
	for _, t := range ps.profileTypes {
		if t == ProfileMutexCount || t == ProfileMutexDuration {
			return true
		}
	}

	return false
}

func (ps *Session) isGoroutinesEnabled() bool {
	for _, t := range ps.profileTypes {
		if t == ProfileGoroutines {
			return true
		}
	}

	return false
}

func (ps *Session) isGoroutineLeakEnabled() bool {
	for _, t := range ps.profileTypes {
		if t == ProfileGoroutineLeak {
			return true
		}
	}

	return false
}

func (ps *Session) reset(startTime, endTime time.Time) {
	ps.logger.Debugf("profiling session reset %s", startTime.String())
	// first reset should not result in an upload
	if !ps.startTime.IsZero() {
		ps.uploadData(startTime, endTime)
	}
	ps.startTime = endTime
}

func (ps *Session) uploadData(startTime, endTime time.Time) {
	if ps.isGoroutinesEnabled() {
		p := pprof.Lookup("goroutine")
		if p != nil {
			err := p.WriteTo(ps.goroutinesBuf, 0)
			if err != nil {
				ps.logger.Errorf("failed to dump goroutines profile: %s", err)

				return
			}
			ps.upstream.Upload(&upstream.UploadJob{
				Name:            ps.appNames.SDK,
				StartTime:       startTime,
				EndTime:         endTime,
				SpyName:         "gospy",
				Units:           "goroutines",
				AggregationType: "average",
				Format:          upstream.FormatPprof,
				Profile:         copyBuf(ps.goroutinesBuf.Bytes()),
				SampleTypeConfig: map[string]*upstream.SampleType{
					"goroutine": {
						DisplayName: "goroutines",
						Units:       "goroutines",
						Aggregation: "average",
					},
				},
			})
			ps.goroutinesBuf.Reset()
		}
	}

	if ps.isGoroutineLeakEnabled() {
		p := pprof.Lookup("goroutineleak")
		if p != nil {
			err := p.WriteTo(ps.goroutineLeakBuf, 0)
			if err != nil {
				ps.logger.Errorf("failed to dump goroutine leak profile: %s", err)

				return
			}
			ps.upstream.Upload(&upstream.UploadJob{
				Name:             ps.appNames.SDK,
				StartTime:        startTime,
				EndTime:          endTime,
				SpyName:          "gospy",
				Units:            "goroutines",
				AggregationType:  "average",
				Format:           upstream.FormatPprof,
				Profile:          copyBuf(ps.goroutineLeakBuf.Bytes()),
				SampleTypeConfig: sampleTypeConfigGoroutineLeak,
			})
			ps.goroutineLeakBuf.Reset()
		}
	}

	if ps.isBlockEnabled() {
		ps.dumpBlockProfile(startTime, endTime)
	}
	if ps.isMutexEnabled() {
		ps.dumpMutexProfile(startTime, endTime)
	}
	if ps.isMemEnabled() {
		ps.dumpHeapProfile(startTime, endTime)
	}
}

func (ps *Session) dumpHeapProfile(startTime time.Time, endTime time.Time) {
	defer func() {
		if r := recover(); r != nil {
			ps.logger.Errorf("dump heap profiler panic %s", string(debug.Stack()))
		}
	}()
	currentGCGeneration := numGC()
	// sometimes GC doesn't run within 10 seconds
	//   in such cases we force a GC run
	//   users can disable it with disableGCRuns option
	if currentGCGeneration == ps.lastGCGeneration && !ps.disableGCRuns {
		runtime.GC()
		currentGCGeneration = numGC()
	}
	if currentGCGeneration != ps.lastGCGeneration {
		ps.memBuf.Reset()
		err := ps.deltaHeap.Profile(ps.memBuf)
		if err != nil {
			ps.logger.Errorf("failed to dump heap profile: %s", err)

			return
		}
		curMemBytes := copyBuf(ps.memBuf.Bytes())
		job := &upstream.UploadJob{
			Name:             ps.appNames.Godeltaprof,
			StartTime:        startTime,
			EndTime:          endTime,
			SpyName:          "gospy",
			SampleRate:       100,
			Format:           upstream.FormatPprof,
			Profile:          curMemBytes,
			SampleTypeConfig: sampleTypeConfigHeap,
		}
		ps.upstream.Upload(job)
		ps.lastGCGeneration = currentGCGeneration
	}
}

func (ps *Session) dumpMutexProfile(startTime time.Time, endTime time.Time) {
	defer func() {
		if r := recover(); r != nil {
			ps.logger.Errorf("dump mutex profiler panic %s", string(debug.Stack()))
		}
	}()
	ps.mutexBuf.Reset()
	err := ps.deltaMutex.Profile(ps.mutexBuf)
	if err != nil {
		ps.logger.Errorf("failed to dump mutex profile: %s", err)

		return
	}
	curMutexBuf := copyBuf(ps.mutexBuf.Bytes())
	job := &upstream.UploadJob{
		Name:             ps.appNames.Godeltaprof,
		StartTime:        startTime,
		EndTime:          endTime,
		SpyName:          "gospy",
		Format:           upstream.FormatPprof,
		Profile:          curMutexBuf,
		SampleTypeConfig: sampleTypeConfigMutex,
	}
	ps.upstream.Upload(job)
}

func (ps *Session) dumpBlockProfile(startTime time.Time, endTime time.Time) {
	defer func() {
		if r := recover(); r != nil {
			ps.logger.Errorf("dump block profiler panic %s", string(debug.Stack()))
		}
	}()
	ps.blockBuf.Reset()
	err := ps.deltaBlock.Profile(ps.blockBuf)
	if err != nil {
		ps.logger.Errorf("failed to dump block profile: %s", err)

		return
	}
	curBlockBuf := copyBuf(ps.blockBuf.Bytes())
	job := &upstream.UploadJob{
		Name:             ps.appNames.Godeltaprof,
		StartTime:        startTime,
		EndTime:          endTime,
		SpyName:          "gospy",
		Format:           upstream.FormatPprof,
		Profile:          curBlockBuf,
		SampleTypeConfig: sampleTypeConfigBlock,
	}
	ps.upstream.Upload(job)
}

func (ps *Session) Stop() {
	ps.stopOnce.Do(func() {
		close(ps.stopCh)
		ps.wg.Wait()
	})
}

func (ps *Session) flush(wait bool) {
	f := &flush{
		wg:   sync.WaitGroup{},
		wait: wait,
	}
	f.wg.Add(1)
	ps.flushCh <- f
	if wait {
		f.wg.Wait()
	}
}

func (ps *Session) truncatedTime() time.Time {
	return time.Now().Truncate(ps.uploadRate)
}

func numGC() uint32 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return memStats.NumGC
}
