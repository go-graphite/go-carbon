/*
 * Copyright 2013-2016 Fabian Groffen, Damian Gryski, Vladimir Smirnov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package carbonserver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof" // skipcq: GO-S2108
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"go.uber.org/zap"

	"github.com/NYTimes/gziphandler"
	"github.com/dgryski/go-expirecache"
	"github.com/dgryski/go-trigram"
	"github.com/dgryski/httputil"
	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/helper/grpcutil"
	"github.com/go-graphite/go-carbon/helper/stat"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
	grpcv2 "github.com/go-graphite/protocol/carbonapi_v2_grpc"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/zapwriter"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type metricStruct struct {
	RenderRequests                       uint64
	NotFound                             uint64
	FindRequests                         uint64
	FindZero                             uint64
	InfoRequests                         uint64
	ListRequests                         uint64
	ListQueryRequests                    uint64
	DetailsRequests                      uint64
	CacheHit                             uint64
	CacheMiss                            uint64
	CacheRequestsTotal                   uint64
	CacheWorkTimeNS                      uint64
	CacheWaitTimeFetchNS                 uint64
	DiskWaitTimeNS                       uint64
	DiskRequests                         uint64
	PointsReturned                       uint64
	MetricsReturned                      uint64
	MetricsKnown                         uint64
	OOOFiles                             uint64
	OOOPhysicalBytes                     uint64
	LockFiles                            uint64
	FileScanTimeNS                       uint64
	IndexBuildTimeNS                     uint64
	MetricsFetched                       uint64
	ThrottledCreates                     uint64
	MaxCreatesPerSecond                  uint64
	FetchSize                            uint64
	QueryCacheHit                        uint64
	QueryCacheMiss                       uint64
	FindCacheHit                         uint64
	FindCacheMiss                        uint64
	findMetricsFoundWithoutResponseCache uint64
	findExpandedGlobsCachedHit           uint64
	findExpandedGlobsCacheMiss           uint64
	renderExpandedGlobsCacheHit          uint64
	renderExpandedGlobsCacheMiss         uint64
	TrieNodes                            uint64
	TrieFiles                            uint64
	TrieDirs                             uint64
	TrieCountNodesTimeNs                 uint64
	QuotaApplyTimeNs                     uint64
	UsageRefreshTimeNs                   uint64

	InflightRequests        uint64
	RejectedTooManyRequests uint64
}

type requestsTimes struct {
	sync.RWMutex
	list []int64
}

const (
	QueryIsPending uint64 = 1 << iota
	DataIsAvailable
)

type QueryItem struct {
	Data          atomic.Value
	Flags         uint64 // DataIsAvailable or QueryIsPending
	QueryFinished chan struct{}
}

// TODO merge with globs struct
type ExpandedGlobResponse struct {
	Name      string
	Files     []string
	Leafs     []bool
	TrieNodes []*trieNode
	Lookups   uint32
	Err       error
}

var statusCodes = map[string][]uint64{
	"combined":     make([]uint64, 5),
	"find":         make([]uint64, 5),
	"list":         make([]uint64, 5),
	"render":       make([]uint64, 5),
	"details":      make([]uint64, 5),
	"info":         make([]uint64, 5),
	"capabilities": make([]uint64, 5),
}

// interface to retrieve retention and aggregation
// schema from persister.
type configRetriever interface {
	MetricRetentionPeriod(string) (int, bool)
	MetricAggrConf(string) (string, float64, bool)
}

type responseWriterWithStatus struct {
	http.ResponseWriter
	statusCode int
}

func (rw responseWriterWithStatus) statusCodeMajor() int {
	return rw.statusCode/100 - 1
}

func newResponseWriterWithStatus(w http.ResponseWriter) *responseWriterWithStatus {
	return &responseWriterWithStatus{
		w,
		http.StatusOK,
	}
}

func (w *responseWriterWithStatus) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func (q *QueryItem) FetchOrLock() (interface{}, bool) {
	d := q.Data.Load()
	if d != nil {
		return d, true
	}

	ok := atomic.CompareAndSwapUint64(&q.Flags, 0, QueryIsPending)
	if ok {
		// We are the leader now and will be fetching the data
		return nil, false
	}

	select { //nolint:gosimple //skipcq: SCC-S1000
	// TODO: Add timeout support
	case <-q.QueryFinished:
		break
	}

	return q.Data.Load(), true
}

func (q *QueryItem) StoreAbort() {
	oldChan := q.QueryFinished
	q.QueryFinished = make(chan struct{})
	close(oldChan)
	atomic.StoreUint64(&q.Flags, 0)
}

func (q *QueryItem) StoreAndUnlock(data interface{}) {
	q.Data.Store(data)
	atomic.StoreUint64(&q.Flags, DataIsAvailable)
	close(q.QueryFinished)
}

type expireCache struct {
	ec *expirecache.Cache
}

func (q *expireCache) getQueryItem(k string, size uint64, expire int32) *QueryItem {
	emptyQueryItem := &QueryItem{QueryFinished: make(chan struct{})}
	return q.ec.GetOrSet(k, emptyQueryItem, size, expire).(*QueryItem)
}

type CarbonserverListener struct {
	grpcv2.UnimplementedCarbonV2Server
	helper.Stoppable
	cacheGet          func(key string) []points.Point
	readTimeout       time.Duration
	idleTimeout       time.Duration
	writeTimeout      time.Duration
	requestTimeout    time.Duration
	whisperData       string
	buckets           int
	maxGlobs          int
	emptyResultOk     bool
	doNotLog404s      bool
	failOnMaxGlobs    bool
	percentiles       []int
	scanFrequency     time.Duration
	scanTicker        *time.Ticker
	forceScanChan     chan struct{}
	indexWarmupDone   chan struct{}
	indexWarmupOnce   sync.Once
	indexWorkers      sync.WaitGroup
	stopOnce          sync.Once
	metricsAsCounters bool
	tcpListener       *net.TCPListener
	grpcListener      *net.TCPListener
	httpServer        *http.Server
	grpcServer        *grpc.Server
	serverWG          sync.WaitGroup
	logger            *zap.Logger
	accessLogger      *zap.Logger
	internalStatsDir  string
	flock             bool
	compressed        bool
	removeEmptyFile   bool

	maxMetricsGlobbed      int
	maxMetricsRendered     int
	maxFetchDataGoroutines int

	queryCacheEnabled          bool
	streamingQueryCacheEnabled bool
	queryCacheSizeMB           int
	queryCache                 expireCache
	findCacheEnabled           bool
	findCacheSizeMB            int
	findCache                  expireCache
	globCacheEnabled           bool
	globCacheSizeMB            int
	globCache                  expireCache
	trigramIndex               bool
	trieIndex                  bool
	concurrentIndex            bool

	fileListCacheVersion FLCVersion
	fileListCache        string

	realtimeIndex  int
	newMetricsChan chan string

	fileIdx            atomic.Value
	fileIdxMutex       sync.Mutex
	metricStoreIndexMu sync.Mutex
	sharedRequestMu    sync.RWMutex
	sharedStoreStopped bool

	metrics       *metricStruct
	requestsTimes requestsTimes
	exitChan      chan struct{}
	timeBuckets   []uint64

	cacheGetRecentMetrics func() []map[string]struct{}
	whisperGetConfig      configRetriever
	metricStoreMu         sync.RWMutex
	metricStore           *store.Store

	prometheus prometheus

	db *leveldb.DB

	quotas                    atomic.Value // []*Quota; immutable after publication
	quotaReload               chan struct{}
	estimateSize              func(metric string) (logicalSize, physicalSize, dataPoints int64)
	quotaAndUsageMetrics      chan []points.Points
	quotaUsageReportFrequency time.Duration
	maxCreatesPerSecond       int

	interfalInfoCallbacks map[string]func() map[string]interface{}

	// resource control
	MaxInflightRequests          uint64 // TODO: to deprecate
	NoServiceWhenIndexIsNotReady bool
	apiPerPathRatelimiter        map[string]*ApiPerPathRatelimiter
	globQueryRateLimiters        []*GlobQueryRateLimiter

	renderTraceLoggingEnabled bool
}

type prometheus struct {
	enabled bool

	requests *prom.CounterVec
	request  func(string, int)

	durations prom.Histogram
	duration  func(time.Duration)

	cacheRequests *prom.CounterVec
	cacheRequest  func(string, bool)

	cacheDurations *prom.HistogramVec
	cacheDuration  func(string, time.Duration)

	diskRequests      prom.Counter
	diskRequest       func()
	cancelledRequests prom.Counter
	cancelledRequest  func()
	timeoutRequests   prom.Counter
	timeoutRequest    func()
	diskWaitDurations prom.Histogram
	diskWaitDuration  func(time.Duration)

	returnedMetrics prom.Counter
	returnedMetric  func()
	returnedPoints  prom.Counter
	returnedPoint   func(int)
}

func (c *CarbonserverListener) InitPrometheus(reg prom.Registerer) {
	c.prometheus = prometheus{
		enabled: true,

		requests: prom.NewCounterVec(
			prom.CounterOpts{
				Name: "http_requests_total",
				Help: "How many HTTP requests processed, partitioned by status code and handler",
			},
			[]string{"code", "handler"},
		),

		cacheRequests: prom.NewCounterVec(
			prom.CounterOpts{
				Name: "cache_requests_total",
				Help: "Cache counts, partitioned by type and hit/miss",
			},
			[]string{"type", "hit"},
		),

		durations: prom.NewHistogram(
			prom.HistogramOpts{
				Name:    "http_request_duration_seconds_exp",
				Help:    "Duration of HTTP requests (exponential buckets)",
				Buckets: prom.ExponentialBuckets(time.Millisecond.Seconds(), 2.0, 20),
			},
		),

		cacheDurations: prom.NewHistogramVec(
			prom.HistogramOpts{
				Name:    "cache_duration_seconds_exp",
				Help:    "Time spent in cache (exponential buckets)",
				Buckets: prom.ExponentialBuckets(time.Millisecond.Seconds(), 2.0, 20),
			},
			[]string{"type"},
		),

		diskRequests: prom.NewCounter(prom.CounterOpts{
			Name: "disk_requests_total",
			Help: "Number of times disk has been hit",
		}),
		cancelledRequests: prom.NewCounter(prom.CounterOpts{
			Name: "cancelled_requests_total",
			Help: "Number of times a request has been cancelled",
		}),
		timeoutRequests: prom.NewCounter(prom.CounterOpts{
			Name: "timeout_requests_total",
			Help: "Number of times a request has been timeout",
		}),
		diskWaitDurations: prom.NewHistogram(
			prom.HistogramOpts{
				Name:    "disk_wait_seconds_exp",
				Help:    "Duration of disk wait times (exponential buckets)",
				Buckets: prom.ExponentialBuckets(time.Millisecond.Seconds(), 2.0, 20),
			},
		),

		returnedMetrics: prom.NewCounter(prom.CounterOpts{
			Name: "returned_metrics_total",
			Help: "Number of metrics returned",
		}),
		returnedPoints: prom.NewCounter(prom.CounterOpts{
			Name: "returned_points_total",
			Help: "Number of points returned",
		}),
	}

	c.prometheus.request = func(endpoint string, code int) {
		c.prometheus.requests.WithLabelValues(strconv.Itoa(code), endpoint).Inc()
	}

	c.prometheus.cacheRequest = func(kind string, hit bool) {
		c.prometheus.cacheRequests.WithLabelValues(kind, strconv.FormatBool(hit))
	}

	c.prometheus.duration = func(t time.Duration) {
		c.prometheus.durations.Observe(t.Seconds())
	}

	c.prometheus.cacheDuration = func(kind string, t time.Duration) {
		c.prometheus.cacheDurations.WithLabelValues(kind).Observe(t.Seconds())
	}

	c.prometheus.diskRequest = func() {
		c.prometheus.diskRequests.Inc()
	}

	c.prometheus.cancelledRequest = func() {
		c.prometheus.cancelledRequests.Inc()
	}

	c.prometheus.timeoutRequest = func() {
		c.prometheus.timeoutRequests.Inc()
	}

	c.prometheus.diskWaitDuration = func(t time.Duration) {
		c.prometheus.diskWaitDurations.Observe(t.Seconds())
	}

	c.prometheus.returnedMetric = func() {
		c.prometheus.returnedMetrics.Inc()
	}

	c.prometheus.returnedPoint = func(i int) {
		c.prometheus.returnedPoints.Add(float64(i))
	}

	reg.MustRegister(c.prometheus.requests)
	reg.MustRegister(c.prometheus.cacheRequests)
	reg.MustRegister(c.prometheus.cancelledRequests)
	reg.MustRegister(c.prometheus.timeoutRequests)
	reg.MustRegister(c.prometheus.durations)
	reg.MustRegister(c.prometheus.diskRequests)
	reg.MustRegister(c.prometheus.diskWaitDurations)
	reg.MustRegister(c.prometheus.returnedMetrics)
	reg.MustRegister(c.prometheus.returnedPoints)
}

type metricDetailsFlat struct {
	*protov3.MetricDetails
	Name string
}

type jsonMetricDetailsResponse struct {
	Metrics    []metricDetailsFlat
	FreeSpace  uint64
	TotalSpace uint64
}

type fileIndex struct {
	typ int //nolint:unused //skipcq: SCC-U1000

	idx   trigram.Index
	files []string

	trieIdx *trieIndex

	details     map[string]*protov3.MetricDetails
	accessTimes map[string]int64
	freeSpace   uint64
	totalSpace  uint64
}

func NewCarbonserverListener(cacheGetFunc func(key string) []points.Point) *CarbonserverListener {
	return &CarbonserverListener{
		// Config variables
		metrics:           &metricStruct{},
		exitChan:          make(chan struct{}),
		metricsAsCounters: false,
		cacheGet:          cacheGetFunc,
		logger:            zapwriter.Logger("carbonserver"),
		accessLogger:      zapwriter.Logger("access"),
		findCache:         expireCache{ec: expirecache.New(0)},
		globCache:         expireCache{ec: expirecache.New(0)},
		trigramIndex:      true,
		percentiles:       []int{100, 99, 98, 95, 75, 50},
		prometheus: prometheus{
			request:          func(string, int) {},
			duration:         func(time.Duration) {},
			cacheRequest:     func(string, bool) {},
			cacheDuration:    func(string, time.Duration) {},
			diskRequest:      func() {},
			cancelledRequest: func() {},
			timeoutRequest:   func() {},
			diskWaitDuration: func(time.Duration) {},
			returnedMetric:   func() {},
			returnedPoint:    func(int) {},
		},
		quotaAndUsageMetrics:  make(chan []points.Points, 1),
		quotaReload:           make(chan struct{}, 1),
		apiPerPathRatelimiter: map[string]*ApiPerPathRatelimiter{},
		fileListCacheVersion:  FLCVersion1,
	}
}

func (listener *CarbonserverListener) SetWhisperData(whisperData string) {
	listener.whisperData = strings.TrimRight(whisperData, "/")
}

// SetMetricStore makes carbonserver read metric data and its catalog from an
// embedded shared Whisper store. The caller owns the store lifecycle.
func (listener *CarbonserverListener) SetMetricStore(metricStore *store.Store) {
	listener.metricStoreMu.Lock()
	listener.metricStore = metricStore
	listener.metricStoreMu.Unlock()
	listener.sharedRequestMu.Lock()
	listener.sharedStoreStopped = false
	listener.sharedRequestMu.Unlock()
}

func (listener *CarbonserverListener) getMetricStore() *store.Store {
	listener.metricStoreMu.RLock()
	defer listener.metricStoreMu.RUnlock()
	return listener.metricStore
}

// beginSharedStoreRequest keeps a shared-store request alive through its
// storage read, or rejects one that races listener shutdown.
func (listener *CarbonserverListener) beginSharedStoreRequest() (accepted, locked bool) {
	if listener.getMetricStore() == nil {
		return true, false
	}
	listener.sharedRequestMu.RLock()
	if listener.sharedStoreStopped {
		listener.sharedRequestMu.RUnlock()
		return false, false
	}
	return true, true
}

// stopSharedStoreRequests rejects new shared-store reads and waits for in-flight
// ones. Store reads cannot be cancelled, so a read stuck on the disk must not
// block shutdown forever.
func (listener *CarbonserverListener) stopSharedStoreRequests(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		listener.sharedRequestMu.Lock()
		listener.sharedStoreStopped = true
		listener.sharedRequestMu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		listener.logger.Warn("shared-store requests still in flight after timeout; continuing shutdown",
			zap.Duration("timeout", timeout))
	}
}

func (listener *CarbonserverListener) endSharedStoreRequest(locked bool) {
	if locked {
		listener.sharedRequestMu.RUnlock()
	}
}

// RefreshMetricStoreIndex rebuilds the in-memory index from the shared store
// catalog. Existing indexes stay live if catalog traversal fails.
func (listener *CarbonserverListener) RefreshMetricStoreIndex() error {
	metricStore := listener.getMetricStore()
	if metricStore == nil {
		return nil
	}
	return listener.updateMetricStoreIndex(metricStore)
}

func (listener *CarbonserverListener) SetMaxGlobs(maxGlobs int) {
	listener.maxGlobs = maxGlobs
}
func (listener *CarbonserverListener) SetEmptyResultOk(emptyResultOk bool) {
	listener.emptyResultOk = emptyResultOk
}
func (listener *CarbonserverListener) SetDoNotLog404s(doNotLog404s bool) {
	listener.doNotLog404s = doNotLog404s
}
func (listener *CarbonserverListener) SetFailOnMaxGlobs(failOnMaxGlobs bool) {
	listener.failOnMaxGlobs = failOnMaxGlobs
}
func (listener *CarbonserverListener) SetMaxMetricsGlobbed(m int) {
	listener.maxMetricsGlobbed = m
}
func (listener *CarbonserverListener) SetMaxMetricsRendered(m int) {
	listener.maxMetricsRendered = m
}
func (listener *CarbonserverListener) SetMaxFetchDataGoroutines(m int) {
	listener.maxFetchDataGoroutines = m
}
func (listener *CarbonserverListener) SetFLock(flock bool) {
	listener.flock = flock
}
func (listener *CarbonserverListener) SetBuckets(buckets int) {
	listener.buckets = buckets
}
func (listener *CarbonserverListener) SetScanFrequency(scanFrequency time.Duration) {
	listener.scanFrequency = scanFrequency
}
func (listener *CarbonserverListener) SetQuotaUsageReportFrequency(quotaUsageReportFrequency time.Duration) {
	listener.quotaUsageReportFrequency = quotaUsageReportFrequency
}
func (listener *CarbonserverListener) SetMaxCreatesPerSecond(maxCreatesPerSecond int) {
	listener.maxCreatesPerSecond = maxCreatesPerSecond
}
func (listener *CarbonserverListener) SetReadTimeout(readTimeout time.Duration) {
	listener.readTimeout = readTimeout
}
func (listener *CarbonserverListener) SetIdleTimeout(idleTimeout time.Duration) {
	listener.idleTimeout = idleTimeout
}
func (listener *CarbonserverListener) SetWriteTimeout(writeTimeout time.Duration) {
	listener.writeTimeout = writeTimeout
}
func (listener *CarbonserverListener) SetRequestTimeout(requestTimeout time.Duration) {
	listener.requestTimeout = requestTimeout
}
func (listener *CarbonserverListener) SetCompressed(compressed bool) {
	listener.compressed = compressed
}
func (listener *CarbonserverListener) SetRemoveEmptyFile(remove bool) {
	listener.removeEmptyFile = remove
}
func (listener *CarbonserverListener) SetMetricsAsCounters(metricsAsCounters bool) {
	listener.metricsAsCounters = metricsAsCounters
}
func (listener *CarbonserverListener) SetQueryCacheEnabled(enabled bool) {
	listener.queryCacheEnabled = enabled
}
func (listener *CarbonserverListener) SetQueryCacheSizeMB(size int) {
	listener.queryCacheSizeMB = size
}
func (listener *CarbonserverListener) SetStreamingQueryCacheEnabled(enabled bool) {
	listener.streamingQueryCacheEnabled = enabled
}
func (listener *CarbonserverListener) SetFindCacheEnabled(enabled bool) {
	listener.findCacheEnabled = enabled
}
func (listener *CarbonserverListener) SetFindCacheSizeMB(size int) {
	listener.findCacheSizeMB = size
}
func (listener *CarbonserverListener) SetGlobCacheEnabled(enabled bool) {
	listener.globCacheEnabled = enabled
}
func (listener *CarbonserverListener) SetGlobCacheSizeMB(size int) {
	listener.globCacheSizeMB = size
}
func (listener *CarbonserverListener) SetTrigramIndex(enabled bool) {
	listener.trigramIndex = enabled
}
func (listener *CarbonserverListener) SetTrieIndex(enabled bool) {
	listener.trieIndex = enabled
}
func (listener *CarbonserverListener) SetCacheGetMetricsFunc(recentMetricsFunc func() []map[string]struct{}) {
	listener.cacheGetRecentMetrics = recentMetricsFunc
}
func (listener *CarbonserverListener) SetConfigRetriever(retriever configRetriever) {
	listener.whisperGetConfig = retriever
}
func (listener *CarbonserverListener) SetConcurrentIndex(enabled bool) {
	listener.concurrentIndex = enabled
}
func (listener *CarbonserverListener) SetRealtimeIndex(num int) chan string {
	listener.realtimeIndex = num
	listener.newMetricsChan = make(chan string, num)
	return listener.newMetricsChan
}
func (listener *CarbonserverListener) SetFileListCacheVersion(version int) {
	listener.fileListCacheVersion = FLCVersion(version)
}
func (listener *CarbonserverListener) SetFileListCache(path string) {
	listener.fileListCache = path
}
func (listener *CarbonserverListener) SetInternalStatsDir(dbPath string) {
	listener.internalStatsDir = dbPath
}
func (listener *CarbonserverListener) SetPercentiles(percentiles []int) {
	listener.percentiles = percentiles
}
func (listener *CarbonserverListener) SetEstimateSize(f func(metric string) (logicalSize, physicalSize, dataPoints int64)) {
	listener.estimateSize = f
}
func (listener *CarbonserverListener) SetQuotas(quotas []*Quota) {
	listener.quotas.Store(quotas)
}
func (listener *CarbonserverListener) isQuotaEnabled() bool {
	return listener.getQuotas() != nil
}
func (listener *CarbonserverListener) ShouldThrottleMetric(ps *points.Points, inCache bool) bool {
	fidx := listener.CurrentFileIndex()
	if fidx == nil || fidx.trieIdx == nil {
		return false
	}

	var throttled = fidx.trieIdx.throttle(ps, inCache)

	return throttled
}

// MetricExists checks the published concurrent trie without glob expansion.
// An unavailable or non-concurrent index must not suppress notifications.
func (listener *CarbonserverListener) MetricExists(metric string) bool {
	if !listener.trieIndex || !listener.concurrentIndex {
		return false
	}
	fidx := listener.CurrentFileIndex()
	if fidx == nil || fidx.trieIdx == nil {
		return false
	}
	_, isNew := fidx.trieIdx.metricPath(metric, nil)
	return !isNew
}

// WarmupIndex loads the saved trie before the HTTP listener starts. It does not
// scan the filesystem: restore may still be creating files, and its disk writes
// should not compete with a full directory walk. Listen starts that walk after
// warmup finishes, including when the saved index is absent or corrupt.
// Configure the listener fully before calling this method.
func (listener *CarbonserverListener) WarmupIndex() {
	if listener.getMetricStore() != nil || !listener.trieIndex || listener.scanFrequency == 0 || listener.fileListCache == "" {
		return
	}
	listener.indexWarmupOnce.Do(func() {
		listener.indexWarmupDone = make(chan struct{})
		listener.indexWorkers.Add(1)
		go func() {
			defer listener.indexWorkers.Done()
			defer close(listener.indexWarmupDone)
			listener.updateFileListWithCache(listener.whisperData, nil, nil, true)
		}()
	})
}

func (listener *CarbonserverListener) SetMaxInflightRequests(m uint64) {
	listener.MaxInflightRequests = m
}
func (listener *CarbonserverListener) SetNoServiceWhenIndexIsNotReady(no bool) {
	listener.NoServiceWhenIndexIsNotReady = no
}
func (listener *CarbonserverListener) SetHeavyGlobQueryRateLimiters(rls []*GlobQueryRateLimiter) {
	listener.globQueryRateLimiters = rls
}
func (listener *CarbonserverListener) SetAPIPerPathRateLimiter(rls map[string]*ApiPerPathRatelimiter) {
	listener.apiPerPathRatelimiter = rls
}

func (listener *CarbonserverListener) SetRenderTraceLoggingEnabled(enabled bool) {
	listener.renderTraceLoggingEnabled = enabled
}

// skipcq: RVV-B0011
func (listener *CarbonserverListener) CurrentFileIndex() *fileIndex {
	p := listener.fileIdx.Load()
	if p == nil {
		return nil
	}
	return p.(*fileIndex)
}

func (listener *CarbonserverListener) UpdateFileIndex(fidx *fileIndex) { listener.fileIdx.Store(fidx) }

// skipcq: RVV-A0005
func (listener *CarbonserverListener) UpdateMetricsAccessTimes(metrics map[string]int64, initial bool) {
	idx := listener.CurrentFileIndex()
	if idx == nil {
		return
	}
	listener.fileIdxMutex.Lock()
	defer listener.fileIdxMutex.Unlock()

	batch := new(leveldb.Batch)
	for m, t := range metrics {
		if _, ok := idx.details[m]; ok {
			idx.details[m].RdTime = t
		} else {
			idx.details[m] = &protov3.MetricDetails{RdTime: t}
		}
		idx.accessTimes[m] = t

		if !initial && listener.db != nil {
			buf := make([]byte, 10)
			binary.PutVarint(buf, t)
			batch.Put([]byte(m), buf)
		}
	}

	if !initial && listener.db != nil {
		err := listener.db.Write(batch, nil)
		if err != nil {
			listener.logger.Info("Error updating database",
				zap.Error(err),
			)
		}
	}
}

func (listener *CarbonserverListener) UpdateMetricsAccessTimesByRequest(metrics []string) {
	now := time.Now().Unix()

	accessTimes := make(map[string]int64)
	for _, m := range metrics {
		accessTimes[m] = now
	}

	listener.UpdateMetricsAccessTimes(accessTimes, false)
}

func splitAndInsert(cacheMetricNames map[string]struct{}, newCacheMetricNames []map[string]struct{}) map[string]struct{} {
	// splits each new metric from cache-scan and inserts
	// into the current cacheMetricNames map
	// in: new.metric.name1 --> split by "."
	// insert "/new" , "/new/metric", "/new/metric/name1.wsp" into the
	// metricsName map. This is inline with the inserts
	// during filescan walk
	for _, shardAddMap := range newCacheMetricNames {
		for newMetric := range shardAddMap {
			split := strings.Split(newMetric, ".")
			fileName := "/"
			for i, seg := range split {
				fileName = filepath.Join(fileName, seg)
				if i == len(split)-1 {
					fileName += ".wsp"
				}
				if _, ok := cacheMetricNames[fileName]; !ok {
					cacheMetricNames[fileName] = struct{}{}
				}
			}
		}
	}
	return cacheMetricNames
}

// waitForInitialIndex orders periodic updates after initial publication and
// allows shutdown while the startup index is still being constructed.
func (listener *CarbonserverListener) waitForInitialIndex(exit <-chan struct{}) bool {
	if listener.indexWarmupDone == nil {
		return true
	}
	select {
	case <-listener.indexWarmupDone:
		return true
	case <-exit:
		return false
	}
}

// fileListStatTickers selects quota accounting or lightweight metric counts.
// The caller owns the returned stop function, including the no-ticker case.
func (listener *CarbonserverListener) fileListStatTickers() (known, quotas <-chan time.Time, stop func()) {
	if listener.isQuotaEnabled() {
		ticker := time.NewTicker(listener.quotaUsageReportFrequency)
		return nil, ticker.C, ticker.Stop
	}
	if listener.trieIndex && listener.concurrentIndex && listener.realtimeIndex > 0 {
		ticker := time.NewTicker(time.Minute)
		return ticker.C, nil, ticker.Stop
	}
	return nil, nil, func() {}
}

func (listener *CarbonserverListener) fileListUpdater(dir string, scanFrequency <-chan time.Time, force <-chan struct{}, exit <-chan struct{}) {
	if !listener.waitForInitialIndex(exit) {
		return
	}
	cacheMetricNames := make(map[string]struct{})
	knownMetricsStatTicker, quotaAndUsageStatTicker, stopTickers := listener.fileListStatTickers()
	defer stopTickers()

uloop:
	for {
		fidx := listener.CurrentFileIndex()
		var newMetricsChan <-chan string
		if listener.trieIndex && listener.concurrentIndex && fidx != nil && fidx.trieIdx != nil {
			newMetricsChan = listener.newMetricsChan
		}
		select {
		case <-exit:
			return
		case <-scanFrequency:
		case <-force:
		case <-knownMetricsStatTicker:
			// It's only useful when using realtime index as the
			// scanFrequency should be a long interval/duration
			// like 2 hours or more, and with concurrent and
			// realtime index, indexed metrics would grow even without disk scanning.
			listener.statKnownMetrics(knownMetricsStatTicker)

			continue uloop
		case <-listener.quotaReload:
			listener.refreshQuotaRules()

			continue uloop
		case <-quotaAndUsageStatTicker:
			listener.refreshQuotaAndUsage(quotaAndUsageStatTicker)

			continue uloop
		case m := <-newMetricsChan:
			// listener.newMetricsChan might have high traffic, but
			// in theory, there should be no starvation on other channels:
			// https://groups.google.com/g/golang-nuts/c/4BR2Sdb6Zzk (2015)

			listener.insertRealtimeMetric(fidx.trieIdx, m)

			continue uloop
		}

		if listener.cacheGetRecentMetrics != nil {
			// cacheMetricNames maintains all new metric names added in cache
			// when cache-scan is enabled in conf
			newCacheMetricNames := listener.cacheGetRecentMetrics()
			cacheMetricNames = splitAndInsert(cacheMetricNames, newCacheMetricNames)
		}

		if listener.updateFileList(dir, cacheMetricNames, quotaAndUsageStatTicker) {
			listener.logger.Info("file list updated with cache, starting a new scan immediately")
			listener.updateFileList(dir, cacheMetricNames, quotaAndUsageStatTicker)
		}
	}
}

// Consume only the current backlog so a busy producer cannot starve the scan.
func (listener *CarbonserverListener) drainRealtimeMetrics(trie *trieIndex) {
	for remaining := len(listener.newMetricsChan); remaining > 0; remaining-- {
		listener.insertRealtimeMetric(trie, <-listener.newMetricsChan)
	}
}

func (listener *CarbonserverListener) insertRealtimeMetric(trie *trieIndex, metric string) {
	path := "/" + filepath.Clean(strings.ReplaceAll(metric, ".", "/")+".wsp")
	if _, err := trie.insert(path, 0, 0, 0, 0); err != nil {
		listener.logTrieInsertError(listener.logger, "failed to insert realtime metric", metric, err)
	}
}

func (listener *CarbonserverListener) startFileListUpdater(dir string, scanFrequency <-chan time.Time, force <-chan struct{}, exit <-chan struct{}) {
	listener.indexWorkers.Add(1)
	go func() {
		defer listener.indexWorkers.Done()
		listener.fileListUpdater(dir, scanFrequency, force, exit)
	}()
}

func (listener *CarbonserverListener) statKnownMetrics(knownMetricsStatTicker <-chan time.Time) {
	defer func() {
		// drain remaining blocked tickers
		for {
			select {
			case <-knownMetricsStatTicker:
			default:
				return
			}
		}
	}()

	fidx := listener.CurrentFileIndex()
	if fidx == nil || fidx.trieIdx == nil {
		return
	}

	start := time.Now()
	count, files, dirs, _, _, _, _, _ := fidx.trieIdx.countNodes()
	atomic.StoreUint64(&listener.metrics.TrieNodes, uint64(count))
	atomic.StoreUint64(&listener.metrics.TrieFiles, uint64(files))
	atomic.StoreUint64(&listener.metrics.TrieDirs, uint64(dirs))
	atomic.StoreUint64(&listener.metrics.ThrottledCreates, fidx.trieIdx.throttledCreates) // throttled per minute
	atomic.AddUint64(&fidx.trieIdx.throttledCreates, -fidx.trieIdx.throttledCreates)
	atomic.StoreUint64(&listener.metrics.MaxCreatesPerSecond, uint64(listener.maxCreatesPerSecond))
	// set using the indexed files, instead of returning on-disk files.
	//
	// WHY: with concurrent and realtime index, disk scan should be set at
	// am interval like 2 hours or longer. counting the files in trie index
	// gives us more timely visibilitty on how many metrics are known now.
	atomic.StoreUint64(&listener.metrics.MetricsKnown, uint64(files))

	atomic.StoreUint64(&listener.metrics.TrieCountNodesTimeNs, uint64(time.Since(start)))

	listener.logger.Debug(
		"trieIndex.countNodes",
		zap.Duration("trie_count_nodes_time", time.Since(start)),
	)
}

func (listener *CarbonserverListener) refreshQuotaAndUsage(quotaAndUsageStatTicker <-chan time.Time) {
	listener.refreshIndexQuotaAndUsage(listener.CurrentFileIndex(), quotaAndUsageStatTicker)
}

func (listener *CarbonserverListener) refreshIndexQuotaAndUsage(fidx *fileIndex, quotaAndUsageStatTicker <-chan time.Time) {
	defer func() {
		// Loading the initial index may take longer than the quota interval.
		// This refresh also satisfies ticks queued while initialization ran.
		for {
			select {
			case <-quotaAndUsageStatTicker:
			default:
				return
			}
		}
	}()

	sharedStore := listener.getMetricStore() != nil
	if sharedStore {
		listener.metricStoreIndexMu.Lock()
		defer listener.metricStoreIndexMu.Unlock()
		fidx = listener.CurrentFileIndex()
	}

	if !listener.isQuotaEnabled() || (!sharedStore && (!listener.concurrentIndex || listener.realtimeIndex <= 0)) || fidx == nil || fidx.trieIdx == nil {
		return
	}

	quotaStart := time.Now()
	throughputs, err := fidx.trieIdx.applyQuotas(listener.quotaUsageReportFrequency, listener.getQuotas()...)
	if err != nil {
		listener.logger.Error(
			"refreshQuotaAndUsage",
			zap.Error(err),
		)
	}

	quotaTime := uint64(time.Since(quotaStart))
	atomic.StoreUint64(&listener.metrics.QuotaApplyTimeNs, quotaTime)
	atomic.StoreUint64(&listener.metrics.ThrottledCreates, fidx.trieIdx.throttledCreates) // throttled per minute
	atomic.AddUint64(&fidx.trieIdx.throttledCreates, -fidx.trieIdx.throttledCreates)
	atomic.StoreUint64(&listener.metrics.MaxCreatesPerSecond, uint64(listener.maxCreatesPerSecond))

	usageStart := time.Now()
	files := fidx.trieIdx.refreshUsage(throughputs)
	usageTime := uint64(time.Since(usageStart))
	atomic.StoreUint64(&listener.metrics.UsageRefreshTimeNs, usageTime)

	// set using the indexed files, instead of returning on-disk files.
	//
	// WHY: quota subsystem atm can only be enabled along with concurrent
	// and realtime index, and with concurrent and realtime index, disk
	// scan should be set at an interval like 2 hours or longer. counting
	// the files in trie index gives us more timely visibility into how
	// many metrics are known now.
	atomic.StoreUint64(&listener.metrics.MetricsKnown, files)

	// WHY select: avoid potential block
	select {
	case listener.quotaAndUsageMetrics <- fidx.trieIdx.qauMetrics:
	default:
	}
	fidx.trieIdx.qauMetrics = nil

	listener.logger.Debug(
		"refreshQuotaAndUsage",
		zap.Uint64("quota_apply_time", quotaTime),
		zap.Uint64("usage_refresh_time", usageTime),
	)
}

func metricFileSizes(path string, info os.FileInfo) (logical, physical int64, err error) {
	add := func(info os.FileInfo) {
		logical += info.Size()
		size := info.Size()
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			size = stat.Blocks * 512
		}
		physical += size
	}

	add(info)
	sidecar, err := os.Stat(whisper.OutOfOrderSidecarPath(path))
	if errors.Is(err, os.ErrNotExist) {
		return logical, physical, nil
	}
	if err != nil {
		return logical, physical, err
	}
	add(sidecar)

	return logical, physical, nil
}

type fileListUpdate struct {
	listener            *CarbonserverListener
	logger              *zap.Logger
	started             time.Time
	fileIndex           *fileIndex
	files               []string
	filesLen            int
	details             map[string]*protov3.MetricDetails
	trieIdx             *trieIndex
	metricsKnown        uint64
	oooFiles            uint64
	oooPhysicalBytes    uint64
	lockFiles           uint64
	infos               []zap.Field
	cacheMetricNames    map[string]struct{}
	cacheMetricLen      int
	cacheIndexRuntime   time.Duration
	readFromCache       bool
	fileListCacheReader FileListCache
	fileListCacheEntry  FLCEntry
	fileListCache       FileListCache
	snapshotWriter      *indexSnapshotWriter
	snapshotReady       bool
	scanCancelled       bool
	scanFailed          bool
}

func newFileListUpdate(listener *CarbonserverListener, cacheMetricNames map[string]struct{}) *fileListUpdate {
	u := &fileListUpdate{
		listener: listener, logger: listener.logger.With(zap.String("handler", "fileListUpdated")), started: time.Now(),
		fileIndex: listener.CurrentFileIndex(), details: make(map[string]*protov3.MetricDetails), cacheMetricNames: cacheMetricNames, cacheMetricLen: len(cacheMetricNames),
	}
	if listener.trieIndex {
		if u.fileIndex == nil || !listener.concurrentIndex {
			u.trieIdx = newTrie(".wsp", listener.maxCreatesPerSecond, listener.estimateSize)
			u.trieIdx.builder = &trieBulkBuilder{}
		} else {
			u.trieIdx = u.fileIndex.trieIdx
			u.trieIdx.root.gen++
		}
	}
	u.populateCacheMetrics(cacheMetricNames)
	return u
}

func (u *fileListUpdate) populateCacheMetrics(cacheMetricNames map[string]struct{}) {
	started := time.Now()
	for fileName := range cacheMetricNames {
		if u.listener.trieIndex {
			if _, err := u.trieIdx.insert(fileName, 0, 0, 0, 0); err != nil {
				u.listener.logTrieInsertError(u.logger, "error populating index from cache indexMap", fileName, err)
			}
		} else {
			u.files = append(u.files, fileName)
		}
		if strings.HasSuffix(fileName, ".wsp") {
			u.metricsKnown++
		}
	}
	u.cacheIndexRuntime = time.Since(started)
}

func (listener *CarbonserverListener) updateFileList(dir string, cacheMetricNames map[string]struct{}, quotaAndUsageStatTicker <-chan time.Time) bool {
	return listener.updateFileListWithCache(dir, cacheMetricNames, quotaAndUsageStatTicker, false)
}

func (listener *CarbonserverListener) updateFileListWithCache(dir string, cacheMetricNames map[string]struct{}, quotaAndUsageStatTicker <-chan time.Time, cacheOnly bool) (readFromCache bool) {
	if metricStore := listener.getMetricStore(); metricStore != nil {
		if err := listener.updateMetricStoreIndex(metricStore); err != nil {
			listener.logger.Error("failed to update shared metric-store index", zap.Error(err))
		}
		return false
	}
	logger := listener.logger.With(zap.String("handler", "fileListUpdated"))
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic encountered",
				zap.Stack("stack"),
				zap.Any("error", r),
			)
		}
	}()
	u := newFileListUpdate(listener, cacheMetricNames)
	defer func() { readFromCache = u.readFromCache }()
	defer u.closeFileListCaches()
	if !u.loadFileListCache(cacheOnly) {
		return false
	}
	if !u.readFromCache && !u.scanFiles(dir, quotaAndUsageStatTicker) {
		return false
	}
	u.pruneRealtimeMetrics()
	u.closeFileListCaches()
	if u.snapshotReady && u.trieIdx != nil && u.trieIdx.snapshot != nil {
		u.replaceSnapshot()
	}
	return u.publish(dir, quotaAndUsageStatTicker)
}

func (u *fileListUpdate) loadFileListCache(cacheOnly bool) bool {
	if !u.listener.trieIndex || u.fileIndex != nil || u.listener.fileListCache == "" {
		return !cacheOnly
	}
	if u.listener.concurrentIndex {
		started := time.Now()
		if snapshot, err := openIndexSnapshot(u.listener.fileListCache, u.listener.whisperData); err == nil {
			u.trieIdx = newTrie(".wsp", u.listener.maxCreatesPerSecond, u.listener.estimateSize)
			u.trieIdx.snapshot = snapshot
			u.metricsKnown = 0
			u.populateCacheMetrics(u.cacheMetricNames)
			u.metricsKnown = snapshot.manifest.Records + uint64(u.trieIdx.fileCount)
			u.readFromCache = true
			u.infos = append(u.infos, zap.Duration("snapshot_load_time", time.Since(started)))
			return true
		} else if !os.IsNotExist(err) {
			u.logger.Warn("index snapshot unavailable; loading legacy cache", zap.Error(err))
		}
	}
	flc, err := NewFileListCache(u.listener.fileListCache, FLCVersionUnspecified, 'r')
	if err != nil {
		if !os.IsNotExist(err) {
			u.logger.Error("failed to read file list cache", zap.Error(err))
		}
		return !cacheOnly
	}
	u.fileListCacheReader = flc
	u.infos = append(u.infos, zap.Int("file_list_cache_version", int(flc.GetVersion())))
	u.readFromCache = true
	for u.readNextCacheEntry(flc) {
	}
	if u.stopped() {
		return false
	}
	return u.readFromCache || !cacheOnly
}

func (u *fileListUpdate) readNextCacheEntry(flc FileListCache) bool {
	select {
	case <-u.listener.exitChan:
		u.readFromCache = false
		return false
	default:
	}
	entry := &u.fileListCacheEntry
	var err error
	if reader, ok := flc.(interface{ readInto(*FLCEntry) error }); ok {
		err = reader.readInto(entry)
	} else {
		var next *FLCEntry
		next, err = flc.Read()
		if err == nil {
			*entry = *next
		}
	}
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		u.infos = append(u.infos, zap.NamedError("file_list_cache_read_error", err))
		u.resetTrie()
		return false
	}
	if entry.Path == "" {
		return true
	}
	if _, err := u.trieIdx.insert(entry.Path, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt); err != nil {
		u.listener.logTrieInsertError(u.logger, "failed to read from file list cache", entry.Path, err)
		u.resetTrie()
		return false
	}
	u.filesLen++
	if strings.HasSuffix(entry.Path, ".wsp") {
		u.metricsKnown++
	}
	return true
}

func (u *fileListUpdate) resetTrie() {
	u.readFromCache = false
	u.trieIdx = newTrie(".wsp", u.listener.maxCreatesPerSecond, u.listener.estimateSize)
	u.trieIdx.builder = &trieBulkBuilder{}
}

func (u *fileListUpdate) scanFiles(dir string, quotaAndUsageStatTicker <-chan time.Time) bool {
	u.fileListCache = u.newFileListCacheWriter()
	u.logWhisperDataDir(dir)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		return u.walkFile(path, info, err, quotaAndUsageStatTicker)
	})
	if u.scanCancelled {
		return false
	}
	if err != nil {
		u.scanFailed = true
		u.logger.Error("error getting file list", zap.Error(err))
	}
	return true
}

func (u *fileListUpdate) newFileListCacheWriter() FileListCache {
	if u.listener.fileListCache == "" {
		return nil
	}
	flc, err := NewFileListCache(u.listener.fileListCache, u.listener.fileListCacheVersion, 'w')
	if err != nil {
		if !os.IsNotExist(err) {
			u.logger.Error("failed to create file list cache", zap.Error(err))
		}
		return nil
	}
	u.infos = append(u.infos, zap.Int("file_list_cache_version", int(flc.GetVersion())))
	// Build restart snapshots only while a complete read index is already live.
	// A first scan without a usable cache must not wait for snapshot construction.
	if u.fileIndex != nil && u.listener.trieIndex && u.listener.concurrentIndex && flc.GetVersion() == FLCVersion2 {
		if writer, err := newIndexSnapshotWriter(u.listener.fileListCache, u.listener.whisperData); err != nil {
			u.logger.Warn("failed to prepare index snapshot", zap.Error(err))
		} else {
			u.snapshotWriter = writer
		}
	}
	return flc
}

func (u *fileListUpdate) closeFileListCaches() {
	defer func() { u.fileListCache, u.fileListCacheReader, u.snapshotWriter = nil, nil, nil }()
	complete := !u.scanCancelled && !u.scanFailed
	if u.fileListCache == nil {
		complete = false
	} else if !complete {
		if err := u.fileListCache.Abort(); err != nil {
			u.logger.Error("failed to abort file list cache", zap.Error(err))
		}
	} else if err := u.fileListCache.Close(); err != nil {
		complete = false
		u.logger.Error("failed to close flie list cache", zap.Error(err))
	}
	if u.snapshotWriter != nil {
		if complete {
			if err := u.snapshotWriter.finish(); err != nil {
				u.logger.Warn("failed to publish index snapshot", zap.Error(err))
			} else {
				u.snapshotReady = true
				u.logger.Info("index snapshot written", zap.Uint64("records", u.snapshotWriter.manifest.Records))
			}
		} else {
			_ = u.snapshotWriter.abort()
		}
	}
	if u.fileListCacheReader != nil {
		if err := u.fileListCacheReader.Close(); err != nil {
			u.logger.Error("failed to close file list cache", zap.Error(err))
		}
	}
}

func (u *fileListUpdate) logWhisperDataDir(dir string) {
	if fi, err := os.Lstat(dir); err != nil {
		u.logger.Error("failed to stat whisper data directory", zap.String("path", dir), zap.Error(err))
	} else if fi.Mode()&os.ModeSymlink == 1 {
		u.logger.Error("can't index symlink data dir", zap.String("path", dir))
	}
}

func (u *fileListUpdate) walkFile(path string, info os.FileInfo, walkErr error, quotaAndUsageStatTicker <-chan time.Time) error {
	if u.cancelled() {
		return filepath.SkipAll
	}
	if walkErr != nil {
		u.scanFailed = true
		u.logger.Info("error processing", zap.String("path", path), zap.Error(walkErr))
		return nil
	}
	u.refreshQuotaAndRealtimeMetrics(quotaAndUsageStatTicker)
	if info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ".lock") {
		u.lockFiles++
	}
	if info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ".ooo") {
		u.oooFiles++
		size := info.Size()
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			size = stat.Blocks * 512
		}
		u.oooPhysicalBytes += uint64(size)
	}
	if info.IsDir() || strings.HasSuffix(info.Name(), ".wsp") {
		u.addFile(path, info)
	}
	return nil
}

func (u *fileListUpdate) cancelled() bool {
	if u.stopped() {
		u.scanCancelled = true
		return true
	}
	return false
}

func (u *fileListUpdate) stopped() bool {
	select {
	case <-u.listener.exitChan:
		return true
	default:
		return false
	}
}

func (u *fileListUpdate) refreshQuotaAndRealtimeMetrics(quotaAndUsageStatTicker <-chan time.Time) {
	select {
	case <-u.listener.quotaReload:
		u.listener.refreshQuotaRules()
	default:
	}
	if u.listener.isQuotaEnabled() {
		select {
		case <-quotaAndUsageStatTicker:
			u.listener.refreshQuotaAndUsage(quotaAndUsageStatTicker)
		default:
		}
	}
	if u.listener.trieIndex && u.listener.concurrentIndex {
		u.listener.drainRealtimeMetrics(u.trieIdx)
	}
}

func (u *fileListUpdate) addFile(path string, info os.FileInfo) {
	trimmedName := strings.TrimPrefix(path, u.listener.whisperData)
	isFullMetric := strings.HasSuffix(info.Name(), ".wsp")
	u.filesLen++
	dataPoints, logicalSize, physicalSize := u.metricSizes(path, info, trimmedName, isFullMetric)
	firstSeenAt := u.indexFile(trimmedName, isFullMetric, logicalSize, physicalSize, dataPoints)
	u.cacheFile(trimmedName, isFullMetric, dataPoints, logicalSize, physicalSize, firstSeenAt)
	u.addMetricDetails(info, trimmedName, isFullMetric, logicalSize, physicalSize)
}

func (u *fileListUpdate) metricSizes(path string, info os.FileInfo, trimmedName string, isFullMetric bool) (dataPoints, logicalSize, physicalSize int64) {
	if !isFullMetric {
		return 0, 0, 0
	}
	if u.listener.estimateSize != nil {
		metric := strings.ReplaceAll(trimmedName, "/", ".")
		_, _, dataPoints = u.listener.estimateSize(metric[1 : len(metric)-4])
	}
	logicalSize, physicalSize, err := metricFileSizes(path, info)
	if err != nil {
		u.logger.Info("failed to stat out-of-order sidecar", zap.String("path", whisper.OutOfOrderSidecarPath(path)), zap.Error(err))
	}
	return dataPoints, logicalSize, physicalSize
}

func (u *fileListUpdate) indexFile(name string, isFullMetric bool, logicalSize, physicalSize, dataPoints int64) int64 {
	if _, present := u.cacheMetricNames[name]; present {
		delete(u.cacheMetricNames, name)
		return 0
	}
	var firstSeenAt int64
	if !u.listener.trieIndex {
		u.files = append(u.files, name)
	} else if isFullMetric {
		// Do not materialize every mapped file as a heap node during a scan.
		if u.trieIdx.snapshot != nil {
			if entry, found, err := u.trieIdx.snapshot.lookup(name); err == nil && found {
				u.metricsKnown++
				if entry.FirstSeenAt == 0 {
					return u.trieIdx.snapshot.openedAt
				}
				return entry.FirstSeenAt
			}
		}
		node, err := u.trieIdx.insert(name, logicalSize, physicalSize, dataPoints, 0)
		if err != nil {
			u.listener.logTrieInsertError(u.logger, "updateFileList.trie: failed to index path", name, err)
		} else if node.meta != nil {
			firstSeenAt = node.meta.(*fileMeta).firstSeenAt
		}
	}
	if isFullMetric {
		u.metricsKnown++
	}
	return firstSeenAt
}

func (u *fileListUpdate) cacheFile(name string, isFullMetric bool, dataPoints, logicalSize, physicalSize, firstSeenAt int64) {
	if u.fileListCache == nil || (u.listener.trieIndex && !isFullMetric) {
		return
	}
	entry := FLCEntry{Path: name, DataPoints: dataPoints, LogicalSize: logicalSize, PhysicalSize: physicalSize, FirstSeenAt: firstSeenAt}
	if err := u.fileListCache.Write(&entry); err != nil {
		u.logger.Error("failed to write to file list cache", zap.Error(err))
		if err := u.fileListCache.Close(); err != nil {
			u.logger.Error("failed to close flie list cache", zap.Error(err))
		}
		u.fileListCache = nil
	}
	if u.snapshotWriter != nil {
		if err := u.snapshotWriter.append(&entry); err != nil {
			u.logger.Warn("failed to build index snapshot", zap.Error(err))
			_ = u.snapshotWriter.abort()
			u.snapshotWriter = nil
		}
	}
}

func (u *fileListUpdate) addMetricDetails(info os.FileInfo, name string, isFullMetric bool, logicalSize, physicalSize int64) {
	if !isFullMetric || u.listener.internalStatsDir == "" {
		return
	}
	details := stat.GetStat(info)
	details.Size = logicalSize
	details.RealSize = physicalSize
	metric := strings.ReplaceAll(name[1:len(name)-4], "/", ".")
	u.details[metric] = &protov3.MetricDetails{Size: details.Size, ModTime: details.MTime, ATime: details.ATime, RealSize: details.RealSize}
}

func (u *fileListUpdate) pruneRealtimeMetrics() {
	if u.listener.concurrentIndex && u.trieIdx != nil {
		// Include notifications queued while loading the file-list cache.
		u.listener.drainRealtimeMetrics(u.trieIdx)
		// Fresh tries have no old generations or deleted branches. Insertion
		// already maintains the compressed radix shape, so pruning adds a full
		// traversal without changing this private tree.
		if u.trieIdx.builder == nil && !u.scanFailed {
			if u.trieIdx.snapshot != nil && u.listener.cacheGet != nil {
				// A queued metric may still be awaiting its first disk write.
				// Preserve those overlay entries even if this scan saw no file.
				names, nodes, _, _, _ := u.trieIdx.allMetricsNodeMutable(u.trieIdx.root, '.', "", int(^uint(0)>>1), false)
				for i, name := range names {
					if nodes[i].gen != u.trieIdx.root.gen && len(u.listener.cacheGet(name)) > 0 {
						u.listener.insertRealtimeMetric(u.trieIdx, name)
					}
				}
			}
			u.trieIdx.prune()
		}
	}
}

func (u *fileListUpdate) publish(dir string, quotaAndUsageStatTicker <-chan time.Time) bool {
	freeSpace, totalSpace, ok := u.fileSystemSpace(dir)
	if !ok {
		return u.readFromCache
	}
	fileScanRuntime := time.Since(u.started)
	if u.trieIdx != nil && u.trieIdx.snapshot != nil {
		u.metricsKnown = u.trieIdx.snapshot.manifest.Records + uint64(u.trieIdx.fileCount)
	}
	atomic.StoreUint64(&u.listener.metrics.MetricsKnown, u.metricsKnown)
	atomic.AddUint64(&u.listener.metrics.FileScanTimeNS, uint64(fileScanRuntime.Nanoseconds()))
	index, indexType, indexSize, pruned, indexingRuntime := u.buildIndex(freeSpace, totalSpace)
	rdTimeUpdateRuntime := u.copyAccessTimes(index)
	if u.fileIndex == nil || (index.trieIdx != nil && index.trieIdx != u.fileIndex.trieIdx) {
		// The first published index must already enforce its configured quotas.
		quotaStarted := time.Now()
		u.listener.refreshIndexQuotaAndUsage(index, quotaAndUsageStatTicker)
		u.infos = append(u.infos, zap.Duration("initial_quota_usage_time", time.Since(quotaStarted)))
	}
	if u.stopped() {
		return false
	}
	if index.trieIdx != nil {
		index.trieIdx.builder = nil
	}
	u.listener.UpdateFileIndex(index)
	// File-list caches omit sidecars, and incomplete scans can undercount them.
	if !u.readFromCache && !u.scanFailed {
		atomic.StoreUint64(&u.listener.metrics.OOOFiles, u.oooFiles)
		atomic.StoreUint64(&u.listener.metrics.OOOPhysicalBytes, u.oooPhysicalBytes)
		atomic.StoreUint64(&u.listener.metrics.LockFiles, u.lockFiles)
	}
	u.logResult(fileScanRuntime, indexingRuntime, rdTimeUpdateRuntime, indexType, indexSize, pruned)
	return u.readFromCache
}

func (u *fileListUpdate) fileSystemSpace(dir string) (uint64, uint64, bool) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		u.logger.Info("error getting FS Stats", zap.String("dir", dir), zap.Error(err))
		return 0, 0, false
	}
	var freeSpace uint64
	// diskspace can be negative and Bavail is therefore int64
	if stat.Bavail >= 0 { // nolint:staticcheck // skipcq: SCC-SA4003
		freeSpace = uint64(stat.Bavail) * uint64(stat.Bsize)
	}
	return freeSpace, stat.Blocks * uint64(stat.Bsize), true
}

func (u *fileListUpdate) buildIndex(freeSpace, totalSpace uint64) (*fileIndex, string, int, int, time.Duration) {
	index := &fileIndex{details: u.details, freeSpace: freeSpace, totalSpace: totalSpace, accessTimes: make(map[string]int64)}
	indexType, indexSize, pruned := "trigram", 0, 0
	started := time.Now()
	if u.listener.trieIndex {
		indexType, index.trieIdx = "trie", u.trieIdx
		if u.trieIdx.snapshot != nil {
			indexType = "snapshot+trie"
		}
		indexSize = u.addTrieStats(index)
	} else {
		index.files = u.files
		index.idx = trigram.NewIndex(u.files)
		pruned = index.idx.Prune(0.95)
		indexSize = len(index.idx)
	}
	runtime := time.Since(started)
	atomic.AddUint64(&u.listener.metrics.IndexBuildTimeNS, uint64(runtime.Nanoseconds()))
	return index, indexType, indexSize, pruned, runtime
}

func (u *fileListUpdate) addTrieStats(index *fileIndex) int {
	u.infos = append(u.infos, zap.Int("trie_depth", int(index.trieIdx.depth)), zap.String("longest_metric", index.trieIdx.longestMetric))
	started := time.Now()
	var count, files, dirs int
	if b := index.trieIdx.builder; b != nil {
		count, files, dirs = b.nodes, index.trieIdx.fileCount, b.dirs
	} else {
		count, files, dirs, _, _, _, _, _ = index.trieIdx.countNodes()
	}
	atomic.StoreUint64(&u.listener.metrics.TrieNodes, uint64(count))
	atomic.StoreUint64(&u.listener.metrics.TrieFiles, uint64(files))
	atomic.StoreUint64(&u.listener.metrics.TrieDirs, uint64(dirs))
	u.infos = append(u.infos, zap.Duration("trie_count_nodes_time", time.Since(started)))
	return count
}

func (u *fileListUpdate) copyAccessTimes(index *fileIndex) time.Duration {
	started := time.Now()
	if u.fileIndex == nil || u.listener.internalStatsDir == "" {
		return time.Since(started)
	}
	u.listener.fileIdxMutex.Lock()
	defer u.listener.fileIdxMutex.Unlock()
	for metric := range u.fileIndex.accessTimes {
		if details, ok := u.details[metric]; ok {
			details.RdTime = u.fileIndex.accessTimes[metric]
		} else {
			delete(u.fileIndex.accessTimes, metric)
			if u.listener.db != nil {
				u.listener.db.Delete([]byte(metric), nil)
			}
		}
	}
	index.accessTimes = u.fileIndex.accessTimes
	return time.Since(started)
}

func (u *fileListUpdate) logResult(fileScanRuntime, indexingRuntime, rdTimeUpdateRuntime time.Duration, indexType string, indexSize, pruned int) {
	u.infos = append(u.infos,
		zap.Duration("file_scan_runtime", fileScanRuntime), zap.Duration("indexing_runtime", indexingRuntime), zap.Duration("rdtime_update_runtime", rdTimeUpdateRuntime),
		zap.Duration("cache_index_runtime", u.cacheIndexRuntime), zap.Duration("total_runtime", time.Since(u.started)), zap.Int("Files", u.filesLen),
		zap.Int("index_size", indexSize), zap.Int("pruned_trigrams", pruned), zap.Int("cache_metric_len_before", u.cacheMetricLen),
		zap.Int("cache_metric_len_after", len(u.cacheMetricNames)), zap.Uint64("metrics_known", u.metricsKnown), zap.String("index_type", indexType), zap.Bool("read_from_cache", u.readFromCache),
	)
	u.logger.Info("file list updated", u.infos...)
}

func (*CarbonserverListener) logTrieInsertError(logger *zap.Logger, msg, metric string, err error) {
	zfields := []zap.Field{zap.Error(err), zap.String("metric", metric)}
	var ierr *trieInsertError
	if errors.As(err, &ierr) {
		zfields = append(zfields, zap.String("err_info", ierr.info))
	}
	logger.Error(msg, zfields...)
}

func (listener *CarbonserverListener) expandGlobs(ctx context.Context, query string, resultCh chan<- *ExpandedGlobResponse) {
	defer func() {
		if err := recover(); err != nil {
			resultCh <- &ExpandedGlobResponse{query, nil, nil, nil, 0, fmt.Errorf("%s\n%s", err, debug.Stack())}
		}
	}()

	release, err := listener.acquireGlobQuerySlot(ctx, query)
	if err != nil {
		resultCh <- &ExpandedGlobResponse{query, nil, nil, nil, 0, err}
		return
	}
	if release != nil {
		defer release()
	}

	logger := TraceContextToZap(ctx, listener.logger)
	matchedCount := 0
	started := time.Now()
	defer listener.logSlowGlobExpansion(logger, query, started, &matchedCount)

	if listener.trieIndex && listener.CurrentFileIndex() != nil {
		files, leafs, nodes, lookups, err := listener.expandGlobsTrie(query)
		resultCh <- &ExpandedGlobResponse{query, files, leafs, nodes, lookups, err}
		return
	}

	useGlob := listener.shouldUseFilesystemGlob(query)
	logger = logger.With(zap.Bool("use_glob", useGlob))

	/* things to glob:
	 * - carbon.relays  -> carbon.relays
	 * - carbon.re      -> carbon.relays, carbon.rewhatever
	 * - carbon.[rz]    -> carbon.relays, carbon.zipper
	 * - carbon.{re,zi} -> carbon.relays, carbon.zipper
	 * - match is either dir or .wsp file
	 * unfortunately, filepath.Glob doesn't handle the curly brace
	 * expansion for us */

	globs, err := listener.globPatterns(query, logger)
	if err != nil {
		resultCh <- &ExpandedGlobResponse{query, nil, nil, nil, 0, err}
		return
	}
	files := listener.expandGlobFiles(globs, useGlob)
	files, leafs := listener.normalizeGlobFiles(files)

	matchedCount = len(files)
	resultCh <- &ExpandedGlobResponse{query, files, leafs, nil, 0, nil}
}

func (listener *CarbonserverListener) globPatterns(query string, logger *zap.Logger) ([]string, error) {
	query = strings.ReplaceAll(query, ".", "/")
	globs := []string{query}
	if !strings.HasSuffix(query, "*") {
		globs = append([]string{query + ".wsp"}, globs...)
		logger.Debug("appending file to globs struct", zap.Strings("globs", globs))
	}
	return listener.expandGlobBraces(globs)
}

func (listener *CarbonserverListener) expandGlobFiles(globs []string, useGlob bool) []string {
	fidx := listener.CurrentFileIndex()
	fallbackToFS := !listener.trigramIndex || fidx == nil || len(fidx.files) == 0
	files := listener.matchGlobIndex(fidx, globs, useGlob)
	if useGlob || fallbackToFS {
		files = append(files, listener.matchGlobFilesystem(globs)...)
	}
	return files
}

func (listener *CarbonserverListener) matchGlobIndex(fidx *fileIndex, globs []string, useGlob bool) []string {
	if fidx == nil || useGlob {
		return nil
	}
	docs := make(map[trigram.DocID]struct{})
	for _, glob := range globs {
		matchGlobDocuments(fidx, glob, docs)
	}
	files := make([]string, 0, len(docs))
	for id := range docs {
		files = append(files, listener.whisperData+fidx.files[id])
	}
	sort.Strings(files)
	return files
}

func matchGlobDocuments(fidx *fileIndex, glob string, docs map[trigram.DocID]struct{}) {
	for _, id := range fidx.idx.QueryTrigrams(extractTrigrams(glob)) {
		docID := trigram.DocID(id)
		if _, seen := docs[docID]; seen {
			continue
		}
		matched, err := filepath.Match("/"+glob, fidx.files[id])
		if err == nil && matched {
			docs[docID] = struct{}{}
		}
	}
}

func (listener *CarbonserverListener) matchGlobFilesystem(globs []string) []string {
	var files []string
	for _, glob := range globs {
		if matched, err := filepath.Glob(listener.whisperData + "/" + glob); err == nil {
			files = append(files, matched...)
		}
	}
	return files
}

func (listener *CarbonserverListener) normalizeGlobFiles(files []string) ([]string, []bool) {
	leafs := make([]bool, len(files))
	for i, path := range files {
		info, err := os.Stat(path)
		if err != nil && !os.IsNotExist(err) {
			continue
		}
		path, leafs[i] = listener.normalizeGlobFile(path, info)
		files[i] = path
	}
	return files, leafs
}

func (listener *CarbonserverListener) normalizeGlobFile(path string, info os.FileInfo) (string, bool) {
	path = path[len(listener.whisperData+"/"):]
	leaf := strings.HasSuffix(path, ".wsp") && (info == nil || !info.IsDir())
	if leaf {
		path = path[:len(path)-4]
	}
	return strings.ReplaceAll(path, "/", "."), leaf
}

func (listener *CarbonserverListener) acquireGlobQuerySlot(ctx context.Context, query string) (func(), error) {
	for _, rl := range listener.globQueryRateLimiters {
		if !rl.pattern.MatchString(query) {
			continue
		}
		if cap(rl.maxInflightRequests) == 0 {
			return nil, fmt.Errorf("rejected by query rate limiter: %s", rl.pattern.String())
		}
		rl.maxInflightRequests <- struct{}{}
		if listener.checkRequestCtx(ctx) != nil {
			<-rl.maxInflightRequests
			return nil, fmt.Errorf("time out due to heavy glob query rate limiter: %s", rl.pattern.String())
		}
		return func() { <-rl.maxInflightRequests }, nil
	}
	return nil, nil
}

func (listener *CarbonserverListener) logSlowGlobExpansion(logger *zap.Logger, query string, started time.Time, matchedCount *int) {
	duration := time.Since(started)
	if duration <= time.Second {
		return
	}
	indexType := ""
	if listener.trieIndex {
		indexType = "trie"
		if listener.trigramIndex {
			indexType = "trie-trigram"
		}
	} else if listener.trigramIndex {
		indexType = "trigram"
	}
	logger.Info("slow_expand_globs", zap.Duration("time", duration), zap.String("query", query), zap.Int("matched_count", *matchedCount), zap.String("index_type", indexType))
}

func (listener *CarbonserverListener) shouldUseFilesystemGlob(query string) bool {
	star := strings.IndexByte(query, '*')
	return listener.getMetricStore() == nil && listener.cacheGetRecentMetrics == nil && strings.IndexByte(query, '[') == -1 && strings.IndexByte(query, '?') == -1 && (star == -1 || star == len(query)-1)
}

// TODO(dgryski): add tests
func (listener *CarbonserverListener) expandGlobBraces(globs []string) ([]string, error) {
	for {
		bracematch := false
		var newglobs []string
		for _, glob := range globs {
			lbrace := strings.Index(glob, "{")
			rbrace := -1
			if lbrace > -1 {
				rbrace = strings.Index(glob[lbrace:], "}")
				if rbrace > -1 {
					rbrace += lbrace
				}
			}

			if lbrace > -1 && rbrace > -1 {
				bracematch = true
				expansion := glob[lbrace+1 : rbrace]
				parts := strings.Split(expansion, ",")
				for _, sub := range parts {
					if len(newglobs) > listener.maxGlobs {
						if listener.failOnMaxGlobs {
							return nil, errMaxGlobsExhausted
						}
						break
					}
					newglobs = append(newglobs, glob[:lbrace]+sub+glob[rbrace+1:])
				}
			} else {
				if len(newglobs) > listener.maxGlobs {
					if listener.failOnMaxGlobs {
						return nil, errMaxGlobsExhausted
					}
					break
				}
				newglobs = append(newglobs, glob)
			}
		}
		globs = newglobs
		if !bracematch {
			break
		}
	}
	return globs, nil
}

func (listener *CarbonserverListener) Stat(send helper.StatCallback) {
	senderRaw := helper.SendUint64
	sender := helper.SendAndSubstractUint64
	if listener.metricsAsCounters {
		sender = helper.SendUint64
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	pauseNS := m.PauseTotalNs
	alloc := m.Alloc
	totalAlloc := m.TotalAlloc
	numGC := uint64(m.NumGC)

	sender("render_requests", &listener.metrics.RenderRequests, send)
	sender("notfound", &listener.metrics.NotFound, send)
	sender("find_requests", &listener.metrics.FindRequests, send)
	sender("find_zero", &listener.metrics.FindZero, send)
	sender("list_requests", &listener.metrics.ListRequests, send)
	sender("details_requests", &listener.metrics.DetailsRequests, send)
	sender("cache_hit", &listener.metrics.CacheHit, send)
	sender("cache_miss", &listener.metrics.CacheMiss, send)
	sender("cache_work_time_ns", &listener.metrics.CacheWorkTimeNS, send)
	sender("cache_wait_time_fetch_ns", &listener.metrics.CacheWaitTimeFetchNS, send)
	sender("cache_requests", &listener.metrics.CacheRequestsTotal, send)
	sender("disk_wait_time_ns", &listener.metrics.DiskWaitTimeNS, send)
	sender("disk_requests", &listener.metrics.DiskRequests, send)
	sender("points_returned", &listener.metrics.PointsReturned, send)
	sender("metrics_returned", &listener.metrics.MetricsReturned, send)
	sender("find_metrics_found_without_response_cache", &listener.metrics.findMetricsFoundWithoutResponseCache, send)
	sender("throttled_creates", &listener.metrics.ThrottledCreates, send)
	sender("max_creates_per_second", &listener.metrics.MaxCreatesPerSecond, send)
	sender("fetch_size_bytes", &listener.metrics.FetchSize, send)

	senderRaw("metrics_known", &listener.metrics.MetricsKnown, send)
	senderRaw("oooFiles", &listener.metrics.OOOFiles, send)
	senderRaw("oooPhysicalBytes", &listener.metrics.OOOPhysicalBytes, send)
	senderRaw("lockFiles", &listener.metrics.LockFiles, send)
	sender("index_build_time_ns", &listener.metrics.IndexBuildTimeNS, send)
	sender("file_scan_time_ns", &listener.metrics.FileScanTimeNS, send)

	sender("query_cache_hit", &listener.metrics.QueryCacheHit, send)
	sender("query_cache_miss", &listener.metrics.QueryCacheMiss, send)

	sender("find_cache_hit", &listener.metrics.FindCacheHit, send)
	sender("find_cache_miss", &listener.metrics.FindCacheMiss, send)

	sender("find_expanded_globs_cache_hit", &listener.metrics.findExpandedGlobsCachedHit, send)
	sender("find_expanded_globs_cache_miss", &listener.metrics.findExpandedGlobsCacheMiss, send)

	sender("render_expanded_globs_cache_hit", &listener.metrics.renderExpandedGlobsCacheHit, send)
	sender("render_expanded_globs_cache_miss", &listener.metrics.renderExpandedGlobsCacheMiss, send)

	sender("inflight_requests_count", &listener.metrics.InflightRequests, send)
	senderRaw("inflight_requests_limit", &listener.MaxInflightRequests, send)
	sender("rejected_too_many_requests", &listener.metrics.RejectedTooManyRequests, send)

	if listener.concurrentIndex {
		senderRaw("trie_index_nodes", &listener.metrics.TrieNodes, send)
		senderRaw("trie_index_files", &listener.metrics.TrieFiles, send)
		senderRaw("trie_index_dirs", &listener.metrics.TrieDirs, send)
		senderRaw("trie_count_nodes_time_ns", &listener.metrics.TrieCountNodesTimeNs, send)
	}
	if listener.isQuotaEnabled() {
		senderRaw("quota_apply_time_ns", &listener.metrics.QuotaApplyTimeNs, send)
		senderRaw("usage_refresh_time_ns", &listener.metrics.UsageRefreshTimeNs, send)
	}

	sender("alloc", &alloc, send)
	sender("total_alloc", &totalAlloc, send)
	sender("num_gc", &numGC, send)
	sender("pause_ns", &pauseNS, send)

	for name, codes := range statusCodes {
		for i := range codes {
			sender(fmt.Sprintf("request_codes.%s.%vxx", name, i+1), &codes[i], send)
		}
	}
	bucketStart := 0
	bucketEnd := 10
	for i := 0; i <= listener.buckets; i++ {
		metricName := fmt.Sprintf("requests_in_%dms_to_%dms", bucketStart, bucketEnd)
		if i == listener.buckets {
			metricName = fmt.Sprintf("requests_in_%dms_to_inf", bucketStart)
		}
		sender(metricName, &listener.timeBuckets[i], send)
		if bucketStart == 0 {
			bucketStart = 1
		}
		bucketStart *= 10
		bucketEnd *= 10
	}

	// Computing response percentiles
	if len(listener.percentiles) > 0 {
		listener.requestsTimes.Lock()
		list := listener.requestsTimes.list
		listener.requestsTimes.list = make([]int64, 0, len(list))
		listener.requestsTimes.Unlock()
		if len(list) == 0 {
			for _, p := range listener.percentiles {
				send(fmt.Sprintf("request_time_%vth_percentile_ns", p), 0)
			}
		} else {
			sort.Slice(list, func(i, j int) bool { return list[i] < list[j] })

			for _, p := range listener.percentiles {
				key := int(float64(p)/100*float64(len(list))) - 1
				if key < 0 {
					key = 0
				}
				send(fmt.Sprintf("request_time_%vth_percentile_ns", p), float64(list[key]))
			}
		}
	}

	// WHY select: avoid potential block
	select {
	case qauMetrics := <-listener.quotaAndUsageMetrics:
		for _, ps := range qauMetrics {
			send(ps.Metric, float64(ps.Data[0].Value))
		}
	default:
	}
}

func (listener *CarbonserverListener) Stop() error {
	listener.stopOnce.Do(func() {
		if listener.scanTicker != nil {
			listener.scanTicker.Stop()
		}
		if listener.exitChan != nil {
			close(listener.exitChan)
		}
		listener.indexWorkers.Wait()
		if listener.httpServer != nil {
			shutdownContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := listener.httpServer.Shutdown(shutdownContext)
			cancel()
			if err != nil {
				listener.logger.Warn("failed to gracefully stop HTTP server", zap.Error(err))
				listener.httpServer.Close()
			}
		}
		if listener.grpcServer != nil {
			gracefulStopped := make(chan struct{})
			go func() {
				listener.grpcServer.GracefulStop()
				close(gracefulStopped)
			}()
			select {
			case <-gracefulStopped:
			case <-time.After(30 * time.Second):
				listener.logger.Warn("timed out stopping gRPC server; forcing shutdown")
				listener.grpcServer.Stop()
				<-gracefulStopped
			}
		}
		listener.serverWG.Wait()
		if listener.getMetricStore() != nil {
			listener.stopSharedStoreRequests(30 * time.Second)
		}
		if listener.db != nil {
			listener.db.Close()
		}
		if listener.tcpListener != nil {
			listener.tcpListener.Close()
		}
		if listener.grpcListener != nil {
			listener.grpcListener.Close()
		}
	})
	return nil
}

func removeDirectory(dir string) error {
	// A small safety check, it doesn't cover all the cases, but will help a little bit in case of misconfiguration
	switch strings.TrimSuffix(dir, "/") {
	case "/", "/etc", "/usr", "/bin", "/sbin", "/lib", "/lib64", "/usr/lib", "/usr/lib64", "/usr/bin", "/usr/sbin", "C:", "C:\\":
		return fmt.Errorf("Can't remove system directory: %s", dir)
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	files, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}

	for _, f := range files {
		err = os.RemoveAll(filepath.Join(dir, f))
		if err != nil {
			return err
		}
	}

	return nil
}

func (listener *CarbonserverListener) initStatsDB() error {
	var err error
	if listener.internalStatsDir != "" {
		o := &opt.Options{
			Filter: filter.NewBloomFilter(10),
		}

		listener.db, err = leveldb.OpenFile(listener.internalStatsDir, o)
		if err != nil {
			listener.logger.Error("Can't open statistics database",
				zap.Error(err),
			)

			err = removeDirectory(listener.internalStatsDir)
			if err != nil {
				listener.logger.Error("Can't remove old statistics database",
					zap.Error(err),
				)
				return err
			}

			listener.db, err = leveldb.OpenFile(listener.internalStatsDir, o)
			if err != nil {
				listener.logger.Error("Can't recreate statistics database",
					zap.Error(err),
				)
				return err
			}
		}
	}
	return nil
}

func (listener *CarbonserverListener) getPathRateLimiter(path string) *ApiPerPathRatelimiter {
	rl, ok := listener.apiPerPathRatelimiter[path]
	if ok {
		return rl
	}
	return nil
}

func (listener *CarbonserverListener) checkRequestCtx(ctx context.Context) error {
	select {
	case <-ctx.Done():
		switch ctx.Err() {
		case context.DeadlineExceeded:
			listener.prometheus.timeoutRequest()
		case context.Canceled:
			listener.prometheus.cancelledRequest()
		}
		return errors.New("context is done")
	default:
	}
	return nil
}

func (listener *CarbonserverListener) shouldBlockForIndex() bool {
	return listener.NoServiceWhenIndexIsNotReady && listener.CurrentFileIndex() == nil
}

func (listener *CarbonserverListener) rateLimitRequest(h http.HandlerFunc) http.HandlerFunc {
	return func(wr http.ResponseWriter, req *http.Request) {
		accepted, locked := listener.beginSharedStoreRequest()
		if !accepted {
			http.Error(wr, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		defer listener.endSharedStoreRequest(locked)
		ratelimiter := listener.getPathRateLimiter(req.URL.Path)
		// Can't use http.TimeoutHandler here due to supporting per-path timeout
		newTimeout := listener.getPathRateLimiterTimeout(ratelimiter)
		if newTimeout > 0 {
			ctx, cancel := context.WithTimeout(req.Context(), newTimeout)
			defer cancel()
			req = req.WithContext(ctx)
		}

		t0 := time.Now()
		ctx := req.Context()
		accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
			zap.String("handler", "rate_limit"),
			zap.String("url", req.URL.RequestURI()),
			zap.String("peer", req.RemoteAddr),
		))

		if listener.shouldBlockForIndex() {
			accessLogger.Error("request denied",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "index not ready"),
				zap.Int("http_code", http.StatusServiceUnavailable),
			)
			http.Error(wr, "Service unavailable (index not ready)", http.StatusServiceUnavailable)
			return
		}

		if ratelimiter != nil {
			if ratelimiter.Enter() != nil {
				http.Error(wr, "Bad request (blocked by api per path rate limiter)", http.StatusBadRequest)
				return
			}
			defer ratelimiter.Exit()

			// why: if the request is already timeout, there is no
			// need to resume execution.
			if listener.checkRequestCtx(ctx) != nil {
				accessLogger.Error("request timeout due to per url rate limiting",
					zap.Duration("runtime_seconds", time.Since(t0)),
					zap.String("reason", "timeout due to per url rate limiting"),
					zap.Int("http_code", http.StatusRequestTimeout),
				)
				http.Error(wr, "Bad request (timeout due to maxInflightRequests)", http.StatusRequestTimeout)
				return
			}
		}

		// TODO: to deprecate as it's replaced by per-path rate limiting?
		//
		// rate limit inflight requests
		inflights := atomic.AddUint64(&listener.metrics.InflightRequests, 1)
		defer atomic.AddUint64(&listener.metrics.InflightRequests, ^uint64(0))
		if listener.MaxInflightRequests > 0 && inflights > listener.MaxInflightRequests {
			atomic.AddUint64(&listener.metrics.RejectedTooManyRequests, 1)

			accessLogger.Error("request denied",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "too many requests"),
				zap.Int("http_code", http.StatusTooManyRequests),
			)
			http.Error(wr, "Bad request (too many requests)", http.StatusTooManyRequests)
			return
		}

		h(wr, req)
	}
}

func (listener *CarbonserverListener) Listen(listen string) error {
	logger := listener.logger

	logger.Info("starting carbonserver",
		zap.String("listen", listen),
		zap.String("whisperData", listener.whisperData),
		zap.Int("maxGlobs", listener.maxGlobs),
		zap.String("scanFrequency", listener.scanFrequency.String()),
	)

	listener.startIndexUpdater()
	listener.initializeCaches()

	carbonserverMux := listener.newHTTPMux()

	tcpAddr, err := net.ResolveTCPAddr("tcp", listen)
	if err != nil {
		return err
	}
	listener.tcpListener, err = net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}

	if listener.internalStatsDir != "" {
		err = listener.initStatsDB()
		if err != nil {
			logger.Error("Failed to reinitialize statistics database")
		} else {
			accessTimes := make(map[string]int64)
			iter := listener.db.NewIterator(nil, nil)
			for iter.Next() {
				// Remember that the contents of the returned slice should not be modified, and
				// only valid until the next call to Next.
				key := iter.Key()
				value := iter.Value()

				v, r := binary.Varint(value)
				if r <= 0 {
					logger.Error("Can't parse value",
						zap.String("key", string(key)),
					)
					continue
				}
				accessTimes[string(key)] = v
			}
			iter.Release()
			err = iter.Error()
			if err != nil {
				logger.Info("Error reading from statistics database",
					zap.Error(err),
				)
				listener.db.Close()
				err = removeDirectory(listener.internalStatsDir)
				if err != nil {
					logger.Error("Failed to reinitialize statistics database",
						zap.Error(err),
					)
				} else {
					err = listener.initStatsDB()
					if err != nil {
						logger.Error("Failed to reinitialize statistics database",
							zap.Error(err),
						)
					}
				}
			}
			listener.UpdateMetricsAccessTimes(accessTimes, true)
		}
	}

	// cache cleaners
	go listener.queryCache.ec.StoppableApproximateCleaner(10*time.Second, listener.exitChan)
	if listener.findCacheEnabled {
		go listener.findCache.ec.StoppableApproximateCleaner(60*time.Second, listener.exitChan)
	}
	if listener.globCacheEnabled {
		go listener.globCache.ec.StoppableApproximateCleaner(60*time.Second, listener.exitChan)
	}

	srv := &http.Server{
		Handler:      gziphandler.GzipHandler(carbonserverMux),
		ReadTimeout:  listener.readTimeout,
		IdleTimeout:  listener.idleTimeout,
		WriteTimeout: listener.writeTimeout,
	}

	listener.httpServer = srv
	listener.serverWG.Add(1)
	go func() {
		defer listener.serverWG.Done()
		if err := srv.Serve(listener.tcpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listener.logger.Error("HTTP server stopped", zap.Error(err))
		}
	}()

	return nil
}

func (listener *CarbonserverListener) startIndexUpdater() {
	if !listener.trigramIndex && !listener.trieIndex {
		return
	}
	if listener.getMetricStore() == nil && listener.scanFrequency == 0 {
		return
	}
	listener.forceScanChan = make(chan struct{}, 1)
	var scanFrequency <-chan time.Time
	if listener.scanFrequency != 0 {
		listener.scanTicker = time.NewTicker(listener.scanFrequency)
		scanFrequency = listener.scanTicker.C
	}
	listener.startFileListUpdater(listener.whisperData, scanFrequency, listener.forceScanChan, listener.exitChan)
	listener.forceScanChan <- struct{}{}
}

func (listener *CarbonserverListener) newHTTPMux() *http.ServeMux {
	mux := http.NewServeMux()
	wrap := func(handler http.HandlerFunc, codes []uint64) http.HandlerFunc {
		return httputil.TrackConnections(httputil.TimeHandler(TraceHandler(listener.rateLimitRequest(handler), statusCodes["combined"], codes, listener.prometheus.request), listener.bucketRequestTimesHTTP))
	}
	mux.HandleFunc("/_internal/capabilities/", wrap(listener.capabilityHandler, statusCodes["capabilities"]))
	mux.HandleFunc("/metrics/find/", wrap(listener.findHandler, statusCodes["find"]))
	mux.HandleFunc("/metrics/list/", wrap(listener.listHandler, statusCodes["list"]))
	mux.HandleFunc("/metrics/list_query/", wrap(listener.listQueryHandler, statusCodes["list"]))
	mux.HandleFunc("/metrics/details/", wrap(listener.detailsHandler, statusCodes["details"]))
	mux.HandleFunc("/render/", wrap(listener.renderHandler, statusCodes["render"]))
	mux.HandleFunc("/info/", wrap(listener.infoHandler, statusCodes["info"]))
	mux.HandleFunc("/forcescan", listener.forceScanHandler)
	mux.HandleFunc("/admin/quota", listener.quotaHandler)
	mux.HandleFunc("/admin/info", listener.adminInfoHandler)
	mux.HandleFunc("/robots.txt", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "User-agent: *\nDisallow: /") })
	return mux
}

func (listener *CarbonserverListener) forceScanHandler(w http.ResponseWriter, _ *http.Request) {
	select {
	case listener.forceScanChan <- struct{}{}:
		w.WriteHeader(http.StatusAccepted)
	case <-time.After(time.Second):
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func (listener *CarbonserverListener) quotaHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Add("Content-Type", "text/plain")
	fidx := listener.CurrentFileIndex()
	if fidx == nil || fidx.trieIdx == nil {
		fmt.Fprintf(w, "index doesn't exist.")
		return
	}
	fidx.trieIdx.getQuotaTree(w)
}

func (listener *CarbonserverListener) adminInfoHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "application/json")
	scopes := parseAdminInfoScopes(r.URL.Query().Get("scopes"))
	infos := make(map[string]map[string]interface{})
	for name, callback := range listener.interfalInfoCallbacks {
		if scopes == nil || scopes[name] {
			infos[name] = callback()
		}
	}
	json.NewEncoder(w).Encode(infos)
}

func parseAdminInfoScopes(value string) map[string]bool {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	scopes := make(map[string]bool)
	for _, scope := range strings.Split(value, ",") {
		scopes[strings.TrimSpace(scope)] = true
	}
	return scopes
}

func (listener *CarbonserverListener) initializeCaches() {
	listener.queryCache = expireCache{ec: expirecache.New(uint64(listener.queryCacheSizeMB))}
	if listener.findCacheEnabled {
		listener.findCache = expireCache{ec: expirecache.New(uint64(listener.findCacheSizeMB))}
	}
	if listener.globCacheEnabled {
		listener.globCache = expireCache{ec: expirecache.New(uint64(listener.globCacheSizeMB))}
	}
	listener.timeBuckets = make([]uint64, listener.buckets+1)
}

func (listener *CarbonserverListener) bucketRequestTimesHTTP(req *http.Request, t time.Duration) {
	bucket := listener.bucketRequestTimes(t)
	if bucket >= listener.buckets {
		listener.logger.Info("slow request",
			zap.String("url", req.URL.RequestURI()),
			zap.String("peer", req.RemoteAddr),
		)
	}
}

func (listener *CarbonserverListener) bucketRequestTimesGRPC(payload, peer string, t time.Duration) {
	bucket := listener.bucketRequestTimes(t)
	if bucket >= listener.buckets {
		listener.logger.Info("slow request",
			zap.String("payload", payload),
			zap.String("peer", peer),
		)
	}
}

func (listener *CarbonserverListener) bucketRequestTimes(t time.Duration) int {
	listener.prometheus.duration(t)

	ms := t.Nanoseconds() / int64(time.Millisecond)

	if len(listener.percentiles) > 0 {
		listener.requestsTimes.Lock()
		listener.requestsTimes.list = append(listener.requestsTimes.list, t.Nanoseconds())
		listener.requestsTimes.Unlock()
	}

	bucket := int(math.Log(float64(ms)) * math.Log10E)

	if bucket < 0 {
		bucket = 0
	}

	if bucket < listener.buckets {
		atomic.AddUint64(&listener.timeBuckets[bucket], 1)
	} else {
		// Too big? Increment overflow bucket
		atomic.AddUint64(&listener.timeBuckets[listener.buckets], 1)
	}
	return bucket
}

func extractTrigrams(query string) []trigram.T {
	if len(query) < 3 {
		return nil
	}

	var start int
	var i int

	var trigrams []trigram.T

	for i < len(query) {
		if query[i] == '[' || query[i] == '*' || query[i] == '?' {
			trigrams = trigram.Extract(query[start:i], trigrams)

			if query[i] == '[' {
				for i < len(query) && query[i] != ']' {
					i++
				}
			}

			start = i + 1
		}
		i++
	}

	if start < i {
		trigrams = trigram.Extract(query[start:i], trigrams)
	}

	return trigrams
}

func (listener *CarbonserverListener) RegisterInternalInfoHandler(name string, f func() map[string]interface{}) {
	if listener.interfalInfoCallbacks == nil {
		listener.interfalInfoCallbacks = map[string]func() map[string]interface{}{}
	}
	listener.interfalInfoCallbacks[name] = f
}

type GlobQueryRateLimiter struct {
	pattern             *regexp.Regexp
	maxInflightRequests chan struct{}
}

func NewGlobQueryRateLimiter(pattern string, m uint) (*GlobQueryRateLimiter, error) {
	exp, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return &GlobQueryRateLimiter{pattern: exp, maxInflightRequests: make(chan struct{}, m)}, nil
}

type ApiPerPathRatelimiter struct {
	maxInflightRequests chan struct{}
	timeout             time.Duration
}

func NewApiPerPathRatelimiter(maxInflightRequests uint, timeout time.Duration) *ApiPerPathRatelimiter {
	return &ApiPerPathRatelimiter{
		maxInflightRequests: make(chan struct{}, maxInflightRequests),
		timeout:             timeout,
	}
}

type RatelimiterError string

func (re RatelimiterError) Error() string {
	return string(re)
}

var (
	BlockedRatelimitError = RatelimiterError("blocked by api per path rate limiter")
)

func (a *ApiPerPathRatelimiter) Enter() error {
	if cap(a.maxInflightRequests) == 0 {
		return BlockedRatelimitError
	}

	a.maxInflightRequests <- struct{}{}
	return nil
}

func (a *ApiPerPathRatelimiter) Exit() {
	select {
	case <-a.maxInflightRequests:
	default:
	}
}

func (listener *CarbonserverListener) ListenGRPC(listen string) error {
	var err error
	var grpcAddr *net.TCPAddr
	grpcAddr, err = net.ResolveTCPAddr("tcp", listen)
	if err != nil {
		return err
	}

	listener.grpcListener, err = net.ListenTCP("tcp", grpcAddr)
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	opts = append(opts,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    60 * time.Second,
			Timeout: 20 * time.Second,
		}))
	// TODO: make initial window size configurable
	opts = append(opts, grpc.InitialWindowSize(4*1024*1024), grpc.InitialConnWindowSize(4*1024*1024))
	opts = append(opts, grpc.ChainStreamInterceptor(
		grpcutil.StreamServerTimeHandler(listener.bucketRequestTimesGRPC),
		grpcutil.StreamServerStatusMetricHandler(statusCodes, listener.prometheus.request),
		listener.StreamServerRatelimitHandler()), grpc.ChainUnaryInterceptor(
		grpcutil.UnaryServerTimeHandler(listener.bucketRequestTimesGRPC),
		grpcutil.UnaryServerStatusMetricHandler(statusCodes, listener.prometheus.request),
		listener.UnaryServerRatelimitHandler()))
	grpcServer := grpc.NewServer(opts...) //skipcq: GO-S0902
	grpcv2.RegisterCarbonV2Server(grpcServer, listener)
	listener.grpcServer = grpcServer
	listener.serverWG.Add(1)
	go func() {
		defer listener.serverWG.Done()
		if err := grpcServer.Serve(listener.grpcListener); err != nil {
			listener.logger.Error("gRPC server stopped", zap.Error(err))
		}
	}()
	return nil
}

func (listener *CarbonserverListener) getPathRateLimiterTimeout(ratelimiter *ApiPerPathRatelimiter) time.Duration {
	var newTimeout time.Duration
	if listener.requestTimeout > 0 {
		newTimeout = listener.requestTimeout
	}
	if ratelimiter != nil && ratelimiter.timeout > 0 {
		newTimeout = ratelimiter.timeout
	}
	return newTimeout
}

func (listener *CarbonserverListener) UnaryServerRatelimitHandler() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		accepted, locked := listener.beginSharedStoreRequest()
		if !accepted {
			return nil, status.Error(codes.Unavailable, "Service unavailable")
		}
		defer listener.endSharedStoreRequest(locked)
		t0 := time.Now()
		var payload string
		if reqStringer, ok := req.(fmt.Stringer); ok {
			payload = reqStringer.String()
		}
		fullMethodName := info.FullMethod
		ratelimiter := listener.getPathRateLimiter(fullMethodName) // Can't use http.TimeoutHandler here due to supporting per-path timeout
		newTimeout := listener.getPathRateLimiterTimeout(ratelimiter)
		if newTimeout > 0 {
			newCtx, cancel := context.WithTimeout(ctx, newTimeout)
			defer cancel()
			ctx = newCtx
		}

		if err := listener.grpcServerRatelimitHandler(ctx, ratelimiter, payload, t0); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (listener *CarbonserverListener) StreamServerRatelimitHandler() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		accepted, locked := listener.beginSharedStoreRequest()
		if !accepted {
			return status.Error(codes.Unavailable, "Service unavailable")
		}
		defer listener.endSharedStoreRequest(locked)
		t0 := time.Now()
		fullMethodName := info.FullMethod
		wss := grpcutil.GetWrappedStream(ss)
		ratelimiter := listener.getPathRateLimiter(fullMethodName) // Can't use http.TimeoutHandler here due to supporting per-path timeout
		newTimeout := listener.getPathRateLimiterTimeout(ratelimiter)
		if newTimeout > 0 {
			ctx, cancel := context.WithTimeout(wss.Context(), newTimeout)
			defer cancel()
			wss.SetContext(ctx)
		}

		ctx := wss.Context()
		if err := listener.grpcServerRatelimitHandler(ctx, ratelimiter, wss.Payload(), t0); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func (listener *CarbonserverListener) grpcServerRatelimitHandler(ctx context.Context, ratelimiter *ApiPerPathRatelimiter, payload string, t0 time.Time) error {
	var reqPeer string
	if p, ok := peer.FromContext(ctx); ok {
		reqPeer = p.Addr.String()
	}
	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "rate_limit"),
		zap.String("payload", payload),
		zap.String("peer", reqPeer),
	))

	if listener.shouldBlockForIndex() {
		accessLogger.Error("request denied",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "index not ready"),
			zap.Int("grpc_code", int(codes.Unavailable)),
		)
		return status.Error(codes.Unavailable, "Service unavailable (index not ready)")
	}

	if ratelimiter != nil {
		if ratelimiter.Enter() != nil {
			accessLogger.Error("request blocked",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "blocked by api per path rate limiter"),
				zap.Int("grpc_code", int(codes.InvalidArgument)),
			)
			return status.Error(codes.InvalidArgument, "blocked by api per path rate limiter")
		}
		defer ratelimiter.Exit()

		// why: if the request is already timeout, there is no
		// need to resume execution.
		if listener.checkRequestCtx(ctx) != nil {
			accessLogger.Error("request timeout due to per url rate limiting",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "timeout due to per url rate limiting"),
				zap.Int("grpc_code", int(codes.ResourceExhausted)),
			)
			return status.Error(codes.ResourceExhausted, "timeout due to maxInflightRequests")
		}
	}

	// TODO: to deprecate as it's replaced by per-path rate limiting?
	//
	// rate limit inflight requests
	inflights := atomic.AddUint64(&listener.metrics.InflightRequests, 1)
	defer atomic.AddUint64(&listener.metrics.InflightRequests, ^uint64(0))
	if listener.MaxInflightRequests > 0 && inflights > listener.MaxInflightRequests {
		atomic.AddUint64(&listener.metrics.RejectedTooManyRequests, 1)
		accessLogger.Error("request denied",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "too many requests"),
			zap.Int("grpc_code", http.StatusTooManyRequests),
		)
		return status.Error(codes.ResourceExhausted, "too many requests")
	}
	return nil
}

func getWithCache(logger *zap.Logger, cache expireCache, key string, size uint64, expire int32, f func() (interface{}, error)) (result interface{}, fromCache bool, err error) {
	item := cache.getQueryItem(key, size, expire)
	res, ok := item.FetchOrLock()
	switch {
	case !ok:
		logger.Debug("cache miss")
		result, err = f()
		if err != nil {
			item.StoreAbort()
		} else {
			item.StoreAndUnlock(result)
		}
	case res != nil:
		logger.Debug("cache hit")
		result = res
		fromCache = true
	default:
		// Whenever there are multiple requests approaching for the same records,
		// and the proceeding request gets an error, waiting requests should get an error too.
		err = fmt.Errorf("invalid cache record for the request")
	}
	return
}
