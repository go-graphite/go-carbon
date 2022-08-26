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
	_ "net/http/pprof"
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
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/NYTimes/gziphandler"
	"github.com/dgryski/go-expirecache"
	"github.com/dgryski/go-trigram"
	"github.com/dgryski/httputil"
	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/helper/grpcutil"
	"github.com/go-graphite/go-carbon/helper/stat"
	"github.com/go-graphite/go-carbon/points"
	grpcv2 "github.com/go-graphite/protocol/carbonapi_v2_grpc"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/zapwriter"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"google.golang.org/grpc"
)

type metricStruct struct {
	RenderRequests       uint64
	RenderErrors         uint64
	NotFound             uint64
	FindRequests         uint64
	FindErrors           uint64
	FindZero             uint64
	InfoRequests         uint64
	InfoErrors           uint64
	ListRequests         uint64
	ListErrors           uint64
	ListQueryRequests    uint64
	ListQueryErrors      uint64
	DetailsRequests      uint64
	DetailsErrors        uint64
	CacheHit             uint64
	CacheMiss            uint64
	CacheRequestsTotal   uint64
	CacheWorkTimeNS      uint64
	CacheWaitTimeFetchNS uint64
	DiskWaitTimeNS       uint64
	DiskRequests         uint64
	PointsReturned       uint64
	MetricsReturned      uint64
	MetricsKnown         uint64
	FileScanTimeNS       uint64
	IndexBuildTimeNS     uint64
	MetricsFetched       uint64
	MetricsFound         uint64
	FetchSize            uint64
	QueryCacheHit        uint64
	QueryCacheMiss       uint64
	FindCacheHit         uint64
	FindCacheMiss        uint64
	TrieNodes            uint64
	TrieFiles            uint64
	TrieDirs             uint64
	TrieCountNodesTimeNs uint64
	QuotaApplyTimeNs     uint64
	UsageRefreshTimeNs   uint64

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

// interface to retrive retention and aggregation
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

type queryCache struct {
	ec *expirecache.Cache
}

func (q *queryCache) getQueryItem(k string, size uint64, expire int32) *QueryItem {
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
	failOnMaxGlobs    bool
	percentiles       []int
	scanFrequency     time.Duration
	scanTicker        *time.Ticker
	forceScanChan     chan struct{}
	metricsAsCounters bool
	tcpListener       *net.TCPListener
	grpcListener      *net.TCPListener
	logger            *zap.Logger
	accessLogger      *zap.Logger
	internalStatsDir  string
	flock             bool
	compressed        bool
	removeEmptyFile   bool

	maxMetricsGlobbed  int
	maxMetricsRendered int

	queryCacheEnabled bool
	queryCacheSizeMB  int
	queryCache        queryCache
	findCacheEnabled  bool
	findCache         queryCache
	trigramIndex      bool
	trieIndex         bool
	concurrentIndex   bool

	fileListCacheVersion FLCVersion
	fileListCache        string

	realtimeIndex  int
	newMetricsChan chan string

	fileIdx      atomic.Value
	fileIdxMutex sync.Mutex

	metrics       *metricStruct
	requestsTimes requestsTimes
	exitChan      chan struct{}
	timeBuckets   []uint64

	cacheGetRecentMetrics func() []map[string]struct{}
	whisperGetConfig      configRetriever

	prometheus prometheus

	db *leveldb.DB

	quotas                    []*Quota
	estimateSize              func(metric string) (size, dataPoints int64)
	quotaAndUsageMetrics      chan []points.Points
	quotaUsageReportFrequency time.Duration

	interfalInfoCallbacks map[string]func() map[string]interface{}

	// resource control
	MaxInflightRequests          uint64 // TODO: to deprecate
	NoServiceWhenIndexIsNotReady bool
	apiPerPathRatelimiter        map[string]*ApiPerPathRatelimiter
	globQueryRateLimiters        []*GlobQueryRateLimiter
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
	typ int //nolint:unused,structcheck //skipcq: SCC-U1000

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
		metricsAsCounters: false,
		cacheGet:          cacheGetFunc,
		logger:            zapwriter.Logger("carbonserver"),
		accessLogger:      zapwriter.Logger("access"),
		findCache:         queryCache{ec: expirecache.New(0)},
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
		apiPerPathRatelimiter: map[string]*ApiPerPathRatelimiter{},
		fileListCacheVersion:  FLCVersion1,
	}
}

func (listener *CarbonserverListener) SetWhisperData(whisperData string) {
	listener.whisperData = strings.TrimRight(whisperData, "/")
}
func (listener *CarbonserverListener) SetMaxGlobs(maxGlobs int) {
	listener.maxGlobs = maxGlobs
}
func (listener *CarbonserverListener) SetEmptyResultOk(emptyResultOk bool) {
	listener.emptyResultOk = emptyResultOk
}
func (listener *CarbonserverListener) SetFailOnMaxGlobs(failOnMaxGlobs bool) {
	listener.failOnMaxGlobs = failOnMaxGlobs
}
func (listener *CarbonserverListener) SetMaxMetricsGlobbed(max int) {
	listener.maxMetricsGlobbed = max
}
func (listener *CarbonserverListener) SetMaxMetricsRendered(max int) {
	listener.maxMetricsRendered = max
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
func (listener *CarbonserverListener) SetFindCacheEnabled(enabled bool) {
	listener.findCacheEnabled = enabled
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
func (listener *CarbonserverListener) SetEstimateSize(f func(metric string) (size, dataPoints int64)) {
	listener.estimateSize = f
}
func (listener *CarbonserverListener) SetQuotas(quotas []*Quota) {
	listener.quotas = quotas
}
func (listener *CarbonserverListener) isQuotaEnabled() bool {
	return listener.quotas != nil
}
func (listener *CarbonserverListener) ShouldThrottleMetric(ps *points.Points, inCache bool) bool {
	fidx := listener.CurrentFileIndex()
	if fidx == nil || fidx.trieIdx == nil {
		return false
	}

	var throttled = fidx.trieIdx.throttle(ps, inCache)

	return throttled
}
func (listener *CarbonserverListener) SetMaxInflightRequests(max uint64) {
	listener.MaxInflightRequests = max
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

func (listener *CarbonserverListener) fileListUpdater(dir string, scanFrequency <-chan time.Time, force <-chan struct{}, exit <-chan struct{}) {
	cacheMetricNames := make(map[string]struct{})

	var knownMetricsStatTicker, quotaAndUsageStatTicker <-chan time.Time
	if listener.isQuotaEnabled() {
		ticker := time.NewTicker(listener.quotaUsageReportFrequency)
		defer ticker.Stop()

		quotaAndUsageStatTicker = ticker.C
	} else if listener.trieIndex && listener.concurrentIndex && listener.realtimeIndex > 0 {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		knownMetricsStatTicker = ticker.C
	}

uloop:
	for {
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
			listener.statKnonwnMetrics(knownMetricsStatTicker)

			continue uloop
		case <-quotaAndUsageStatTicker:
			listener.refreshQuotaAndUsage(quotaAndUsageStatTicker)

			continue uloop
		case m := <-listener.newMetricsChan:
			// listener.newMetricsChan might have high traffic, but
			// in theory, there should be no starvation on other channels:
			// https://groups.google.com/g/golang-nuts/c/4BR2Sdb6Zzk (2015)

			fidx := listener.CurrentFileIndex()
			if listener.trieIndex && listener.concurrentIndex && fidx != nil && fidx.trieIdx != nil {
				metric := "/" + filepath.Clean(strings.ReplaceAll(m, ".", "/")+".wsp")

				if _, err := fidx.trieIdx.insert(metric, 0, 0, 0, 0); err != nil {
					listener.logTrieInsertError(listener.logger, "failed to insert new metrics for realtime indexing", metric, err)
				}
			}

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

func (listener *CarbonserverListener) statKnonwnMetrics(knownMetricsStatTicker <-chan time.Time) {
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
	defer func() {
		// drain remaining blocked tickers
		for {
			select {
			case <-quotaAndUsageStatTicker:
			default:
				return
			}
		}
	}()

	fidx := listener.CurrentFileIndex()

	if !listener.isQuotaEnabled() || !listener.concurrentIndex || listener.realtimeIndex <= 0 || fidx == nil || fidx.trieIdx == nil {
		return
	}

	quotaStart := time.Now()
	throughputs, err := fidx.trieIdx.applyQuotas(listener.quotaUsageReportFrequency, listener.quotas...)
	if err != nil {
		listener.logger.Error(
			"refreshQuotaAndUsage",
			zap.Error(err),
		)
	}

	quotaTime := uint64(time.Since(quotaStart))
	atomic.StoreUint64(&listener.metrics.QuotaApplyTimeNs, quotaTime)

	usageStart := time.Now()
	files := fidx.trieIdx.refreshUsage(throughputs)
	usageTime := uint64(time.Since(usageStart))
	atomic.StoreUint64(&listener.metrics.UsageRefreshTimeNs, usageTime)

	// set using the indexed files, instead of returning on-disk files.
	//
	// WHY: quota subsystem atm can only be enabled along with concurrent
	// and realtime idnex, and with concurrent and realtime index, disk
	// scan should be set at an interval like 2 hours or longer. counting
	// the files in trie index gives us more timely visibilitty into how
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

func (listener *CarbonserverListener) updateFileList(dir string, cacheMetricNames map[string]struct{}, quotaAndUsageStatTicker <-chan time.Time) (readFromCache bool) {
	logger := listener.logger.With(zap.String("handler", "fileListUpdated"))
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic encountered",
				zap.Stack("stack"),
				zap.Any("error", r),
			)
		}
	}()

	var t0 = time.Now()
	var fidx = listener.CurrentFileIndex()
	var files []string
	var filesLen int
	var details = make(map[string]*protov3.MetricDetails)
	var trieIdx *trieIndex
	var metricsKnown uint64
	var infos []zap.Field
	if listener.trieIndex {
		if fidx == nil || !listener.concurrentIndex {
			trieIdx = newTrie(".wsp", listener.estimateSize)
		} else {
			trieIdx = fidx.trieIdx
			trieIdx.root.gen++
		}
	}

	// populate index for all the metric names in cache
	// the iteration takes place only when cache-scan is enabled in conf
	var tcache = time.Now()
	var cacheMetricLen = len(cacheMetricNames)
	for fileName := range cacheMetricNames {
		if listener.trieIndex {
			if _, err := trieIdx.insert(fileName, 0, 0, 0, 0); err != nil {
				listener.logTrieInsertError(logger, "error populating index from cache indexMap", fileName, err)
			}
		} else {
			files = append(files, fileName)
		}
		if strings.HasSuffix(fileName, ".wsp") {
			metricsKnown++
		}
	}
	cacheIndexRuntime := time.Since(tcache)

	// readFromCache only run once at the start of the program.
	// A new version is generated everytime a file list scan is completed.
	if fidx == nil && listener.fileListCache != "" {
		// why not listener.fileListCacheVersion: this is for
		// transparent file list cache version upgrade and reverse.
		flc, err := NewFileListCache(listener.fileListCache, FLCVersionUnspecified, 'r')
		if err != nil {
			if !os.IsNotExist(err) {
				logger.Error("failed to read file list cache", zap.Error(err))
			}
		} else {
			infos = append(infos, zap.Int("file_list_cache_version", int(flc.GetVersion())))

			readFromCache = true
			for {
				entry, err := flc.Read()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						infos = append(infos, zap.NamedError("file_list_cache_read_error", err))

						readFromCache = false
						trieIdx = newTrie(".wsp", listener.estimateSize)
					}

					break
				}

				if entry.Path == "" {
					continue
				}

				if _, err := trieIdx.insert(entry.Path, entry.LogicalSize, entry.PhysicalSize, entry.DataPoints, entry.FirstSeenAt); err != nil {
					listener.logTrieInsertError(logger, "failed to read from file list cache", entry.Path, err)

					readFromCache = false
					trieIdx = newTrie(".wsp", listener.estimateSize)

					break
				}

				filesLen++
				if strings.HasSuffix(entry.Path, ".wsp") {
					metricsKnown++
				}
			}
			if err := flc.Close(); err != nil {
				logger.Error("failed to close file list cache", zap.Error(err))
			}
		}
	}

	if !readFromCache {
		var flc FileListCache
		if listener.fileListCache != "" {
			var err error
			flc, err = NewFileListCache(listener.fileListCache, listener.fileListCacheVersion, 'w')
			if err != nil {
				if !os.IsNotExist(err) {
					logger.Error("failed to create file list cache", zap.Error(err))
				}
			} else {
				defer func() {
					// flc could be reset to nil during filepath walk
					if flc != nil {
						if err := flc.Close(); err != nil {
							logger.Error("failed to close flie list cache", zap.Error(err))
						}
					}
				}()
			}

			infos = append(infos, zap.Int("file_list_cache_version", int(flc.GetVersion())))
		}

		if fi, err := os.Lstat(dir); err != nil {
			logger.Error("failed to stat whisper data directory", zap.String("path", dir), zap.Error(err))
		} else if fi.Mode()&os.ModeSymlink == 1 {
			logger.Error("can't index symlink data dir", zap.String("path", dir))
		}

		err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				logger.Info("error processing", zap.String("path", p), zap.Error(err))
				return nil
			}

			// WHY: as filepath.walk could potentially taking a long
			// time to complete (>= 5 minutes or more), depending
			// on how many files are there on disk. It's nice to
			// have consistent quota and usage metrics produced as
			// regularly as possible according to the
			// quotaUsageReportFrequency specified in the config.
			if listener.isQuotaEnabled() {
				select {
				case <-quotaAndUsageStatTicker:
					listener.refreshQuotaAndUsage(quotaAndUsageStatTicker)
				default:
				}
			}

			// WHY: as filepath.walk could potentially taking a long
			// time to complete (>= 5 minutes or more), depending
			// on how many files are there on disk. It's nice to
			// try to flush newMetricsChan if possible.
			//
			// TODO: only trigger enter the loop when it's half full?
			// 	len(listener.newMetricsChan) >= cap(listener.newMetricsChan)/2
			if listener.trieIndex && listener.concurrentIndex && listener.newMetricsChan != nil {
			newMetricsLoop:
				for {
					select {
					case m := <-listener.newMetricsChan:
						fileName := "/" + filepath.Clean(strings.ReplaceAll(m, ".", "/")+".wsp")
						if _, err := trieIdx.insert(fileName, 0, 0, 0, 0); err != nil {
							listener.logTrieInsertError(logger, "failed to update realtime trie index", m, err)
						}
					default:
						break newMetricsLoop
					}
				}
			}

			isFullMetric := strings.HasSuffix(info.Name(), ".wsp")
			if info.IsDir() || isFullMetric { // both dir and metric file is needed for supporting trigram index.
				trimmedName := strings.TrimPrefix(p, listener.whisperData)
				filesLen++

				var dataPoints, logicalSize, physicalSize int64
				if isFullMetric {
					if listener.estimateSize != nil {
						m := strings.ReplaceAll(trimmedName, "/", ".")
						m = m[1 : len(m)-4]
						_, dataPoints = listener.estimateSize(m)
					}
					logicalSize = info.Size()
					physicalSize = logicalSize
					if stat, ok := info.Sys().(*syscall.Stat_t); ok {
						physicalSize = stat.Blocks * 512
					}
				}

				var metricFirstSeenAt int64

				// use cacheMetricNames to check and prevent appending duplicate metrics
				// into the index when cacheMetricNamesIndex is enabled
				if _, present := cacheMetricNames[trimmedName]; present {
					delete(cacheMetricNames, trimmedName)
				} else {
					if listener.trieIndex {
						// WHY:
						//   * this would only affects empty directories
						//   * and empty dir isn't useful (at least most of the time)?
						if isFullMetric {
							if node, err := trieIdx.insert(trimmedName, logicalSize, physicalSize, dataPoints, 0); err != nil {
								// It's better to just log an error than stop indexing
								listener.logTrieInsertError(logger, "updateFileList.trie: failed to index path", trimmedName, err)
							} else if node.meta != nil {
								metricFirstSeenAt = node.meta.(*fileMeta).firstSeenAt
							}
						}
					} else {
						files = append(files, trimmedName)
					}

					if isFullMetric {
						metricsKnown++
					}
				}

				// only metric paths are cached for trie index. trigram index needs to index dir path as well.
				if flc != nil && (!listener.trieIndex || isFullMetric) {
					if err := flc.Write(&FLCEntry{
						Path:         trimmedName,
						DataPoints:   dataPoints,
						LogicalSize:  logicalSize,
						PhysicalSize: physicalSize,
						FirstSeenAt:  metricFirstSeenAt,
					}); err != nil {
						logger.Error("failed to write to file list cache", zap.Error(err))
						if err := flc.Close(); err != nil {
							logger.Error("failed to close flie list cache", zap.Error(err))
						}
						flc = nil
					}
				}

				if isFullMetric && listener.internalStatsDir != "" {
					i := stat.GetStat(info)
					trimmedName = strings.ReplaceAll(trimmedName[1:len(trimmedName)-4], "/", ".")
					details[trimmedName] = &protov3.MetricDetails{
						Size:     i.Size,
						ModTime:  i.MTime,
						ATime:    i.ATime,
						RealSize: i.RealSize,
					}
				}
			}

			return nil
		})
		if err != nil {
			logger.Error("error getting file list",
				zap.Error(err),
			)
		}
	}

	if listener.concurrentIndex && trieIdx != nil {
		trieIdx.prune()
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		logger.Info("error getting FS Stats",
			zap.String("dir", dir),
			zap.Error(err),
		)
		return
	}

	var freeSpace uint64
	// diskspace can be negative and Bavail is therefore int64
	if stat.Bavail >= 0 { // nolint:staticcheck // skipcq: SCC-SA4003
		freeSpace = uint64(stat.Bavail) * uint64(stat.Bsize)
	}
	totalSpace := stat.Blocks * uint64(stat.Bsize)

	fileScanRuntime := time.Since(t0)
	atomic.StoreUint64(&listener.metrics.MetricsKnown, metricsKnown)
	atomic.AddUint64(&listener.metrics.FileScanTimeNS, uint64(fileScanRuntime.Nanoseconds()))

	nfidx := &fileIndex{
		details:     details,
		freeSpace:   freeSpace,
		totalSpace:  totalSpace,
		accessTimes: make(map[string]int64),
	}

	var pruned int
	var indexType = "trigram"
	var tindex = time.Now()
	var indexSize int
	if listener.trieIndex {
		indexType = "trie"
		nfidx.trieIdx = trieIdx
		infos = append(
			infos,
			zap.Int("trie_depth", int(nfidx.trieIdx.depth)),
			zap.String("longest_metric", nfidx.trieIdx.longestMetric),
		)

		if listener.trigramIndex && !listener.concurrentIndex {
			start := time.Now()
			nfidx.trieIdx.setTrigrams()
			infos = append(infos, zap.Duration("set_trigram_time", time.Since(start)))
		}

		start := time.Now()
		count, files, dirs, _, _, _, _, _ := trieIdx.countNodes()
		atomic.StoreUint64(&listener.metrics.TrieNodes, uint64(count))
		atomic.StoreUint64(&listener.metrics.TrieFiles, uint64(files))
		atomic.StoreUint64(&listener.metrics.TrieDirs, uint64(dirs))
		infos = append(infos, zap.Duration("trie_count_nodes_time", time.Since(start)))

		indexSize = count
	} else {
		nfidx.files = files
		nfidx.idx = trigram.NewIndex(files)
		pruned = nfidx.idx.Prune(0.95)
		indexSize = len(nfidx.idx)
	}
	indexingRuntime := time.Since(tindex) // note: no longer meaningful for trie index
	atomic.AddUint64(&listener.metrics.IndexBuildTimeNS, uint64(indexingRuntime.Nanoseconds()))

	var tl = time.Now()
	if fidx != nil && listener.internalStatsDir != "" {
		listener.fileIdxMutex.Lock()
		for m := range fidx.accessTimes {
			if d, ok := details[m]; ok {
				d.RdTime = fidx.accessTimes[m]
			} else {
				delete(fidx.accessTimes, m)
				if listener.db != nil {
					listener.db.Delete([]byte(m), nil)
				}
			}
		}
		nfidx.accessTimes = fidx.accessTimes
		listener.fileIdxMutex.Unlock()
	}
	var rdTimeUpdateRuntime = time.Since(tl)

	listener.UpdateFileIndex(nfidx)

	infos = append(infos,
		zap.Duration("file_scan_runtime", fileScanRuntime),
		zap.Duration("indexing_runtime", indexingRuntime),
		zap.Duration("rdtime_update_runtime", rdTimeUpdateRuntime),
		zap.Duration("cache_index_runtime", cacheIndexRuntime),
		zap.Duration("total_runtime", time.Since(t0)),
		zap.Int("Files", filesLen),
		zap.Int("index_size", indexSize),
		zap.Int("pruned_trigrams", pruned),
		zap.Int("cache_metric_len_before", cacheMetricLen),
		zap.Int("cache_metric_len_after", len(cacheMetricNames)),
		zap.Uint64("metrics_known", metricsKnown),
		zap.String("index_type", indexType),
		zap.Bool("read_from_cache", readFromCache),
	)
	logger.Info("file list updated", infos...)

	return
}

func (*CarbonserverListener) logTrieInsertError(logger *zap.Logger, msg, metric string, err error) {
	zfields := []zap.Field{zap.Error(err), zap.String("metric", metric)}
	if ierr, ok := err.(*trieInsertError); ok {
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

	// Rate limit heavy globbing queries like: *.*.*.*keyword*.
	//
	// Why: it's expensive to scan the whole index while looking for
	// keywords, especially for trie and file system glob.
	for _, rl := range listener.globQueryRateLimiters {
		if !rl.pattern.MatchString(query) {
			continue
		}
		if cap(rl.maxInflightRequests) == 0 {
			err := fmt.Errorf("rejected by query rate limiter: %s", rl.pattern.String())
			resultCh <- &ExpandedGlobResponse{query, nil, nil, nil, 0, err}
			return
		}

		rl.maxInflightRequests <- struct{}{}
		defer func() {
			<-rl.maxInflightRequests
		}()

		// why: no need to continue execution if the request is already timeout.
		if listener.checkRequestCtx(ctx) != nil {
			err := fmt.Errorf("time out due to heavy glob query rate limiter: %s", rl.pattern.String())
			resultCh <- &ExpandedGlobResponse{query, nil, nil, nil, 0, err}
			return
		}

		break
	}

	logger := TraceContextToZap(ctx, listener.logger)
	matchedCount := 0
	defer func(start time.Time) {
		dur := time.Since(start)
		if dur <= time.Second {
			return
		}
		var itype string
		if listener.trieIndex {
			itype = "trie"
			if listener.trigramIndex {
				itype = "trie-trigram"
			}
		} else if listener.trigramIndex {
			itype = "trigram"
		}
		logger.Info("slow_expand_globs", zap.Duration("time", dur), zap.String("query", query), zap.Int("matched_count", matchedCount), zap.String("index_type", itype))
	}(time.Now())

	if listener.trieIndex && listener.CurrentFileIndex() != nil {
		files, leafs, nodes, lookups, err := listener.expandGlobsTrie(query)
		resultCh <- &ExpandedGlobResponse{query, files, leafs, nodes, lookups, err}
		return
	}

	var useGlob bool

	// TODO: Find out why we have set 'useGlob' if 'star == -1'
	if star := strings.IndexByte(query, '*'); listener.cacheGetRecentMetrics == nil &&
		strings.IndexByte(query, '[') == -1 &&
		strings.IndexByte(query, '?') == -1 &&
		(star == -1 || star == len(query)-1) {
		useGlob = true
	}
	logger = logger.With(zap.Bool("use_glob", useGlob))

	/* things to glob:
	 * - carbon.relays  -> carbon.relays
	 * - carbon.re      -> carbon.relays, carbon.rewhatever
	 * - carbon.[rz]    -> carbon.relays, carbon.zipper
	 * - carbon.{re,zi} -> carbon.relays, carbon.zipper
	 * - match is either dir or .wsp file
	 * unfortunately, filepath.Glob doesn't handle the curly brace
	 * expansion for us */

	query = strings.ReplaceAll(query, ".", "/")

	var globs []string
	if !strings.HasSuffix(query, "*") {
		globs = append(globs, query+".wsp")
		logger.Debug("appending file to globs struct",
			zap.Strings("globs", globs),
		)
	}
	globs = append(globs, query)
	globs, err := listener.expandGlobBraces(globs)
	if err != nil {
		resultCh <- &ExpandedGlobResponse{query, nil, nil, nil, 0, err}
		return
	}

	fidx := listener.CurrentFileIndex()
	var files []string
	fallbackToFS := false
	if !listener.trigramIndex || fidx == nil || len(fidx.files) == 0 {
		fallbackToFS = true
	}

	if fidx != nil && !useGlob {
		// use the index
		docs := make(map[trigram.DocID]struct{})

		for _, g := range globs {
			gpath := "/" + g
			ts := extractTrigrams(g)

			// TODO(dgryski): If we have 'not enough trigrams' we
			// should bail and use the file-system glob instead

			ids := fidx.idx.QueryTrigrams(ts)
			for _, id := range ids {
				docid := trigram.DocID(id)
				if _, ok := docs[docid]; !ok {
					matched, err := filepath.Match(gpath, fidx.files[id])
					if err == nil && matched {
						docs[docid] = struct{}{}
					}
				}
			}
		}

		for id := range docs {
			files = append(files, listener.whisperData+fidx.files[id])
		}

		sort.Strings(files)
	}

	// Not an 'else' clause because the trigram-searching code might want
	// to fall back to the file-system glob

	if useGlob || fallbackToFS {
		// no index or we were asked to hit the filesystem
		for _, g := range globs {
			nfiles, err := filepath.Glob(listener.whisperData + "/" + g)
			if err == nil {
				files = append(files, nfiles...)
			}
		}
	}

	leafs := make([]bool, len(files))
	for i, p := range files {
		s, err := os.Stat(p)
		switch {
		case err == nil:
			// exists on disk
			p = p[len(listener.whisperData+"/"):]
			if !s.IsDir() && strings.HasSuffix(p, ".wsp") {
				p = p[:len(p)-4]
				leafs[i] = true
			} else {
				leafs[i] = false
			}
			files[i] = strings.ReplaceAll(p, "/", ".")
		case os.IsNotExist(err):
			// cache-only, so no fileinfo
			// mark "leafs" based on wsp suffix
			p = p[len(listener.whisperData+"/"):]
			if strings.HasSuffix(p, ".wsp") {
				p = p[:len(p)-4]
				leafs[i] = true
			} else {
				leafs[i] = false
			}
			files[i] = strings.ReplaceAll(p, "/", ".")
		default:
			continue
		}
	}

	matchedCount = len(files)
	resultCh <- &ExpandedGlobResponse{query, files, leafs, nil, 0, nil}
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
	pauseNS := uint64(m.PauseTotalNs)
	alloc := uint64(m.Alloc)
	totalAlloc := uint64(m.TotalAlloc)
	numGC := uint64(m.NumGC)

	sender("render_requests", &listener.metrics.RenderRequests, send)
	sender("render_errors", &listener.metrics.RenderErrors, send)
	sender("notfound", &listener.metrics.NotFound, send)
	sender("find_requests", &listener.metrics.FindRequests, send)
	sender("find_errors", &listener.metrics.FindErrors, send)
	sender("find_zero", &listener.metrics.FindZero, send)
	sender("list_requests", &listener.metrics.ListRequests, send)
	sender("list_errors", &listener.metrics.ListErrors, send)
	sender("details_requests", &listener.metrics.DetailsRequests, send)
	sender("details_errors", &listener.metrics.DetailsErrors, send)
	sender("cache_hit", &listener.metrics.CacheHit, send)
	sender("cache_miss", &listener.metrics.CacheMiss, send)
	sender("cache_work_time_ns", &listener.metrics.CacheWorkTimeNS, send)
	sender("cache_wait_time_fetch_ns", &listener.metrics.CacheWaitTimeFetchNS, send)
	sender("cache_requests", &listener.metrics.CacheRequestsTotal, send)
	sender("disk_wait_time_ns", &listener.metrics.DiskWaitTimeNS, send)
	sender("disk_requests", &listener.metrics.DiskRequests, send)
	sender("points_returned", &listener.metrics.PointsReturned, send)
	sender("metrics_returned", &listener.metrics.MetricsReturned, send)
	sender("metrics_found", &listener.metrics.MetricsFound, send)
	sender("fetch_size_bytes", &listener.metrics.FetchSize, send)

	senderRaw("metrics_known", &listener.metrics.MetricsKnown, send)
	sender("index_build_time_ns", &listener.metrics.IndexBuildTimeNS, send)
	sender("file_scan_time_ns", &listener.metrics.FileScanTimeNS, send)

	sender("query_cache_hit", &listener.metrics.QueryCacheHit, send)
	sender("query_cache_miss", &listener.metrics.QueryCacheMiss, send)

	sender("find_cache_hit", &listener.metrics.FindCacheHit, send)
	sender("find_cache_miss", &listener.metrics.FindCacheMiss, send)

	sender("inflight_requests_count", &listener.metrics.InflightRequests, send)
	sender("inflight_requests_limit", &listener.MaxInflightRequests, send)
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
	for i := 0; i <= listener.buckets; i++ {
		sender(fmt.Sprintf("requests_in_%dms_to_%dms", i*100, (i+1)*100), &listener.timeBuckets[i], send)
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
	close(listener.forceScanChan)
	if listener.scanTicker != nil {
		listener.scanTicker.Stop()
	}
	close(listener.exitChan)
	if listener.db != nil {
		listener.db.Close()
	}
	listener.tcpListener.Close()
	listener.grpcListener.Close()
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

	listener.exitChan = make(chan struct{})
	if (listener.trigramIndex || listener.trieIndex) && listener.scanFrequency != 0 {
		listener.forceScanChan = make(chan struct{})
		listener.scanTicker = time.NewTicker(listener.scanFrequency)
		go listener.fileListUpdater(listener.whisperData, listener.scanTicker.C, listener.forceScanChan, listener.exitChan) //nolint:staticcheck
		listener.forceScanChan <- struct{}{}
	}

	listener.queryCache = queryCache{ec: expirecache.New(uint64(listener.queryCacheSizeMB))}

	// +1 to track every over the number of buckets we track
	listener.timeBuckets = make([]uint64, listener.buckets+1)

	carbonserverMux := http.NewServeMux()
	wrapHandler := func(h http.HandlerFunc, handlerStatusCodes []uint64) http.HandlerFunc {
		return httputil.TrackConnections(
			httputil.TimeHandler(
				TraceHandler(
					listener.rateLimitRequest(h),
					statusCodes["combined"],
					handlerStatusCodes,
					listener.prometheus.request,
				),
				listener.bucketRequestTimesHTTP,
			),
		)
	}

	carbonserverMux.HandleFunc("/_internal/capabilities/", wrapHandler(listener.capabilityHandler, statusCodes["capabilities"]))
	carbonserverMux.HandleFunc("/metrics/find/", wrapHandler(listener.findHandler, statusCodes["find"]))
	carbonserverMux.HandleFunc("/metrics/list/", wrapHandler(listener.listHandler, statusCodes["list"]))
	carbonserverMux.HandleFunc("/metrics/list_query/", wrapHandler(listener.listQueryHandler, statusCodes["list"]))
	carbonserverMux.HandleFunc("/metrics/details/", wrapHandler(listener.detailsHandler, statusCodes["details"]))
	carbonserverMux.HandleFunc("/render/", wrapHandler(listener.renderHandler, statusCodes["render"]))
	carbonserverMux.HandleFunc("/info/", wrapHandler(listener.infoHandler, statusCodes["info"]))

	carbonserverMux.HandleFunc("/forcescan", func(w http.ResponseWriter, r *http.Request) {
		select {
		case listener.forceScanChan <- struct{}{}:
			w.WriteHeader(http.StatusAccepted)
		case <-time.After(time.Second):
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	carbonserverMux.HandleFunc("/admin/quota", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/plain")

		fidx := listener.CurrentFileIndex()
		if fidx == nil && fidx.trieIdx == nil {
			fmt.Fprintf(w, "index doesn't exist.")
			return
		}

		fidx.trieIdx.getQuotaTree(w)
	})
	carbonserverMux.HandleFunc("/admin/info", func(w http.ResponseWriter, r *http.Request) {
		// URL: /admin/info?scopes=cache,config
		w.Header().Add("Content-Type", "application/json")

		// Parameter "scopes" is a csv string. Valid values: cache, config.
		// By default, /admin/info returns all admin info
		var scopes map[string]bool
		if fs := strings.TrimSpace(r.URL.Query().Get("scopes")); fs != "" {
			scopes = map[string]bool{}
			for _, f := range strings.Split(fs, ",") {
				scopes[strings.TrimSpace(f)] = true
			}
		}

		infos := map[string]map[string]interface{}{}
		for name, f := range listener.interfalInfoCallbacks {
			if scopes != nil && !scopes[name] {
				continue
			}

			infos[name] = f()
		}

		json.NewEncoder(w).Encode(infos)
	})

	carbonserverMux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "User-agent: *\nDisallow: /")
	})

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

	go listener.queryCache.ec.StoppableApproximateCleaner(10*time.Second, listener.exitChan)

	srv := &http.Server{
		Handler:      gziphandler.GzipHandler(carbonserverMux),
		ReadTimeout:  listener.readTimeout,
		IdleTimeout:  listener.idleTimeout,
		WriteTimeout: listener.writeTimeout,
	}

	go srv.Serve(listener.tcpListener)

	return nil
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

func NewGlobQueryRateLimiter(pattern string, max uint) (*GlobQueryRateLimiter, error) {
	exp, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return &GlobQueryRateLimiter{pattern: exp, maxInflightRequests: make(chan struct{}, max)}, nil
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
	opts = append(opts, grpc.ChainStreamInterceptor(
		grpcutil.StreamServerTimeHandler(listener.bucketRequestTimesGRPC),
		grpcutil.StreamServerStatusMetricHandler(statusCodes, listener.prometheus.request),
		listener.StreamServerRatelimitHandler()), grpc.ChainUnaryInterceptor(
		grpcutil.UnaryServerTimeHandler(listener.bucketRequestTimesGRPC),
		grpcutil.UnaryServerStatusMetricHandler(statusCodes, listener.prometheus.request),
		listener.UnaryServerRatelimitHandler()))
	grpcServer := grpc.NewServer(opts...) //skipcq: GO-S0902
	grpcv2.RegisterCarbonV2Server(grpcServer, listener)
	go grpcServer.Serve(listener.grpcListener)
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

func getWithCache(logger *zap.Logger, cache queryCache, key string, size uint64, expire int32, f func() (interface{}, error)) (result interface{}, fromCache bool, err error) {
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
