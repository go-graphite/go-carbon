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
	"fmt"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/NYTimes/gziphandler"
	"github.com/dgryski/go-expirecache"
	"github.com/dgryski/go-trigram"
	"github.com/dgryski/httputil"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/helper/stat"
	"github.com/lomik/go-carbon/points"
	"github.com/lomik/zapwriter"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
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

var TraceHeaders = map[string]string{
	"X-CTX-CarbonAPI-UUID":    "carbonapi_uuid",
	"X-CTX-CarbonZipper-UUID": "carbonzipper_uuid",
	"X-Request-ID":            "request_id",
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

func TraceContextToZap(ctx context.Context, logger *zap.Logger) *zap.Logger {
	for header, field := range TraceHeaders {
		v := ctx.Value(header)
		if v == nil {
			continue
		}

		if value, ok := v.(string); ok {
			logger = logger.With(zap.String(field, value))
		}
	}

	return logger
}

func TraceHandler(h http.HandlerFunc, globalStatusCodes []uint64, handlerStatusCodes []uint64, promRequest func(string, int)) http.HandlerFunc {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		for header := range TraceHeaders {
			v := req.Header.Get(header)
			if v != "" {
				ctx = context.WithValue(ctx, header, v)
			}
		}

		lrw := newResponseWriterWithStatus(rw)

		h.ServeHTTP(lrw, req.WithContext(ctx))

		if lrw.statusCodeMajor() < len(globalStatusCodes) {
			atomic.AddUint64(&globalStatusCodes[lrw.statusCodeMajor()], 1)
			atomic.AddUint64(&handlerStatusCodes[lrw.statusCodeMajor()], 1)
		}

		endpoint := path.Clean(req.URL.Path)
		promRequest(endpoint, lrw.statusCode)
	})
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

	select {
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
	helper.Stoppable
	cacheGet          func(key string) []points.Point
	readTimeout       time.Duration
	idleTimeout       time.Duration
	writeTimeout      time.Duration
	whisperData       string
	buckets           int
	maxGlobs          int
	failOnMaxGlobs    bool
	percentiles       []int
	scanFrequency     time.Duration
	forceScanChan     chan struct{}
	metricsAsCounters bool
	tcpListener       *net.TCPListener
	logger            *zap.Logger
	accessLogger      *zap.Logger
	internalStatsDir  string
	flock             bool

	queryCacheEnabled bool
	queryCacheSizeMB  int
	queryCache        queryCache
	findCacheEnabled  bool
	findCache         queryCache
	trigramIndex      bool

	fileIdx      atomic.Value
	fileIdxMutex sync.Mutex

	metrics       *metricStruct
	requestsTimes requestsTimes
	exitChan      chan struct{}
	timeBuckets   []uint64

	prometheus prometheus

	db *leveldb.DB
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
	idx     trigram.Index
	files   []string
	details map[string]*protov3.MetricDetails

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
			diskWaitDuration: func(time.Duration) {},
			returnedMetric:   func() {},
			returnedPoint:    func(int) {},
		},
	}
}

func (listener *CarbonserverListener) SetWhisperData(whisperData string) {
	listener.whisperData = strings.TrimRight(whisperData, "/")
}
func (listener *CarbonserverListener) SetMaxGlobs(maxGlobs int) {
	listener.maxGlobs = maxGlobs
}
func (listener *CarbonserverListener) SetFailOnMaxGlobs(failOnMaxGlobs bool) {
	listener.failOnMaxGlobs = failOnMaxGlobs
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
func (listener *CarbonserverListener) SetReadTimeout(readTimeout time.Duration) {
	listener.readTimeout = readTimeout
}
func (listener *CarbonserverListener) SetIdleTimeout(idleTimeout time.Duration) {
	listener.idleTimeout = idleTimeout
}
func (listener *CarbonserverListener) SetWriteTimeout(writeTimeout time.Duration) {
	listener.writeTimeout = writeTimeout
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
func (listener *CarbonserverListener) SetInternalStatsDir(dbPath string) {
	listener.internalStatsDir = dbPath
}
func (listener *CarbonserverListener) SetPercentiles(percentiles []int) {
	listener.percentiles = percentiles
}
func (listener *CarbonserverListener) CurrentFileIndex() *fileIndex {
	p := listener.fileIdx.Load()
	if p == nil {
		return nil
	}
	return p.(*fileIndex)
}

func (listener *CarbonserverListener) UpdateFileIndex(fidx *fileIndex) { listener.fileIdx.Store(fidx) }

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

func (listener *CarbonserverListener) fileListUpdater(dir string, tick <-chan time.Time, force <-chan struct{}, exit <-chan struct{}) {
	for {
		select {
		case <-exit:
			return
		case <-tick:
		case <-force:
		}
		listener.updateFileList(dir)
	}
}

func (listener *CarbonserverListener) updateFileList(dir string) {
	logger := listener.logger.With(zap.String("handler", "fileListUpdated"))
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic encountered",
				zap.Stack("stack"),
				zap.Any("error", r),
			)
		}
	}()
	t0 := time.Now()

	var files []string
	details := make(map[string]*protov3.MetricDetails)

	metricsKnown := uint64(0)
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Info("error processing", zap.String("path", p), zap.Error(err))
			return nil
		}
		i := stat.GetStat(info)

		hasSuffix := strings.HasSuffix(info.Name(), ".wsp")
		if info.IsDir() || hasSuffix {
			trimmedName := strings.TrimPrefix(p, listener.whisperData)
			files = append(files, trimmedName)
			if hasSuffix {
				metricsKnown++
				trimmedName = strings.Replace(trimmedName[1:len(trimmedName)-4], "/", ".", -1)
				details[trimmedName] = &protov3.MetricDetails{
					Size_:    i.Size,
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

	var stat syscall.Statfs_t
	err = syscall.Statfs(dir, &stat)
	if err != nil {
		logger.Info("error getting FS Stats",
			zap.String("dir", dir),
			zap.Error(err),
		)
		return
	}

	var freeSpace uint64
	if stat.Bavail >= 0 {
		freeSpace = uint64(stat.Bavail) * uint64(stat.Bsize)
	}
	totalSpace := stat.Blocks * uint64(stat.Bsize)

	fileScanRuntime := time.Since(t0)
	atomic.StoreUint64(&listener.metrics.MetricsKnown, metricsKnown)
	atomic.AddUint64(&listener.metrics.FileScanTimeNS, uint64(fileScanRuntime.Nanoseconds()))

	t0 = time.Now()
	idx := trigram.NewIndex(files)

	indexingRuntime := time.Since(t0)
	atomic.AddUint64(&listener.metrics.IndexBuildTimeNS, uint64(indexingRuntime.Nanoseconds()))
	indexSize := len(idx)

	pruned := idx.Prune(0.95)

	tl := time.Now()
	fidx := listener.CurrentFileIndex()

	oldAccessTimes := make(map[string]int64)
	if fidx != nil {
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
		oldAccessTimes = fidx.accessTimes
		listener.fileIdxMutex.Unlock()
	}
	rdTimeUpdateRuntime := time.Since(tl)

	listener.UpdateFileIndex(&fileIndex{
		idx:         idx,
		files:       files,
		details:     details,
		freeSpace:   freeSpace,
		totalSpace:  totalSpace,
		accessTimes: oldAccessTimes,
	})

	logger.Info("file list updated",
		zap.Duration("file_scan_runtime", fileScanRuntime),
		zap.Duration("indexing_runtime", indexingRuntime),
		zap.Duration("rdtime_update_runtime", rdTimeUpdateRuntime),
		zap.Duration("total_runtime", time.Since(t0)),
		zap.Int("Files", len(files)),
		zap.Int("index_size", indexSize),
		zap.Int("pruned_trigrams", pruned),
	)
}

func (listener *CarbonserverListener) expandGlobs(query string) ([]string, []bool, error) {
	var useGlob bool
	logger := zapwriter.Logger("carbonserver")

	// TODO: Find out why we have set 'useGlob' if 'star == -1'
	if star := strings.IndexByte(query, '*'); strings.IndexByte(query, '[') == -1 && strings.IndexByte(query, '?') == -1 && (star == -1 || star == len(query)-1) {
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

	query = strings.Replace(query, ".", "/", -1)

	var globs []string
	if !strings.HasSuffix(query, "*") {
		globs = append(globs, query+".wsp")
		logger.Debug("appending file to globs struct",
			zap.Strings("globs", globs),
		)
	}
	globs = append(globs, query)
	// TODO(dgryski): move this loop into its own function + add tests
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
							return nil, nil, errMaxGlobsExhausted
						}
						break
					}
					newglobs = append(newglobs, glob[:lbrace]+sub+glob[rbrace+1:])
				}
			} else {
				if len(newglobs) > listener.maxGlobs {
					if listener.failOnMaxGlobs {
						return nil, nil, errMaxGlobsExhausted
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

	var files []string

	fidx := listener.CurrentFileIndex()

	fallbackToFS := false
	if listener.trigramIndex == false || (fidx != nil && len(fidx.files) == 0) {
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
		if err != nil {
			continue
		}
		p = p[len(listener.whisperData+"/"):]
		if !s.IsDir() && strings.HasSuffix(p, ".wsp") {
			p = p[:len(p)-4]
			leafs[i] = true
		} else {
			leafs[i] = false
		}
		files[i] = strings.Replace(p, "/", ".", -1)
	}

	return files, leafs, nil
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
}

func (listener *CarbonserverListener) Stop() error {
	close(listener.forceScanChan)
	close(listener.exitChan)
	if listener.db != nil {
		listener.db.Close()
	}
	listener.tcpListener.Close()
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

func (listener *CarbonserverListener) Listen(listen string) error {
	logger := listener.logger

	logger.Info("starting carbonserver",
		zap.String("listen", listen),
		zap.String("whisperData", listener.whisperData),
		zap.Int("maxGlobs", listener.maxGlobs),
		zap.String("scanFrequency", listener.scanFrequency.String()),
	)

	listener.exitChan = make(chan struct{})
	if listener.trigramIndex && listener.scanFrequency != 0 {
		listener.forceScanChan = make(chan struct{})
		go listener.fileListUpdater(listener.whisperData, time.Tick(listener.scanFrequency), listener.forceScanChan, listener.exitChan)
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
					h,
					statusCodes["combined"],
					handlerStatusCodes,
					listener.prometheus.request,
				),
				listener.bucketRequestTimes,
			),
		)
	}
	carbonserverMux.HandleFunc("/_internal/capabilities/", wrapHandler(listener.capabilityHandler, statusCodes["capabilities"]))
	carbonserverMux.HandleFunc("/metrics/find/", wrapHandler(listener.findHandler, statusCodes["find"]))
	carbonserverMux.HandleFunc("/metrics/list/", wrapHandler(listener.listHandler, statusCodes["list"]))
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

func (listener *CarbonserverListener) renderTimeBuckets() interface{} {
	return listener.timeBuckets
}

func (listener *CarbonserverListener) bucketRequestTimes(req *http.Request, t time.Duration) {
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
		// Too big? Increment overflow bucket and log
		atomic.AddUint64(&listener.timeBuckets[listener.buckets], 1)
		listener.logger.Info("slow request",
			zap.String("url", req.URL.RequestURI()),
			zap.String("peer", req.RemoteAddr),
		)
	}
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
