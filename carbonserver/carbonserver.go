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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/NYTimes/gziphandler"
	"github.com/dgryski/go-expirecache"
	trigram "github.com/dgryski/go-trigram"
	"github.com/dgryski/httputil"
	"github.com/lomik/go-carbon/helper"
	pb "github.com/lomik/go-carbon/helper/carbonzipperpb"
	"github.com/lomik/go-carbon/helper/stat"
	"github.com/lomik/go-carbon/points"
	whisper "github.com/lomik/go-whisper"
	pickle "github.com/lomik/og-rek"
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

const (
	QueryIsPending uint64 = 1 << iota
	DataIsAvailable
)

type QueryItem struct {
	Data          atomic.Value
	Flags         uint64 // DataIsAvailable or QueryIsPending
	QueryFinished chan struct{}
}

type fetchResponse struct {
	data           []byte
	contentType    string
	metricsFetched int
	valuesFetched  int
	memoryUsed     int
	metrics        []string
}

var TraceHeaders = map[string]string{
	"X-CTX-CarbonAPI-UUID":    "carbonapi_uuid",
	"X-CTX-CarbonZipper-UUID": "carbonzipper_uuid",
	"X-Request-ID":            "request_id",
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

func TraceHandler(h http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		for header := range TraceHeaders {
			v := req.Header.Get(header)
			if v != "" {
				ctx = context.WithValue(ctx, header, v)
			}
		}

		h.ServeHTTP(rw, req.WithContext(ctx))
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
	scanFrequency     time.Duration
	metricsAsCounters bool
	tcpListener       *net.TCPListener
	logger            *zap.Logger
	accessLogger      *zap.Logger
	graphiteweb10     bool
	internalStatsDir  string

	queryCacheEnabled bool
	queryCacheSizeMB  int
	queryCache        queryCache
	findCacheEnabled  bool
	findCache         queryCache
	trigramIndex      bool

	fileIdx atomic.Value

	metrics     metricStruct
	exitChan    chan struct{}
	timeBuckets []uint64

	db *leveldb.DB
}

type metricDetailsFlat struct {
	*pb.MetricDetails
	Name string
}

type jsonMetricDetailsResponse struct {
	Metrics    []metricDetailsFlat
	FreeSpace  uint64
	TotalSpace uint64
}

type fileIndex struct {
	sync.Mutex

	idx     trigram.Index
	files   []string
	details map[string]*pb.MetricDetails

	accessTimes map[string]int64
	freeSpace   uint64
	totalSpace  uint64
}

func NewCarbonserverListener(cacheGetFunc func(key string) []points.Point) *CarbonserverListener {
	return &CarbonserverListener{
		// Config variables
		metricsAsCounters: false,
		cacheGet:          cacheGetFunc,
		logger:            zapwriter.Logger("carbonserver"),
		accessLogger:      zapwriter.Logger("access"),
		findCache:         queryCache{ec: expirecache.New(0)},
		trigramIndex:      true,
		graphiteweb10:     false,
	}
}

func (listener *CarbonserverListener) SetWhisperData(whisperData string) {
	listener.whisperData = strings.TrimRight(whisperData, "/")
}
func (listener *CarbonserverListener) SetMaxGlobs(maxGlobs int) {
	listener.maxGlobs = maxGlobs
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
func (listener *CarbonserverListener) SetGraphiteWeb10(enabled bool) {
	listener.graphiteweb10 = enabled
}

func (listener *CarbonserverListener) SetTrigramIndex(enabled bool) {
	listener.trigramIndex = enabled
}

func (listener *CarbonserverListener) SetInternalStatsDir(dbPath string) {
	listener.internalStatsDir = dbPath
}

func (listener *CarbonserverListener) CurrentFileIndex() *fileIndex {
	p := listener.fileIdx.Load()
	if p == nil {
		return nil
	}
	return p.(*fileIndex)
}

func (listener *CarbonserverListener) UpdateFileIndex(fidx *fileIndex) { listener.fileIdx.Store(fidx) }

func (listener *CarbonserverListener) UpdateMetricsAccessTimes(metrics map[string]int64) {
	p := listener.fileIdx.Load()
	if p == nil {
		return
	}
	idx := p.(*fileIndex)
	idx.Lock()
	defer idx.Unlock()

	for m, t := range metrics {
		if _, ok := idx.details[m]; ok {
			idx.details[m].RdTime = t
		} else {
			idx.details[m] = &pb.MetricDetails{RdTime: t}
		}
		idx.accessTimes[m] = t
	}
}

func (listener *CarbonserverListener) UpdateMetricsAccessTimesByString(metrics []string) {
	p := listener.fileIdx.Load()
	if p == nil {
		return
	}
	idx := p.(*fileIndex)
	idx.Lock()
	defer idx.Unlock()

	batch := new(leveldb.Batch)
	now := time.Now().Unix()
	for _, m := range metrics {
		if _, ok := idx.details[m]; ok {
			idx.details[m].RdTime = now
		} else {
			idx.details[m] = &pb.MetricDetails{RdTime: now}
		}
		idx.accessTimes[m] = now
		buf := make([]byte, 10)
		binary.PutVarint(buf, now)
		batch.Put([]byte(m), buf)
	}
	if listener.db != nil {
		err := listener.db.Write(batch, nil)
		if err != nil {
			listener.logger.Info("Error updating database",
				zap.Error(err),
			)
		}
	}
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
	details := make(map[string]*pb.MetricDetails)

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
				details[trimmedName] = &pb.MetricDetails{
					Size_:   i.RealSize,
					ModTime: i.MTime,
					ATime:   i.ATime,
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

	freeSpace := stat.Bavail * uint64(stat.Bsize)
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
		fidx.Lock()
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
		fidx.Unlock()
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
		zap.Int("files", len(files)),
		zap.Int("index_size", indexSize),
		zap.Int("pruned_trigrams", pruned),
	)
}

func (listener *CarbonserverListener) expandGlobs(query string) ([]string, []bool) {
	var useGlob bool
	logger := zapwriter.Logger("carbonserver")

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
		logger.Debug("appending to globs",
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
						break
					}
					newglobs = append(newglobs, glob[:lbrace]+sub+glob[rbrace+1:])
				}
			} else {
				if len(newglobs) > listener.maxGlobs {
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

	return files, leafs
}

var errMetricsListEmpty = fmt.Errorf("File index is empty or disabled")

func (listener *CarbonserverListener) getMetricsList() ([]string, error) {
	fidx := listener.CurrentFileIndex()
	var metrics []string

	if fidx == nil {
		return nil, errMetricsListEmpty
	}

	for _, p := range fidx.files {
		if !strings.HasSuffix(p, ".wsp") {
			continue
		}
		p = p[1 : len(p)-4]
		metrics = append(metrics, strings.Replace(p, "/", ".", -1))
	}
	return metrics, nil
}

func (listener *CarbonserverListener) detailsHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/details/?format=json
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.DetailsRequests, 1)

	req.ParseForm()
	format := req.FormValue("format")

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "details"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("format", format),
	))

	if format != "json" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.DetailsErrors, 1)
		accessLogger.Info("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	var err error

	fidx := listener.CurrentFileIndex()
	if fidx == nil {
		atomic.AddUint64(&listener.metrics.DetailsErrors, 1)
		accessLogger.Info("details failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "can't fetch metrics list"),
			zap.Error(errMetricsListEmpty),
		)
		http.Error(wr, fmt.Sprintf("Can't fetch metrics details: %s", err), http.StatusInternalServerError)
		return
	}

	var b []byte

	switch format {
	case "json":
		response := jsonMetricDetailsResponse{
			FreeSpace:  fidx.freeSpace,
			TotalSpace: fidx.totalSpace,
		}
		for m, v := range fidx.details {
			response.Metrics = append(response.Metrics, metricDetailsFlat{
				Name:          m,
				MetricDetails: v,
			})
		}
		b, err = json.Marshal(response)
	case "protobuf", "protobuf3":
		response := &pb.MetricDetailsResponse{
			Metrics:    fidx.details,
			FreeSpace:  fidx.freeSpace,
			TotalSpace: fidx.totalSpace,
		}
		b, err = response.Marshal()
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		accessLogger.Info("details failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "response encode failed"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("An internal error has occured: %s", err), http.StatusInternalServerError)
		return
	}
	wr.Write(b)

	accessLogger.Info("details served",
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
	return

}

func (listener *CarbonserverListener) listHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/list/?format=json
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.ListRequests, 1)

	req.ParseForm()
	format := req.FormValue("format")

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "list"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("format", format),
	))

	if format != "json" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		accessLogger.Info("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	var err error

	metrics, err := listener.getMetricsList()
	if err != nil {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		accessLogger.Info("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "can't fetch metrics list"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Can't fetch metrics list: %s", err), http.StatusInternalServerError)
		return
	}

	var b []byte
	response := &pb.ListMetricsResponse{Metrics: metrics}
	switch format {
	case "json":
		b, err = json.Marshal(response)
	case "protobuf":
		b, err = response.Marshal()
	case "protobuf3":
		b, err = response.Marshal()
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		accessLogger.Info("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "response encode failed"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("An internal error has occured: %s", err), http.StatusInternalServerError)
		return
	}
	wr.Write(b)

	accessLogger.Info("list served",
		zap.Duration("runtime_seconds", time.Since(t0)),
	)
	return

}

type findResponse struct {
	data        []byte
	contentType string
	files       int
}

func (listener *CarbonserverListener) findHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/find/?local=1&format=pickle&query=the.metric.path.with.glob

	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.FindRequests, 1)

	req.ParseForm()
	format := req.FormValue("format")
	query := req.FormValue("query")

	var response *findResponse

	logger := TraceContextToZap(ctx, listener.logger.With(
		zap.String("handler", "find"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("query", query),
		zap.String("format", format),
	))

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "find"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("query", query),
		zap.String("format", format),
	))

	if format != "json" && format != "pickle" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.FindErrors, 1)
		accessLogger.Info("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	if query == "" {
		atomic.AddUint64(&listener.metrics.FindErrors, 1)
		accessLogger.Info("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "empty query"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (no query)", http.StatusBadRequest)
		return
	}

	var err error
	fromCache := false
	if listener.findCacheEnabled {
		key := query + "&" + format
		size := uint64(100 * 1024 * 1024)
		item := listener.findCache.getQueryItem(key, size, 300)
		res, ok := item.FetchOrLock()
		if !ok {
			logger.Debug("find cache miss")
			atomic.AddUint64(&listener.metrics.FindCacheMiss, 1)
			response, err = listener.findMetrics(logger, t0, format, query)
			if err != nil {
				item.StoreAbort()
			} else {
				item.StoreAndUnlock(response)
			}
		} else if res != nil {
			logger.Debug("query cache hit")
			atomic.AddUint64(&listener.metrics.FindCacheHit, 1)
			response = res.(*findResponse)
			fromCache = true
		}
	} else {
		response, err = listener.findMetrics(logger, t0, format, query)
	}

	if response == nil {
		accessLogger.Info("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "internal error while processing request"),
			zap.Error(err),
			zap.Int("http_code", http.StatusInternalServerError),
		)
		http.Error(wr, fmt.Sprintf("Internal error while processing request (%v)", err),
			http.StatusInternalServerError)
		return
	}

	wr.Header().Set("Content-Type", response.contentType)
	wr.Write(response.data)

	if response.files == 0 {
		// to get an idea how often we search for nothing
		atomic.AddUint64(&listener.metrics.FindZero, 1)
	}

	accessLogger.Info("find success",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("files", response.files),
		zap.Bool("find_cache_enabled", listener.findCacheEnabled),
		zap.Bool("from_cache", fromCache),
		zap.Int("http_code", http.StatusOK),
	)
	return
}

var findMetricsNoMetricsError = fmt.Errorf("no metrics available for this query")

func (listener *CarbonserverListener) findMetrics(logger *zap.Logger, t0 time.Time, format, name string) (*findResponse, error) {
	var result findResponse
	files, leafs := listener.expandGlobs(name)

	metricsCount := uint64(0)
	for i := range files {
		if leafs[i] {
			metricsCount++
		}
	}
	result.files = len(files)
	atomic.AddUint64(&listener.metrics.MetricsFound, metricsCount)
	logger.Debug("expandGlobs result",
		zap.String("action", "expandGlobs"),
		zap.String("metric", name),
		zap.Uint64("metrics_count", metricsCount),
	)

	if format == "json" || format == "protobuf" || format == "protobuf3" {
		var err error
		response := pb.GlobResponse{
			Name:    name,
			Matches: make([]*pb.GlobMatch, 0),
		}

		for i, p := range files {
			response.Matches = append(response.Matches, &pb.GlobMatch{Path: p, IsLeaf: leafs[i]})
		}

		switch format {
		case "json":
			result.contentType = "application/json"
			result.data, err = json.Marshal(response)
		case "protobuf3", "protobuf":
			result.contentType = "application/protobuf"
			result.data, err = response.Marshal()
		}

		if err != nil {
			atomic.AddUint64(&listener.metrics.FindErrors, 1)

			logger.Info("find failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "response encode failed"),
				zap.Error(err),
			)
			return nil, err
		} else {
			if len(response.Matches) == 0 {
				err = findMetricsNoMetricsError
			}
		}
		return &result, err
	} else if format == "pickle" {
		// [{'metric_path': 'metric', 'intervals': [(x,y)], 'isLeaf': True},]
		var metrics []map[string]interface{}
		var m map[string]interface{}

		intervals := &IntervalSet{Start: 0, End: int32(time.Now().Unix()) + 60}

		for i, p := range files {
			m = make(map[string]interface{})
			if listener.graphiteweb10 {
				// graphite master
				m["path"] = p
				m["is_leaf"] = leafs[i]
				m["intervals"] = intervals
			} else {
				// graphite 0.9.x
				m["metric_path"] = p
				m["isLeaf"] = leafs[i]
			}

			metrics = append(metrics, m)
		}
		var buf bytes.Buffer
		pEnc := pickle.NewEncoder(&buf)
		pEnc.Encode(metrics)
		return &findResponse{buf.Bytes(), "application/pickle", len(files)}, nil
	}
	return nil, nil
}

func (listener *CarbonserverListener) renderHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /render/?target=the.metric.name&format=pickle&from=1396008021&until=1396022421
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.RenderRequests, 1)

	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")
	from := req.FormValue("from")
	until := req.FormValue("until")

	logger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "render"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("metric", metric),
		zap.String("from", from),
		zap.String("until", until),
		zap.String("format", format),
	))

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "render"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("metric", metric),
		zap.String("from", from),
		zap.String("until", until),
		zap.String("format", format),
	))

	// Make sure we log which metric caused a panic()
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic recovered",
				zap.Stack("stack"),
				zap.Any("error", r),
			)
			accessLogger.Error("fetch failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "panic during serving the request"),
				zap.Stack("stack"),
				zap.Any("error", r),
				zap.Int("http_code", http.StatusInternalServerError),
			)
			http.Error(wr, fmt.Sprintf("Panic occured, see logs for more information"),
				http.StatusInternalServerError)
		}
	}()

	if format != "json" && format != "pickle" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Info("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	response, fromCache, err := listener.fetchWithCache(logger, format, metric, from, until)

	wr.Header().Set("Content-Type", response.contentType)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Info("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "failed to read data"),
			zap.Int("http_code", http.StatusBadRequest),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Bad request (%s)", err),
			http.StatusBadRequest)
		return
	}

	listener.UpdateMetricsAccessTimesByString(response.metrics)

	wr.Write(response.data)

	atomic.AddUint64(&listener.metrics.FetchSize, uint64(response.memoryUsed))
	logger.Info("fetch served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Bool("query_cache_enabled", listener.queryCacheEnabled),
		zap.Bool("from_cache", fromCache),
		zap.Int("metrics_fetched", response.metricsFetched),
		zap.Int("values_fetched", response.valuesFetched),
		zap.Int("memory_used_bytes", response.memoryUsed),
		zap.Int("http_code", http.StatusOK),
	)

}

func (listener *CarbonserverListener) fetchWithCache(logger *zap.Logger, format, metric, from, until string) (fetchResponse, bool, error) {
	logger = logger.With(
		zap.String("function", "fetchWithCache"),
	)
	fromCache := false
	var badTime bool

	i, err := strconv.Atoi(from)
	if err != nil {
		logger.Debug("invalid fromTime", zap.Error(err))
		badTime = true
	}
	fromTime := int32(i)
	i, err = strconv.Atoi(until)
	if err != nil {
		logger.Debug("invalid untilTime", zap.Error(err))
		badTime = true
	}
	untilTime := int32(i)

	if badTime {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		err = errors.New("Bad request (invalid from/until time)")
		return fetchResponse{nil, "application/text", 0, 0, 0, nil}, fromCache, err
	}

	var response fetchResponse
	if listener.queryCacheEnabled {
		key := metric + "&" + format + "&" + from + "&" + until
		size := uint64(100 * 1024 * 1024)
		renderRequests := atomic.LoadUint64(&listener.metrics.RenderRequests)
		fetchSize := atomic.LoadUint64(&listener.metrics.FetchSize)
		if renderRequests > 0 {
			size = fetchSize / renderRequests
		}
		item := listener.queryCache.getQueryItem(key, size, 60)
		res, ok := item.FetchOrLock()
		if !ok {
			logger.Debug("query cache miss")
			atomic.AddUint64(&listener.metrics.QueryCacheMiss, 1)
			response, err = listener.prepareData(format, metric, fromTime, untilTime)
			if err != nil {
				item.StoreAbort()
			} else {
				item.StoreAndUnlock(response)
			}
		} else {
			logger.Debug("query cache hit")
			atomic.AddUint64(&listener.metrics.QueryCacheHit, 1)
			response = res.(fetchResponse)
			fromCache = true
		}
	} else {
		response, err = listener.prepareData(format, metric, fromTime, untilTime)
	}
	return response, fromCache, err
}

func (listener *CarbonserverListener) prepareData(format, metric string, fromTime, untilTime int32) (fetchResponse, error) {
	contentType := "application/text"
	var b []byte
	var err error
	var metricsFetched int
	var memoryUsed int
	var valuesFetched int

	listener.logger.Debug("fetching data...")
	files, leafs := listener.expandGlobs(metric)

	metricsCount := 0
	for i := range files {
		if leafs[i] {
			metricsCount++
		}
	}
	listener.logger.Debug("expandGlobs result",
		zap.String("handler", "render"),
		zap.String("action", "expandGlobs"),
		zap.String("metric", metric),
		zap.Int("metrics_count", metricsCount),
		zap.Int32("from", fromTime),
		zap.Int32("until", untilTime),
	)

	multi, err := listener.fetchDataPB(metric, files, leafs, fromTime, untilTime)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		return fetchResponse{nil, contentType, 0, 0, 0, nil}, err

	}

	var metrics []string
	metricsFetched = len(multi.Metrics)
	for i := range multi.Metrics {
		metrics = append(metrics, multi.Metrics[i].Name)
		memoryUsed += multi.Metrics[i].Size()
		valuesFetched += len(multi.Metrics[i].Values)
	}

	switch format {
	case "json":
		contentType = "application/json"
		b, err = json.Marshal(multi)
	case "protobuf3", "protobuf":
		contentType = "application/protobuf"
		b, err = multi.Marshal()
	case "pickle":
		// transform protobuf data into what pickle expects
		//[{'start': 1396271100, 'step': 60, 'name': 'metric',
		//'values': [9.0, 19.0, None], 'end': 1396273140}

		var response []map[string]interface{}

		for _, metric := range multi.GetMetrics() {

			var m map[string]interface{}

			m = make(map[string]interface{})
			m["start"] = metric.StartTime
			m["step"] = metric.StepTime
			m["end"] = metric.StopTime
			m["name"] = metric.Name

			mv := make([]interface{}, len(metric.Values))
			for i, p := range metric.Values {
				if metric.IsAbsent[i] {
					mv[i] = nil
				} else {
					mv[i] = p
				}
			}

			m["values"] = mv
			response = append(response, m)
		}

		contentType = "application/pickle"
		var buf bytes.Buffer
		pEnc := pickle.NewEncoder(&buf)
		err = pEnc.Encode(response)
		b = buf.Bytes()
	}

	if err != nil {
		return fetchResponse{nil, contentType, 0, 0, 0, nil}, err
	}
	return fetchResponse{b, contentType, metricsFetched, valuesFetched, memoryUsed, metrics}, nil
}

func (listener *CarbonserverListener) fetchSingleMetric(metric string, fromTime, untilTime int32) (*pb.FetchResponse, error) {
	var step int32

	// We need to obtain the metadata from whisper file anyway.
	path := listener.whisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.Open(path)
	if err != nil {
		// the FE/carbonzipper often requests metrics we don't have
		// We shouldn't really see this any more -- expandGlobs() should filter them out
		atomic.AddUint64(&listener.metrics.NotFound, 1)
		listener.logger.Info("open error", zap.String("path", path), zap.Error(err))
		return nil, errors.New("Can't open metric")
	}

	logger := listener.logger.With(
		zap.String("path", path),
		zap.Int("fromTime", int(fromTime)),
		zap.Int("untilTime", int(untilTime)),
	)

	retentions := w.Retentions()
	now := int32(time.Now().Unix())
	diff := now - fromTime
	bestStep := int32(retentions[0].SecondsPerPoint())
	for _, retention := range retentions {
		if int32(retention.MaxRetention()) >= diff {
			step = int32(retention.SecondsPerPoint())
			break
		}
	}

	if step == 0 {
		maxRetention := int32(retentions[len(retentions)-1].MaxRetention())
		if now-maxRetention > untilTime {
			atomic.AddUint64(&listener.metrics.RenderErrors, 1)
			logger.Info("can't find proper archive for the request")
			return nil, errors.New("Can't find proper archive")
		}
		logger.Debug("can't find archive that contains full set of data, using the least precise one")
		step = maxRetention
	}

	var cacheData []points.Point
	if step != bestStep {
		logger.Debug("cache is not supported for this query (required step != best step)",
			zap.Int("step", int(step)),
			zap.Int("bestStep", int(bestStep)),
		)
	} else {
		// query cache
		cacheStartTime := time.Now()
		cacheData = listener.cacheGet(metric)
		waitTime := uint64(time.Since(cacheStartTime).Nanoseconds())
		atomic.AddUint64(&listener.metrics.CacheWaitTimeFetchNS, waitTime)
	}

	logger.Debug("fetching disk metric")
	atomic.AddUint64(&listener.metrics.DiskRequests, 1)
	diskStartTime := time.Now()
	points, err := w.Fetch(int(fromTime), int(untilTime))
	w.Close()
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		logger.Info("failed to fetch points", zap.Error(err))
		return nil, errors.New("failed to fetch points")
	}

	// Should never happen, because we have a check for proper archive now
	if points == nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		logger.Info("metric time range not found")
		return nil, errors.New("time range not found")
	}
	atomic.AddUint64(&listener.metrics.MetricsReturned, 1)
	values := points.Values()

	fromTime = int32(points.FromTime())
	untilTime = int32(points.UntilTime())
	step = int32(points.Step())

	waitTime := uint64(time.Since(diskStartTime).Nanoseconds())
	atomic.AddUint64(&listener.metrics.DiskWaitTimeNS, waitTime)
	atomic.AddUint64(&listener.metrics.PointsReturned, uint64(len(values)))

	response := pb.FetchResponse{
		Name:      metric,
		StartTime: fromTime,
		StopTime:  untilTime,
		StepTime:  step,
		Values:    make([]float64, len(values)),
		IsAbsent:  make([]bool, len(values)),
	}

	for i, p := range values {
		if math.IsNaN(p) {
			response.Values[i] = 0
			response.IsAbsent[i] = true
		} else {
			response.Values[i] = p
			response.IsAbsent[i] = false
		}
	}

	if cacheData != nil {
		atomic.AddUint64(&listener.metrics.CacheRequestsTotal, 1)
		cacheStartTime := time.Now()
		pointsFetchedFromCache := 0
		for _, item := range cacheData {
			ts := int32(item.Timestamp) - int32(item.Timestamp)%step
			if ts < fromTime || ts >= untilTime {
				continue
			}
			pointsFetchedFromCache++
			index := (ts - fromTime) / step
			response.Values[index] = item.Value
			response.IsAbsent[index] = false
		}
		waitTime := uint64(time.Since(cacheStartTime).Nanoseconds())
		atomic.AddUint64(&listener.metrics.CacheWorkTimeNS, waitTime)
		if pointsFetchedFromCache > 0 {
			atomic.AddUint64(&listener.metrics.CacheHit, 1)
		} else {
			atomic.AddUint64(&listener.metrics.CacheMiss, 1)
		}
	}
	return &response, nil
}

func (listener *CarbonserverListener) fetchDataPB(metric string, files []string, leafs []bool, fromTime, untilTime int32) (*pb.MultiFetchResponse, error) {
	var multi pb.MultiFetchResponse
	for i, metric := range files {
		if !leafs[i] {
			listener.logger.Debug("skipping directory", zap.String("metric", metric))
			// can't fetch a directory
			continue
		}
		response, err := listener.fetchSingleMetric(metric, fromTime, untilTime)
		if err == nil {
			multi.Metrics = append(multi.Metrics, response)
		}
	}
	return &multi, nil
}

func (listener *CarbonserverListener) infoHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /info/?target=the.metric.name&format=json
	t0 := time.Now()
	ctx := req.Context()

	atomic.AddUint64(&listener.metrics.InfoRequests, 1)
	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")

	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
		zap.String("handler", "info"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("target", metric),
		zap.String("format", format),
	))

	if format == "" {
		format = "json"
	}

	if format != "json" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.InfoErrors, 1)
		accessLogger.Error("info failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
			zap.Int("http_code", http.StatusBadRequest),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	path := listener.whisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.Open(path)

	if err != nil {
		atomic.AddUint64(&listener.metrics.NotFound, 1)
		accessLogger.Info("info served",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "metric not found"),
			zap.Int("http_code", http.StatusNotFound),
		)
		http.Error(wr, "Metric not found", http.StatusNotFound)
		return
	}

	defer w.Close()

	aggr := w.AggregationMethod()
	maxr := int32(w.MaxRetention())
	xfiles := float32(w.XFilesFactor())

	var b []byte
	rets := make([]*pb.Retention, 0, 4)
	for _, retention := range w.Retentions() {
		spp := int32(retention.SecondsPerPoint())
		nop := int32(retention.NumberOfPoints())
		rets = append(rets, &pb.Retention{
			SecondsPerPoint: spp,
			NumberOfPoints:  nop,
		})
	}

	response := pb.InfoResponse{
		Name:              metric,
		AggregationMethod: aggr,
		MaxRetention:      maxr,
		XFilesFactor:      xfiles,
		Retentions:        rets,
	}

	switch format {
	case "json":
		b, err = json.Marshal(response)
	case "protobuf3", "protobuf":
		b, err = response.Marshal()
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		accessLogger.Info("info failed",
			zap.String("reason", "response encode failed"),
			zap.Int("http_code", http.StatusInternalServerError),
			zap.Error(err),
		)
		http.Error(wr, "Failed to encode response: "+err.Error(),
			http.StatusInternalServerError)
		return
	}
	wr.Write(b)

	accessLogger.Info("info served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("http_code", http.StatusOK),
	)
	return
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
	for i := 0; i <= listener.buckets; i++ {
		sender(fmt.Sprintf("requests_in_%dms_to_%dms", i*100, (i+1)*100), &listener.timeBuckets[i], send)
	}
}

func (listener *CarbonserverListener) Stop() error {
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
		zap.String("whisperData", listener.whisperData),
		zap.Int("maxGlobs", listener.maxGlobs),
		zap.String("scanFrequency", listener.scanFrequency.String()),
	)

	listener.exitChan = make(chan struct{})
	if listener.trigramIndex && listener.scanFrequency != 0 {
		force := make(chan struct{})
		go listener.fileListUpdater(listener.whisperData, time.Tick(listener.scanFrequency), force, listener.exitChan)
		force <- struct{}{}
	}

	listener.queryCache = queryCache{ec: expirecache.New(uint64(listener.queryCacheSizeMB))}

	// +1 to track every over the number of buckets we track
	listener.timeBuckets = make([]uint64, listener.buckets+1)

	carbonserverMux := http.NewServeMux()

	wrapHandler := func(h http.HandlerFunc) http.HandlerFunc {
		return httputil.TrackConnections(
			httputil.TimeHandler(
				TraceHandler(
					h,
				),
				listener.bucketRequestTimes,
			),
		)
	}
	carbonserverMux.HandleFunc("/metrics/find/", wrapHandler(listener.findHandler))
	carbonserverMux.HandleFunc("/metrics/list/", wrapHandler(listener.listHandler))
	carbonserverMux.HandleFunc("/metrics/details/", wrapHandler(listener.detailsHandler))
	carbonserverMux.HandleFunc("/render/", wrapHandler(listener.renderHandler))
	carbonserverMux.HandleFunc("/info/", wrapHandler(listener.infoHandler))

	carbonserverMux.HandleFunc("/robots.txt", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "User-agent: *\nDisallow: /")
	})

	logger.Info(fmt.Sprintf("listening on %s", listen))
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
			listener.UpdateMetricsAccessTimes(accessTimes)
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

	ms := t.Nanoseconds() / int64(time.Millisecond)

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
