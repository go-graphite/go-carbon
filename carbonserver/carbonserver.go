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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/NYTimes/gziphandler"
	"github.com/dgryski/go-expirecache"
	trigram "github.com/dgryski/go-trigram"
	"github.com/dgryski/httputil"
	"github.com/gogo/protobuf/proto"
	pb2 "github.com/lomik/go-carbon/carbonzipperpb"
	pb3 "github.com/lomik/go-carbon/carbonzipperpb3"
	"github.com/lomik/go-carbon/helper"
	"github.com/lomik/go-carbon/points"
	whisper "github.com/lomik/go-whisper"
	pickle "github.com/lomik/og-rek"
	"github.com/lomik/zapwriter"
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
	case <-q.QueryFinished:
		break
	}

	return q.Data.Load(), true
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

	queryCacheEnabled bool
	queryCacheSizeMB  int
	queryCache        queryCache
	findCacheEnabled  bool
	findCache         queryCache

	fileIdx atomic.Value

	metrics     metricStruct
	exitChan    chan struct{}
	timeBuckets []uint64
}

type fileIndex struct {
	idx   trigram.Index
	files []string
}

func NewCarbonserverListener(cacheGetFunc func(key string) []points.Point) *CarbonserverListener {
	return &CarbonserverListener{
		// Config variables
		metricsAsCounters: false,
		cacheGet:          cacheGetFunc,
		logger:            zapwriter.Logger("carbonserver"),
		findCache:         queryCache{ec: expirecache.New(0)},
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

func (listener *CarbonserverListener) CurrentFileIndex() *fileIndex {
	p := listener.fileIdx.Load()
	if p == nil {
		return nil
	}
	return p.(*fileIndex)
}
func (listener *CarbonserverListener) UpdateFileIndex(fidx *fileIndex) { listener.fileIdx.Store(fidx) }

func (listener *CarbonserverListener) fileListUpdater(dir string, tick <-chan time.Time, force <-chan struct{}, exit <-chan struct{}) {
	logger := listener.logger
	for {

		select {
		case <-exit:
			return
		case <-tick:
		case <-force:
		}

		var files []string

		t0 := time.Now()

		metricsKnown := uint64(0)
		err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				logger.Info("error processing", zap.String("path", p), zap.Error(err))
				return nil
			}

			hasSuffix := strings.HasSuffix(info.Name(), ".wsp")
			if info.IsDir() || hasSuffix {
				files = append(files, strings.TrimPrefix(p, listener.whisperData))
				if hasSuffix {
					metricsKnown++
				}
			}

			return nil
		})

		fileScanRuntime := time.Since(t0)
		atomic.StoreUint64(&listener.metrics.MetricsKnown, metricsKnown)
		atomic.AddUint64(&listener.metrics.FileScanTimeNS, uint64(fileScanRuntime.Nanoseconds()))

		t0 = time.Now()
		idx := trigram.NewIndex(files)

		indexingRuntime := time.Since(t0)
		atomic.AddUint64(&listener.metrics.IndexBuildTimeNS, uint64(indexingRuntime.Nanoseconds()))
		indexSize := len(idx)

		pruned := idx.Prune(0.95)

		logger.Debug("file list updated",
			zap.String("fileScanRuntime", fileScanRuntime.String()),
			zap.Int("files", len(files)),
			zap.String("indexingRuntime", indexingRuntime.String()),
			zap.Int("indexSize", indexSize),
			zap.Int("prunedTrigrams", pruned),
		)

		if err == nil {
			listener.UpdateFileIndex(&fileIndex{idx, files})
		}
	}
}

func (listener *CarbonserverListener) expandGlobs(query string) ([]string, []bool) {
	var useGlob bool

	if star := strings.IndexByte(query, '*'); strings.IndexByte(query, '[') == -1 && strings.IndexByte(query, '?') == -1 && (star == -1 || star == len(query)-1) {
		useGlob = true
	}

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

	if useGlob || fidx == nil {
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

var metricsListEmptyError = fmt.Errorf("File index is empty or disabled")

func (listener *CarbonserverListener) getMetricsList() ([]string, error) {
	fidx := listener.CurrentFileIndex()
	var metrics []string

	if fidx == nil {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		return nil, metricsListEmptyError
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

func (listener *CarbonserverListener) listHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /metrics/list/?format=json
	t0 := time.Now()

	atomic.AddUint64(&listener.metrics.ListRequests, 1)

	req.ParseForm()
	format := req.FormValue("format")

	logger := listener.logger.With(
		zap.String("handler", "listHandler"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("format", format),
	)

	if format != "json" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		logger.Info("list failed",
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
		logger.Info("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "can't fetch metrics list"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Can't fetch metrics list: %s", err), http.StatusInternalServerError)
		return
	}

	var b []byte
	switch format {
	case "json":
		response := pb2.ListMetricsResponse{Metrics: metrics}
		b, err = json.Marshal(response)
	case "protobuf":
		response := &pb2.ListMetricsResponse{Metrics: metrics}
		b, err = proto.Marshal(response)
	case "protobuf3":
		response := &pb3.ListMetricsResponse{Metrics: metrics}
		b, err = response.Marshal()
	}

	if err != nil {
		atomic.AddUint64(&listener.metrics.ListErrors, 1)
		logger.Info("list failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "response encode failed"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("An internal error has occured: %s", err), http.StatusInternalServerError)
		return
	}
	wr.Write(b)

	logger.Info("list served",
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

	atomic.AddUint64(&listener.metrics.FindRequests, 1)

	req.ParseForm()
	format := req.FormValue("format")
	query := req.FormValue("query")

	var response *findResponse

	logger := listener.logger.With(
		zap.String("handler", "findhHandler"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("query", query),
		zap.String("format", format),
	)

	if format != "json" && format != "pickle" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.FindErrors, 1)
		logger.Info("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	if query == "" {
		atomic.AddUint64(&listener.metrics.FindErrors, 1)
		logger.Info("find failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "empty query"),
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
			item.StoreAndUnlock(response)
		} else {
			logger.Debug("query cache hit")
			atomic.AddUint64(&listener.metrics.FindCacheHit, 1)
			response = res.(*findResponse)
			fromCache = true
		}
	} else {
		response, err = listener.findMetrics(logger, t0, format, query)
	}
	if response == nil {
		http.Error(wr, fmt.Sprintf("Internal error while processing request (%v)", err),
			http.StatusBadRequest)
		return
	}

	wr.Header().Set("Content-Type", response.contentType)
	wr.Write(response.data)

	if response.files == 0 {
		// to get an idea how often we search for nothing
		atomic.AddUint64(&listener.metrics.FindZero, 1)
	}

	logger.Info("find success",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Int("files", response.files),
		zap.Bool("find_cache_enabled", listener.findCacheEnabled),
		zap.Bool("from_cache", fromCache),
	)
	return
}

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
		if format == "protobuf3" {
			response := pb3.GlobResponse{
				Name:    name,
				Matches: make([]*pb3.GlobMatch, 0),
			}

			for i, p := range files {
				response.Matches = append(response.Matches, &pb3.GlobMatch{Path: p, IsLeaf: leafs[i]})
			}

			result.contentType = "application/protobuf"
			result.data, err = response.Marshal()
		} else {
			response := pb2.GlobResponse{
				Name:    &name,
				Matches: make([]*pb2.GlobMatch, 0),
			}

			for i, p := range files {
				response.Matches = append(response.Matches, &pb2.GlobMatch{Path: proto.String(p), IsLeaf: proto.Bool(leafs[i])})
			}

			switch format {
			case "json":
				result.contentType = "application/json"
				result.data, err = json.Marshal(response)
			case "protobuf":
				result.contentType = "application/protobuf"
				result.data, err = proto.Marshal(&response)
			}
		}

		if err != nil {
			atomic.AddUint64(&listener.metrics.FindErrors, 1)

			logger.Info("find failed",
				zap.Duration("runtime_seconds", time.Since(t0)),
				zap.String("reason", "response encode failed"),
				zap.Error(err),
			)
			return nil, err
		}
		return &result, err
	} else if format == "pickle" {
		// [{'metric_path': 'metric', 'intervals': [(x,y)], 'isLeaf': True},]
		var metrics []map[string]interface{}
		var m map[string]interface{}

		intervals := &IntervalSet{Start: 0, End: int32(time.Now().Unix()) + 60}

		for i, p := range files {
			m = make(map[string]interface{})
			// graphite 0.9.x
			m["metric_path"] = p
			m["isLeaf"] = leafs[i]

			// graphite master
			m["path"] = p
			m["is_leaf"] = leafs[i]
			m["intervals"] = intervals
			metrics = append(metrics, m)
		}
		var buf bytes.Buffer
		pEnc := pickle.NewEncoder(&buf)
		pEnc.Encode(metrics)
		return &findResponse{buf.Bytes(), "application/pickle", len(files)}, nil
	}
	// This should not happen!
	return nil, nil
}

func (listener *CarbonserverListener) fetchHandler(wr http.ResponseWriter, req *http.Request) {
	// URL: /render/?target=the.metric.name&format=pickle&from=1396008021&until=1396022421
	t0 := time.Now()

	atomic.AddUint64(&listener.metrics.RenderRequests, 1)

	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")
	from := req.FormValue("from")
	until := req.FormValue("until")

	logger := listener.logger.With(
		zap.String("handler", "fetchHandler"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("metric", metric),
		zap.String("from", from),
		zap.String("until", until),
		zap.String("format", format),
	)

	// Make sure we log which metric caused a panic()
	defer func() {
		if r := recover(); r != nil {
			var buf [4096]byte
			runtime.Stack(buf[:], false)
			logger.Info("panic recovered",
				zap.String("error", fmt.Sprintf("%v", r)),
				zap.String("stack", string(buf[:])),
			)
		}
	}()

	if format != "json" && format != "pickle" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		logger.Info("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	response, fromCache, err := listener.fetchWithCache(logger, format, metric, from, until)

	wr.Header().Set("Content-Type", response.contentType)
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		logger.Info("fetch failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "failed to read data"),
			zap.Error(err),
		)
		http.Error(wr, fmt.Sprintf("Bad request (%s)", err),
			http.StatusBadRequest)
		return
	}

	wr.Write(response.data)

	atomic.AddUint64(&listener.metrics.FetchSize, uint64(response.memoryUsed))
	logger.Info("fetch served",
		zap.Duration("runtime_seconds", time.Since(t0)),
		zap.Bool("query_cache_enabled", listener.queryCacheEnabled),
		zap.Bool("from_cache", fromCache),
		zap.Int("metrics_fetched", response.metricsFetched),
		zap.Int("values_fetched", response.valuesFetched),
		zap.Int("memory_used_bytes", response.memoryUsed),
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
		return fetchResponse{nil, "application/text", 0, 0, 0}, fromCache, err
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
			item.StoreAndUnlock(response)
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
	metricsFetched := 0
	memoryUsed := 0
	valuesFetched := 0

	listener.logger.Debug("fetching data...")
	files, leafs := listener.expandGlobs(metric)

	metricsCount := 0
	for i := range files {
		if leafs[i] {
			metricsCount++
		}
	}
	listener.logger.Debug("expandGlobs result",
		zap.String("handler", "fetchHandler"),
		zap.String("action", "expandGlobs"),
		zap.String("metric", metric),
		zap.Int("metrics_count", metricsCount),
		zap.Int32("from", fromTime),
		zap.Int32("until", untilTime),
	)

	if format == "protobuf3" {
		multi, err := listener.fetchDataPB3(metric, files, leafs, fromTime, untilTime)
		if err != nil {
			atomic.AddUint64(&listener.metrics.RenderErrors, 1)
			return fetchResponse{nil, contentType, 0, 0, 0}, err

		}

		metricsFetched = len(multi.Metrics)
		for i := range multi.Metrics {
			memoryUsed += multi.Metrics[i].Size()
			valuesFetched += len(multi.Metrics[i].Values)
		}

		contentType = "application/protobuf"
		b, err = multi.Marshal()

	} else {
		multi, err := listener.fetchDataPB2(metric, files, leafs, fromTime, untilTime)
		if err != nil {
			atomic.AddUint64(&listener.metrics.RenderErrors, 1)
			return fetchResponse{nil, contentType, 0, 0, 0}, err
		}

		metricsFetched = len(multi.Metrics)
		for i := range multi.Metrics {
			memoryUsed += multi.Metrics[i].Size()
			valuesFetched += len(multi.Metrics[i].Values)
		}

		switch format {
		case "json":
			contentType = "application/json"
			b, err = json.Marshal(multi)

		case "protobuf":
			contentType = "application/protobuf"
			b, err = proto.Marshal(multi)

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
	}

	if err != nil {
		return fetchResponse{nil, contentType, 0, 0, 0}, err
	}
	return fetchResponse{b, contentType, metricsFetched, valuesFetched, memoryUsed}, nil
}

func (listener *CarbonserverListener) fetchSingleMetric(metric string, fromTime, untilTime int32) (*pb3.FetchResponse, error) {
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

	response := pb3.FetchResponse{
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

func (listener *CarbonserverListener) fetchDataPB2(metric string, files []string, leafs []bool, fromTime, untilTime int32) (*pb2.MultiFetchResponse, error) {
	var multi pb2.MultiFetchResponse
	for i, metric := range files {
		if !leafs[i] {
			listener.logger.Debug("skipping directory", zap.String("metric", metric))
			// can't fetch a directory
			continue
		}
		response, err := listener.fetchSingleMetric(metric, fromTime, untilTime)
		if err == nil {
			pb2FetchResponse := &pb2.FetchResponse{
				Name:      proto.String(response.Name),
				StartTime: &response.StartTime,
				StopTime:  &response.StopTime,
				StepTime:  &response.StepTime,
				Values:    response.Values,
				IsAbsent:  response.IsAbsent,
			}
			multi.Metrics = append(multi.Metrics, pb2FetchResponse)
		}
	}
	return &multi, nil
}

func (listener *CarbonserverListener) fetchDataPB3(metric string, files []string, leafs []bool, fromTime, untilTime int32) (*pb3.MultiFetchResponse, error) {
	var multi pb3.MultiFetchResponse
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

	atomic.AddUint64(&listener.metrics.InfoRequests, 1)
	req.ParseForm()
	metric := req.FormValue("target")
	format := req.FormValue("format")

	logger := listener.logger.With(
		zap.String("handler", "infoHandler"),
		zap.String("url", req.URL.RequestURI()),
		zap.String("peer", req.RemoteAddr),
		zap.String("target", metric),
		zap.String("format", format),
	)

	if format == "" {
		format = "json"
	}

	if format != "json" && format != "protobuf" && format != "protobuf3" {
		atomic.AddUint64(&listener.metrics.InfoErrors, 1)
		logger.Info("info failed",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "unsupported format"),
		)
		http.Error(wr, "Bad request (unsupported format)",
			http.StatusBadRequest)
		return
	}

	path := listener.whisperData + "/" + strings.Replace(metric, ".", "/", -1) + ".wsp"
	w, err := whisper.Open(path)

	if err != nil {
		atomic.AddUint64(&listener.metrics.NotFound, 1)
		logger.Info("info served",
			zap.Duration("runtime_seconds", time.Since(t0)),
			zap.String("reason", "metric not found"),
		)
		http.Error(wr, "Metric not found", http.StatusNotFound)
		return
	}

	defer w.Close()

	aggr := w.AggregationMethod()
	maxr := int32(w.MaxRetention())
	xfiles := float32(w.XFilesFactor())

	var b []byte
	if format == "protobuf3" {
		rets := make([]*pb3.Retention, 0, 4)
		for _, retention := range w.Retentions() {
			spp := int32(retention.SecondsPerPoint())
			nop := int32(retention.NumberOfPoints())
			rets = append(rets, &pb3.Retention{
				SecondsPerPoint: spp,
				NumberOfPoints:  nop,
			})
		}

		response := pb3.InfoResponse{
			Name:              metric,
			AggregationMethod: aggr,
			MaxRetention:      maxr,
			XFilesFactor:      xfiles,
			Retentions:        rets,
		}

		b, err = response.Marshal()
	} else {
		rets := make([]*pb2.Retention, 0, 4)
		for _, retention := range w.Retentions() {
			spp := int32(retention.SecondsPerPoint())
			nop := int32(retention.NumberOfPoints())
			rets = append(rets, &pb2.Retention{
				SecondsPerPoint: &spp,
				NumberOfPoints:  &nop,
			})
		}

		response := pb2.InfoResponse{
			Name:              &metric,
			AggregationMethod: &aggr,
			MaxRetention:      &maxr,
			XFilesFactor:      &xfiles,
			Retentions:        rets,
		}

		switch format {
		case "json":
			b, err = json.Marshal(response)
		case "protobuf":
			b, err = proto.Marshal(&response)
		}
	}
	if err != nil {
		atomic.AddUint64(&listener.metrics.RenderErrors, 1)
		logger.Info("info failed",
			zap.String("reason", "response encode failed"),
			zap.Error(err),
		)
		return
	}
	wr.Write(b)

	logger.Info("info served",
		zap.Duration("runtime_seconds", time.Since(t0)),
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
	listener.exitChan <- struct{}{}
	listener.tcpListener.Close()
	return nil
}

func (listener *CarbonserverListener) Listen(listen string) error {
	logger := listener.logger

	logger.Warn("carbonserver support is still experimental, use at your own risk")
	logger.Info("starting carbonserver",
		zap.String("whisperData", listener.whisperData),
		zap.Int("maxGlobs", listener.maxGlobs),
		zap.String("scanFrequency", listener.scanFrequency.String()),
	)

	if listener.scanFrequency != 0 {
		force := make(chan struct{})
		listener.exitChan = make(chan struct{})
		go listener.fileListUpdater(listener.whisperData, time.Tick(listener.scanFrequency), force, listener.exitChan)
		force <- struct{}{}
	}

	listener.queryCache = queryCache{ec: expirecache.New(uint64(listener.queryCacheSizeMB))}

	// +1 to track every over the number of buckets we track
	listener.timeBuckets = make([]uint64, listener.buckets+1)

	carbonserverMux := http.NewServeMux()
	carbonserverMux.HandleFunc("/metrics/find/", httputil.TrackConnections(httputil.TimeHandler(listener.findHandler, listener.bucketRequestTimes)))
	carbonserverMux.HandleFunc("/metrics/list/", httputil.TrackConnections(httputil.TimeHandler(listener.listHandler, listener.bucketRequestTimes)))
	carbonserverMux.HandleFunc("/render/", httputil.TrackConnections(httputil.TimeHandler(listener.fetchHandler, listener.bucketRequestTimes)))
	carbonserverMux.HandleFunc("/info/", httputil.TrackConnections(httputil.TimeHandler(listener.infoHandler, listener.bucketRequestTimes)))

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
