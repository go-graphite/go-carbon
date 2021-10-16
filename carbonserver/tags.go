package carbonserver

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/go-graphite/go-carbon/tags"
)

// Graphite doc: https://graphite.readthedocs.io/en/latest/tags.html

type metricID int32

type tagsIndex struct {
	idIdx   metricID
	ids     map[metricID]string
	metrics map[string]metricID

	tags map[string]map[string][]metricID

	// tvTrigrams?
	//  tag value trigrams -> value id

	tagsTries map[string]*trieIndex
}

type tagMeta struct {
	ids []metricID
}

func (*tagMeta) trieMeta() {}

// 10 m * 4 * 10

func newTagsIndex() *tagsIndex {
	return &tagsIndex{
		metrics: map[string]metricID{},
		ids:     map[metricID]string{},
		tags:    map[string]map[string][]metricID{},
	}
}

// my.series;tag1=value1;tag2=value2
//
// Tag names must have a length >= 1 and may contain any ascii
// characters except ;!^=. Tag values must also have a length >= 1,
// they may contain any ascii characters except ; and the first
// character must not be ~. UTF-8 characters may work for names and
// values, but they are not well tested and it is not recommended to
// use non-ascii characters in metric names or tags. Metric names get
// indexed under the special tag name, if a metric name starts with one
// or multiple ~ they simply get removed from the derived tag value
// because the ~ character is not allowed to be in the first position
// of the tag value. If a metric name consists of no other characters
// than ~, then it is considered invalid and may get dropped.

func (ti *tagsIndex) addMetric(taggedMetric string) error {
	taggedMetric, err := tags.Normalize(taggedMetric)
	if err != nil {
		return err
	}

	if _, ok := ti.metrics[taggedMetric]; ok {
		return nil
	}

	id := ti.idIdx
	ti.idIdx++

	ti.metrics[taggedMetric] = id
	ti.ids[id] = taggedMetric

	strs := strings.Split(taggedMetric, ";") // TODO: make it zero-allocation?

	// tagSet := map[string]string{}
	for i, tagp := range strs {
		var tag, val string
		if i == 0 {
			tag = "name" // TODO: allow tag using value of "name"
			val = tagp
		} else {
			kv := strings.Split(tagp, "=") // TODO: make it zero-allocation?
			tag = kv[0]
			val = kv[1]
		}

		vals, ok := ti.tags[tag]
		if !ok {
			ti.tags[tag] = map[string][]metricID{}
			vals = ti.tags[tag]
		}

		// TODO: sorted insert
		vals[val] = append(vals[val], id)
	}

	return nil
}

type tagMatcher int8

const (
	tagMatcherEqual tagMatcher = iota
	tagMatcherNotEqual
	tagMatcherMatch
	tagMatcherNotMatch
)

// # find all series that have tag1 set to value1, sorted by total
// seriesByTag('tag1=value1') | sortByTotal()
//
// # find all series where name matches the regular expression cpu\..*, AND tag1 is not value1
// seriesByTag('name=~cpu\..*', 'tag1!=value1')
//
// Tags expressions are strings, and may have the following formats:
//
// 	tag=spec    tag value exactly matches spec
// 	tag!=spec   tag value does not exactly match spec
// 	tag=~value  tag value matches the regular expression spec
// 	tag!=~spec  tag value does not match the regular expression spec
func (ti *tagsIndex) seriesByTag(tagqs []string) ([]string, error) {
	// case 1: high number of tags
	// 	solution: trigram index
	// case 2: high number of values in some tags
	// 	solution: trigram index
	// case 3: high number of metric in some tag values
	// 	solution: skip list or bit set (roaring)?

	var idSets [][]metricID
	for _, tagq := range tagqs {
		var tag, spec string
		var expr tagMatcher
		var specRegexp *regexp.Regexp
		for i := 0; i < len(tagq); i++ {
			if tagq[i] == '=' {
				tag = tagq[:i]

				// TODO: handle incomplete expr
				if tagq[i+1] == '~' {
					expr = tagMatcherMatch
					spec = tagq[i+2:]

					var err error
					specRegexp, err = regexp.Compile(spec)
					if err != nil {
						return nil, fmt.Errorf("tags: failed to parse regexp: %s: %s", tagq, err)
					}
				} else {
					expr = tagMatcherEqual
					spec = tagq[i+1:]
				}
			} else if tagq[i] == '!' {
				// TODO: handle incomplete expr
				if tagq[i+1] != '=' {
					return nil, fmt.Errorf("tags: broken tag expression: %s", tagq)
				}
				if tagq[i+2] == '~' {
					expr = tagMatcherNotMatch
					spec = tagq[i+3:]

					var err error
					specRegexp, err = regexp.Compile(spec)
					if err != nil {
						return nil, fmt.Errorf("tags: failed to parse regexp: %s: %s", tagq, err)
					}
				} else {
					expr = tagMatcherNotEqual
					spec = tagq[i+2:]
				}
			}
		}

		vals, ok := ti.tags[tag]
		if !ok {
			return nil, nil
		}

		for val, ids := range vals {
			switch expr {
			case tagMatcherEqual:
				if val == spec {
					idSets = append(idSets, ids)
				}
			case tagMatcherNotEqual:
				if val != spec {
					idSets = append(idSets, ids)
				}
			case tagMatcherMatch:
				if specRegexp.MatchString(val) {
					idSets = append(idSets, ids)
				}
			case tagMatcherNotMatch:
				if !specRegexp.MatchString(val) {
					idSets = append(idSets, ids)
				}
			}
		}
	}

	// interset idSets
	if len(idSets) == 0 {
		return nil, nil
	}
	ids := idSets[0]
	idSets = idSets[1:]
	for len(idSets) > 0 {
		ids1 := ids
		ids2 := idSets[0]

		ids = []metricID{}
		idSets = idSets[1:]

		// sort.Slice(ids1, func(i, j int) bool { return ids1[i] < ids1[j] })
		// sort.Slice(ids2, func(i, j int) bool { return ids2[i] < ids2[j] })

		// ids are guaranteed to be sorted
		for i, j := 0, 0; i < len(ids1) && j < len(ids2); {
			if ids1[i] == ids2[j] {
				ids = append(ids, ids1[i])
				i++
				j++
			} else if ids1[i] > ids2[j] {
				j++
			} else {
				i++
			}
		}
	}

	var metrics []string
	for _, id := range ids {
		metrics = append(metrics, ti.ids[id])
	}

	return metrics, nil
}

// curl -s "http://graphite/tags?pretty=1&filter=data"
//
// [
//   {
//     "tag": "datacenter"
//   }
// ]

type TagName struct {
	Tag string
}

func (ti *tagsIndex) getTags(filter string, limit int) ([]TagName, error) {
	exp, err := regexp.Compile(filter)
	if err != nil {
		return nil, fmt.Errorf("getTags: failed to compile %q: %s", filter, err)
	}

	// TODO: optimize
	var matches []TagName
	for t := range ti.tags {
		if exp.MatchString(t) {
			matches = append(matches, TagName{Tag: t})
		}
	}

	return matches, nil
}

// curl -s "http://graphite/tags/datacenter?pretty=1&filter=dc1"
//
// {
//   "tag": "datacenter",
//   "values": [
//     {
//       "count": 2,
//       "value": "dc1"
//     }
//   ]
// }

type TagData struct {
	Tag    string
	Values []TagValue
}

type TagValue struct {
	Count int
	Value string
}

func (ti *tagsIndex) getTagValues(tag string, filter string, limit int) (*TagData, error) {
	exp, err := regexp.Compile(filter)
	if err != nil {
		return nil, fmt.Errorf("getTags: failed to compile %q: %s", filter, err)
	}

	var tdata TagData
	for v, mids := range ti.tags[tag] {
		if exp.MatchString(v) {
			tdata.Values = append(tdata.Values, TagValue{Count: len(mids), Value: v})
		}
	}

	return &tdata, nil
}

func newRegexpState(expr string) (*gmatcher, error) {
	var m = gmatcher{root: &gstate{}, exact: true, expr: expr}
	var cur = m.root
	var alters [][2]*gstate

	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch c {
		case '[':
			m.exact = false
			s := &gstate{}
			i++
			if i >= len(expr) {
				return nil, errors.New("glob: broken range syntax")
			}
			negative := expr[i] == '^'
			if negative {
				i++
			}
			for i < len(expr) && expr[i] != ']' {
				if expr[i] == '-' {
					if i+1 >= len(expr) {
						return nil, errors.New("glob: missing closing range")
					}
					if expr[i-1] > expr[i+1] {
						return nil, errors.New("glob: range start is bigger than range end")
					}
					// a simple check to make sure that range doesn't ends with 0xff,
					// which would causes endless loop bellow
					if expr[i-1] > 128 || expr[i+1] > 128 {
						return nil, errors.New("glob: range overflow")
					}

					for j := expr[i-1] + 1; j <= expr[i+1]; j++ {
						if j != '*' {
							s.c[j] = true
						}
					}

					i += 2
					continue
				}

				s.c[expr[i]] = true
				i++
			}
			if i >= len(expr) || expr[i] != ']' {
				return nil, errors.New("glob: missing ]")
			}

			if negative {
				// TODO: revisit
				for i := 32; i <= 126; i++ {
					if i != '*' {
						s.c[i] = !s.c[i]
					}
				}
			}

			cur.next = append(cur.next, s)
			cur = s

			m.lsComplex = false
		case '?':
			m.exact = false
			var star gstate
			star.c['*'] = true
			cur.next = append(cur.next, &star)
			cur = &star

			m.lsComplex = false
		case '*':
			m.exact = false
			if i == 0 && len(expr) > 2 {
				// TODO: check dup stars
				m.lsComplex = true
			}

			// de-dup multi stars: *** -> *
			for ; i+1 < len(expr) && expr[i+1] == '*'; i++ {
			}

			var split, star gstate
			star.c['*'] = true
			split.c[gstateSplit] = true
			split.next = append(split.next, &star)
			cur.next = append(cur.next, &split)
			star.next = append(star.next, &split)

			cur = &split
		case '{':
			alterStart := &gstate{c: [maxGstateCLen]bool{gstateSplit: true}}
			alterEnd := &gstate{c: [maxGstateCLen]bool{gstateSplit: true}}
			cur.next = append(cur.next, alterStart)
			cur = alterStart
			alters = append(alters, [2]*gstate{alterStart, alterEnd})

			m.lsComplex = false
		case '}':
			if len(alters) == 0 {
				return nil, errors.New("glob: missing {")
			}
			cur.next = append(cur.next, alters[len(alters)-1][1])
			cur = alters[len(alters)-1][1]
			alters = alters[:len(alters)-1]

			m.lsComplex = false
		case ',':
			if len(alters) > 0 {
				cur.next = append(cur.next, alters[len(alters)-1][1])
				cur = alters[len(alters)-1][0]
				continue
			}

			// TODO: should return error?
			fallthrough
		default:
			s := &gstate{}
			s.c[c] = true
			cur.next = append(cur.next, s)
			cur = s
		}
	}
	cur.next = append(cur.next, endGstate)

	if len(alters) > 0 {
		return nil, errors.New("glob: missing }")
	}

	var droot gdstate
	for _, s := range m.root.next {
		droot.gstates = droot.add(droot.gstates, s)
	}
	m.dstates = append(m.dstates, &droot)

	return &m, nil
}

// func (listener *CarbonserverListener) findHandler(wr http.ResponseWriter, req *http.Request) {
// 	// URL: /tags?pretty=1&filter=data

// 	t0 := time.Now()
// 	ctx := req.Context()
// 	span := trace.SpanFromContext(ctx)

// 	atomic.AddUint64(&listener.metrics.TagsRequests, 1)

// 	format := req.FormValue("format")
// 	query := req.Form["query"]

// 	var response *findResponse

// 	logger := TraceContextToZap(ctx, listener.logger.With(
// 		zap.String("handler", "find"),
// 		zap.String("url", req.URL.RequestURI()),
// 		zap.String("peer", req.RemoteAddr),
// 	))

// 	accessLogger := TraceContextToZap(ctx, listener.accessLogger.With(
// 		zap.String("handler", "find"),
// 		zap.String("url", req.URL.RequestURI()),
// 		zap.String("peer", req.RemoteAddr),
// 	))

// 	accepts := req.Header["Accept"]
// 	for _, accept := range accepts {
// 		if accept == httpHeaders.ContentTypeCarbonAPIv3PB {
// 			format = "carbonapi_v3_pb"
// 			break
// 		}
// 	}

// 	if format == "" {
// 		format = "json"
// 	}

// 	formatCode, ok := knownFormats[format]
// 	if !ok {
// 		atomic.AddUint64(&listener.metrics.FindErrors, 1)
// 		accessLogger.Error("find failed",
// 			zap.Duration("runtime_seconds", time.Since(t0)),
// 			zap.String("reason", "unsupported format"),
// 			zap.Int("http_code", http.StatusBadRequest),
// 		)
// 		http.Error(wr, "Bad request (unsupported format)",
// 			http.StatusBadRequest)
// 		return
// 	}

// 	if formatCode == protoV3Format {
// 		body, err := ioutil.ReadAll(req.Body)
// 		if err != nil {
// 			accessLogger.Error("find failed",
// 				zap.Duration("runtime_seconds", time.Since(t0)),
// 				zap.String("reason", err.Error()),
// 				zap.Int("http_code", http.StatusBadRequest),
// 			)
// 			http.Error(wr, "Bad request (unsupported format)",
// 				http.StatusBadRequest)
// 		}

// 		var pv3Request protov3.MultiGlobRequest
// 		pv3Request.Unmarshal(body)

// 		fmt.Printf("\n\n%+v\n\n", pv3Request)

// 		query = pv3Request.Metrics
// 	}

// 	logger = logger.With(
// 		zap.Strings("query", query),
// 		zap.String("format", format),
// 	)

// 	accessLogger = accessLogger.With(
// 		zap.Strings("query", query),
// 		zap.String("format", format),
// 	)

// 	span.SetAttributes(
// 		kv.String("graphite.query", strings.Join(query, ",")),
// 		kv.String("graphite.format", format),
// 	)

// 	if len(query) == 0 {
// 		atomic.AddUint64(&listener.metrics.FindErrors, 1)
// 		accessLogger.Error("find failed",
// 			zap.Duration("runtime_seconds", time.Since(t0)),
// 			zap.String("reason", "empty query"),
// 			zap.Int("http_code", http.StatusBadRequest),
// 		)
// 		http.Error(wr, "Bad request (no query)", http.StatusBadRequest)
// 		return
// 	}

// 	var err error
// 	fromCache := false
// 	if listener.findCacheEnabled {
// 		key := strings.Join(query, ",") + "&" + format
// 		size := uint64(100 * 1024 * 1024)
// 		item := listener.findCache.getQueryItem(key, size, 300)
// 		res, ok := item.FetchOrLock()
// 		listener.prometheus.cacheRequest("find", ok)
// 		if !ok {
// 			logger.Debug("find cache miss")
// 			atomic.AddUint64(&listener.metrics.FindCacheMiss, 1)
// 			response, err = listener.findMetrics(ctx, logger, t0, formatCode, query)
// 			if err != nil {
// 				if _, ok := err.(errorNotFound); ok {
// 					item.StoreAndUnlock(&findResponse{})
// 				} else {
// 					item.StoreAbort()
// 				}
// 			} else {
// 				item.StoreAndUnlock(response)
// 			}
// 		} else if res != nil {
// 			logger.Debug("query cache hit")
// 			atomic.AddUint64(&listener.metrics.FindCacheHit, 1)
// 			response = res.(*findResponse)
// 			fromCache = true

// 			if response.files == 0 {
// 				err = errorNotFound{}
// 			}
// 		}
// 	} else {
// 		response, err = listener.findMetrics(ctx, logger, t0, formatCode, query)
// 	}

// 	if err != nil || response == nil {
// 		var code int
// 		var reason string
// 		if _, ok := err.(errorNotFound); ok {
// 			reason = "Not Found"
// 			code = http.StatusNotFound
// 		} else {
// 			reason = "Internal error while processing request"
// 			code = http.StatusInternalServerError
// 		}

// 		accessLogger.Error("find failed",
// 			zap.Duration("runtime_seconds", time.Since(t0)),
// 			zap.String("reason", reason),
// 			zap.Error(err),
// 			zap.Int("http_code", code),
// 		)
// 		http.Error(wr, fmt.Sprintf("%s (%v)", reason, err), code)

// 		return
// 	}

// 	wr.Header().Set("Content-Type", response.contentType)
// 	wr.Write(response.data)

// 	if response.files == 0 {
// 		// to get an idea how often we search for nothing
// 		atomic.AddUint64(&listener.metrics.FindZero, 1)
// 	}

// 	accessLogger.Info("find success",
// 		zap.Duration("runtime_seconds", time.Since(t0)),
// 		zap.Int("Files", response.files),
// 		zap.Bool("find_cache_enabled", listener.findCacheEnabled),
// 		zap.Bool("from_cache", fromCache),
// 		zap.Int("http_code", http.StatusOK),
// 	)
// 	span.SetAttributes(
// 		kv.Int("graphite.files", response.files),
// 		kv.Bool("graphite.from_cache", fromCache),
// 	)
// }
