package carbonserver

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/NYTimes/gziphandler"
	"github.com/klauspost/compress/gzip"
)

// Cached render responses are kept in the gzip form the HTTP server would send
// anyway. Render data (regular steps, repeated, absent and integer values)
// compresses many times, so the cache holds more entries in the same memory,
// and clients accepting gzip get a hit without compressing it again.
//
// BestSpeed costs about a third of the CPU of the default level gziphandler
// uses, for about a tenth more bytes.
var renderGzipWriters = sync.Pool{
	New: func() interface{} {
		w, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return w
	},
}

var renderGzipReaders sync.Pool

// gzipFetchResponse encodes the response data when gziphandler would compress
// it, which keeps the headers of every response as they were. It counts the
// bytes before and after for the query_cache_*_bytes stats.
func (listener *CarbonserverListener) gzipFetchResponse(response fetchResponse) fetchResponse {
	atomic.AddUint64(&listener.metrics.QueryCacheRawBytes, uint64(len(response.data)))
	if len(response.data) >= gziphandler.DefaultMinSize {
		response.data = gzipBytes(response.data)
		response.gzipped = true
	}
	atomic.AddUint64(&listener.metrics.QueryCacheStoredBytes, uint64(len(response.data)))
	return response
}

// fetchResponseSize is the size of a cached render response: gzip entries
// count their stored size against query-cache-size-mb.
func fetchResponseSize(v interface{}) int {
	return len(v.(fetchResponse).data)
}

func gzipBytes(data []byte) []byte {
	var buf bytes.Buffer
	buf.Grow(len(data) / 8)
	w := renderGzipWriters.Get().(*gzip.Writer)
	w.Reset(&buf)
	// Writes to a bytes.Buffer do not fail.
	_, _ = w.Write(data)
	_ = w.Close()
	renderGzipWriters.Put(w)
	// The cache keeps the slice, so drop the spare capacity of the buffer.
	return bytes.Clone(buf.Bytes())
}

// writeRenderResponse writes the response data, sending a gzip entry as is
// when gziphandler wraps wr. That only happens for clients that accept gzip,
// and gziphandler passes bodies with a Content-Encoding through unchanged.
// Other clients and HEAD requests get the decoded body, which gziphandler
// handles exactly as it did before the cache stored gzip. Like a plain write,
// only decoding reports errors.
func writeRenderResponse(wr http.ResponseWriter, req *http.Request, response fetchResponse) error {
	if !response.gzipped {
		wr.Write(response.data)
		return nil
	}
	if req.Method != http.MethodHead && gzipHandlerWraps(wr) {
		wr.Header().Set("Content-Encoding", "gzip")
		wr.Write(response.data)
		return nil
	}
	return gunzipTo(wr, response.data)
}

func gzipHandlerWraps(wr http.ResponseWriter) bool {
	for {
		switch w := wr.(type) {
		case *gziphandler.GzipResponseWriter, gziphandler.GzipResponseWriterWithCloseNotify:
			return true
		case interface{ Unwrap() http.ResponseWriter }:
			wr = w.Unwrap()
		default:
			return false
		}
	}
}

func gunzipTo(w io.Writer, data []byte) error {
	var err error
	r, _ := renderGzipReaders.Get().(*gzip.Reader)
	if r == nil {
		r, err = gzip.NewReader(bytes.NewReader(data))
	} else {
		err = r.Reset(bytes.NewReader(data))
	}
	if err != nil {
		return err
	}
	_, err = io.Copy(w, r)
	renderGzipReaders.Put(r)
	return err
}
