package httputil

import (
	"net/http"
	"time"
)

// SlowHandler wraps a http.HandlerFunc and calls cb if the request takes
// longer than max time to process.  The function runs to completion -- slow
// requests are not aborted.
func SlowHandler(fn http.HandlerFunc, max time.Duration, cb func(r *http.Request, t time.Duration)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		t0 := time.Now()
		fn(w, req)
		t := time.Since(t0)
		if t > max {
			cb(req, t)
		}
	}
}

// TimeHandler wraps a http.HandlerFunc and calls cb with the duration.
func TimeHandler(fn http.HandlerFunc, cb func(r *http.Request, t time.Duration)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		t0 := time.Now()
		fn(w, req)
		t := time.Since(t0)
		cb(req, t)
	}
}
