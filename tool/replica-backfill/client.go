package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"golang.org/x/time/rate"
)

const contentTypeV3 = "application/x-carbonapi-v3-pb"

// series is one metric's values at its first-archive step, as carbonserver
// returns them: Values[i] belongs to Start + i*Step, NaN when absent.
type series struct {
	Start, Step int64
	Values      []float64
}

func (s series) at(ts int64) (float64, bool) {
	if s.Step <= 0 || ts < s.Start || (ts-s.Start)%s.Step != 0 {
		return math.NaN(), false
	}
	i := (ts - s.Start) / s.Step
	if i >= int64(len(s.Values)) {
		return math.NaN(), false
	}
	return s.Values[i], true
}

type clientStats struct {
	requests, retries, failures, bytesOut, bytesIn atomic.Int64
	// Each attempt has one result: ok (2xx or 404), throttled (429), retry
	// (another failure that is retried) or error (a failure that ends the
	// request).
	ok, throttled, retryAttempts, errors atomic.Int64
	nanos                                atomic.Int64 // time spent in attempts
}

// client talks to one carbonserver (/render multi-fetch and /metrics/list).
type client struct {
	base       string
	http       *http.Client
	limiter    *rate.Limiter // shared by all replicas; nil means unlimited
	retries    int
	backoff    time.Duration
	maxBackoff time.Duration
	stats      clientStats
}

type httpError struct {
	status int
	body   string
}

func (e *httpError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.status, e.body) }

// retryable reports throttling, server errors and transport errors.
func retryable(err error) bool {
	var he *httpError
	if errors.As(err, &he) {
		return he.status == http.StatusTooManyRequests || he.status >= 500
	}
	return !errors.Is(err, context.Canceled)
}

// do sends one request with rate limiting, retries and exponential backoff.
// A 404 returns nil body and no error.
func (c *client) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	delay := c.backoff
	for attempt := 0; ; attempt++ {
		if c.limiter != nil {
			if err := c.limiter.Wait(ctx); err != nil {
				return nil, err
			}
		}
		began := time.Now()
		out, retryAfter, err := c.once(ctx, method, path, body)
		c.stats.nanos.Add(int64(time.Since(began)))
		if err == nil {
			c.stats.ok.Add(1)
			return out, nil
		}
		final := !retryable(err) || attempt >= c.retries || ctx.Err() != nil
		var he *httpError
		switch {
		case errors.As(err, &he) && he.status == http.StatusTooManyRequests:
			c.stats.throttled.Add(1)
		case final:
			c.stats.errors.Add(1)
		default:
			c.stats.retryAttempts.Add(1)
		}
		if final {
			c.stats.failures.Add(1)
			return nil, fmt.Errorf("%s %s%s: %w", method, c.base, path, err)
		}
		c.stats.retries.Add(1)
		wait := max(delay, retryAfter)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		delay = min(2*delay, c.maxBackoff)
	}
}

func (c *client) once(ctx context.Context, method, path string, body []byte) ([]byte, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", contentTypeV3)
	}
	c.stats.requests.Add(1)
	c.stats.bytesOut.Add(int64(len(body)))
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	c.stats.bytesIn.Add(int64(len(out)))
	if err != nil {
		return nil, 0, err
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		return out, 0, nil
	case resp.StatusCode == http.StatusNotFound:
		return nil, 0, nil
	}
	var retryAfter time.Duration
	if s, perr := strconv.Atoi(resp.Header.Get("Retry-After")); perr == nil && s > 0 {
		retryAfter = min(time.Duration(s)*time.Second, c.maxBackoff)
	}
	return nil, retryAfter, &httpError{resp.StatusCode, strings.TrimSpace(string(out[:min(len(out), 200)]))}
}

// list returns every metric name the server indexes.
func (c *client) list(ctx context.Context) ([]string, error) {
	out, err := c.do(ctx, http.MethodGet, "/metrics/list/?format=carbonapi_v3_pb", nil)
	if err != nil {
		return nil, err
	}
	var resp protov3.ListMetricsResponse
	if err := resp.UnmarshalVT(out); err != nil {
		return nil, fmt.Errorf("decode metric list: %w", err)
	}
	return resp.Metrics, nil
}

// fetch multi-fetches exact metric names over [from, until]. Names the server
// does not know are absent from the result.
func (c *client) fetch(ctx context.Context, names []string, from, until int64) (map[string]series, error) {
	req := protov3.MultiFetchRequest{Metrics: make([]*protov3.FetchRequest, len(names))}
	for i, n := range names {
		req.Metrics[i] = &protov3.FetchRequest{Name: n, PathExpression: n, StartTime: from, StopTime: until}
	}
	body, err := req.MarshalVT()
	if err != nil {
		return nil, err
	}
	out, err := c.do(ctx, http.MethodPost, "/render/?format=carbonapi_v3_pb", body)
	if err != nil || out == nil {
		return map[string]series{}, err
	}
	var resp protov3.MultiFetchResponse
	if err := resp.UnmarshalVT(out); err != nil {
		return nil, fmt.Errorf("decode render response from %s: %w", c.base, err)
	}
	res := make(map[string]series, len(resp.Metrics))
	for _, m := range resp.Metrics {
		res[m.Name] = series{Start: m.StartTime, Step: m.StepTime, Values: m.Values}
	}
	return res, nil
}
