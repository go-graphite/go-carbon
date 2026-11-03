// read-replay adds a bounded render workload from a carbonserver JSON log.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

type config struct {
	Target        string        `json:"target"`
	Multiplier    float64       `json:"rate_multiplier"`
	RPS           float64       `json:"rps"`
	MaxRPS        float64       `json:"max_rps"`
	Concurrency   int           `json:"max_concurrency"`
	Duration      time.Duration `json:"duration_ns"`
	Timeout       time.Duration `json:"timeout_ns"`
	MaxErrorRatio float64       `json:"max_error_ratio"`
	AbortP99      time.Duration `json:"abort_p99_ns"`
	Start         time.Time     `json:"start"`
	End           time.Time     `json:"end"`
}

type interval struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	metrics
}

type summary struct {
	Config       config         `json:"config"`
	Files        []string       `json:"files"`
	Input        inputStats     `json:"input"`
	Plan         plan           `json:"plan"`
	DryRun       bool           `json:"dry_run"`
	Started      time.Time      `json:"started"`
	Stopped      time.Time      `json:"sending_stopped"`
	Ended        time.Time      `json:"ended"`
	Reason       string         `json:"stop_reason"`
	Undrained    int            `json:"undrained"`
	SafetyWindow safetySnapshot `json:"safety_window_at_stop"`
	Total        metrics        `json:"total"`
	Minutes      []interval     `json:"minutes"`
}

type safetySnapshot struct {
	Count      uint64  `json:"count"`
	Failed     uint64  `json:"failed"`
	ErrorRatio float64 `json:"error_ratio"`
	P99        float64 `json:"p99_ms"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := runCLI(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func runCLI(ctx context.Context, args []string, out, errOut io.Writer) int {
	c := config{}
	var start, end, output string
	var dry bool
	f := flag.NewFlagSet("read-replay", flag.ContinueOnError)
	f.SetOutput(errOut)
	f.StringVar(&c.Target, "target", "http://127.0.0.1:8080", "origin serving /render/ (no path, query, or credentials)")
	f.Float64Var(&c.Multiplier, "rate-multiplier", .5, "original arrival rate multiplier; .5 adds 50% of logged traffic")
	f.Float64Var(&c.RPS, "rps", 0, "fixed request rate instead of original arrival timing")
	f.Float64Var(&c.MaxRPS, "max-rps", 100, "hard send-rate cap, without bursts")
	f.IntVar(&c.Concurrency, "max-concurrency", 8, "hard in-flight cap; busy slots are dropped")
	f.DurationVar(&c.Duration, "duration", time.Hour, "maximum sending duration")
	f.DurationVar(&c.Timeout, "timeout", 10*time.Second, "per-request timeout, including body reads")
	f.Float64Var(&c.MaxErrorRatio, "max-error-ratio", .05, "abort above this failed-completion ratio over the last 60s")
	f.DurationVar(&c.AbortP99, "abort-p99", 2*time.Second, "abort above this client p99 over the last 60s")
	f.StringVar(&start, "start", "", "inclusive source timestamp (RFC3339 or log timestamp)")
	f.StringVar(&end, "end", "", "exclusive source timestamp (RFC3339 or log timestamp)")
	f.StringVar(&output, "summary", "read-replay-summary.json", "new JSON summary file (must not already exist)")
	f.BoolVar(&dry, "dry-run", false, "describe input and planned rate without sending")
	f.Usage = func() {
		fmt.Fprintln(errOut, "Usage: read-replay [flags] log.json [log.json.gz ...]")
		f.PrintDefaults()
	}
	if err := f.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 1
	}
	fail := func(err error) int { fmt.Fprintln(errOut, "read-replay:", err); return 1 }
	if len(f.Args()) == 0 {
		f.Usage()
		return 1
	}
	for _, item := range []struct {
		value string
		dest  *time.Time
	}{{start, &c.Start}, {end, &c.End}} {
		if item.value != "" {
			parsed, err := parseTimestamp(item.value)
			if err != nil {
				return fail(err)
			}
			*item.dest = parsed
		}
	}
	if err := c.validate(); err != nil {
		return fail(err)
	}
	// Both modes are explicit choices; a fixed rate must not silently override
	// an explicitly supplied multiplier.
	multiplierSet := false
	f.Visit(func(v *flag.Flag) { multiplierSet = multiplierSet || v.Name == "rate-multiplier" })
	if c.RPS > 0 && multiplierSet {
		return fail(errors.New("choose --rps or --rate-multiplier"))
	}
	loadStarted := time.Now()
	requests, input, err := loadRequests(ctx, f.Args(), c.Start, c.End)
	if err != nil && ctx.Err() == nil {
		return fail(err)
	}
	var s schedule
	var p plan
	if ctx.Err() == nil {
		s, p, err = makeSchedule(requests, c)
		if err != nil {
			return fail(err)
		}
	}
	// Reserve the destination before sending. Never truncate an input or an
	// earlier run's evidence, even if a summary path is supplied by mistake.
	file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fail(err)
	}
	report := summary{Config: c, Files: f.Args(), Input: input, Plan: p, DryRun: dry}
	switch {
	case ctx.Err() != nil:
		report.Reason = "signal"
		report.Started, report.Stopped, report.Ended = loadStarted, time.Now(), time.Now()
		_ = json.NewEncoder(out).Encode(struct {
			Type   string `json:"type"`
			Reason string `json:"stop_reason"`
		}{"summary", "signal"})
	case dry:
		printDryRun(out, requests, input, p)
		report.Reason = "dry_run"
	default:
		client, transport := newClient(c)
		report = runReplay(ctx, c, requests, s, report, client, out)
		transport.CloseIdleConnections()
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	writeErr := encoder.Encode(report)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return fail(err)
	}
	if report.Reason == "max_error_ratio" || report.Reason == "abort_p99" {
		return 3
	}
	return 0
}

func (c config) validate() error {
	for _, v := range []float64{c.Multiplier, c.MaxRPS} {
		if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return errors.New("multiplier and max-rps must be finite and positive")
		}
	}
	if c.RPS < 0 || math.IsNaN(c.RPS) || math.IsInf(c.RPS, 0) || c.RPS > 1e9 || c.MaxRPS > 1e9 {
		return errors.New("rps must be in [0,1e9], max-rps in (0,1e9]")
	}
	if math.Ceil(float64(time.Second)/c.MaxRPS) >= float64(math.MaxInt64) {
		return errors.New("max-rps interval exceeds duration range")
	}
	if c.Concurrency <= 0 || c.Duration <= 0 || c.Duration > 365*24*time.Hour || c.Timeout <= 0 || c.AbortP99 <= 0 {
		return errors.New("concurrency, duration, timeout, and abort-p99 must be positive; duration at most 365d")
	}
	if math.IsNaN(c.MaxErrorRatio) || c.MaxErrorRatio < 0 || c.MaxErrorRatio > 1 {
		return errors.New("max-error-ratio must be in [0,1]")
	}
	if !c.Start.IsZero() && !c.End.IsZero() && !c.End.After(c.Start) {
		return errors.New("end must be after start")
	}
	u, err := url.Parse(c.Target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return errors.New("target must be an http(s) origin without path, query, credentials, or fragment")
	}
	return nil
}

func newClient(c config) (*http.Client, *http.Transport) {
	t := &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: c.Timeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns: c.Concurrency, MaxIdleConnsPerHost: c.Concurrency, MaxConnsPerHost: c.Concurrency,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: c.Timeout, ResponseHeaderTimeout: c.Timeout,
		DisableCompression: true,
	}
	return &http.Client{Transport: t, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, t
}

func send(ctx context.Context, client *http.Client, target string, r request) result {
	start := time.Now()
	x := result{Runtime: r.Runtime, FromCache: r.FromCache}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(target, "/")+r.shifted(start), nil)
	if err == nil {
		req.Header.Set("User-Agent", "go-carbon-read-replay")
		var response *http.Response
		response, err = client.Do(req)
		if response != nil {
			x.Code = response.StatusCode
			x.Bytes, err = io.Copy(io.Discard, response.Body)
			err = errors.Join(err, response.Body.Close())
		}
	}
	if err != nil {
		var timeout net.Error
		x.Timeout = errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout())
		x.Error = !x.Timeout
	}
	x.Latency = time.Since(start)
	return x
}

func printDryRun(out io.Writer, requests []request, input inputStats, p plan) {
	fmt.Fprintf(out, "requests=%d lines=%d skipped=%d malformed=%d outside_window=%d from_cache=%d\n", len(requests), input.Lines, input.Skipped, input.Malformed, input.OutsideWindow, input.FromCache)
	fmt.Fprintf(out, "source=[%s,%s) original_rps=%.6f planned_rps=%.6f capped_rps_upper_bound=%.6f cycle=%s\n", p.Start.Format(time.RFC3339Nano), p.End.Format(time.RFC3339Nano), p.OriginalRPS, p.RequestedRPS, p.CappedRPS, p.Cycle)
	minutes := make(map[time.Time]int)
	targets := make(map[string]int)
	for _, r := range requests {
		minutes[r.At.Truncate(time.Minute)]++
		u, _ := url.ParseRequestURI(r.URL)
		for _, target := range u.Query()["target"] {
			targets[target]++
		}
	}
	// Print empty minutes too, including the silent edges of explicit windows.
	for minute := p.Start.Truncate(time.Minute); minute.Before(p.End); minute = minute.Add(time.Minute) {
		fmt.Fprintf(out, "arrival_minute=%s count=%d\n", minute.Format(time.RFC3339), minutes[minute])
	}
	type targetCount struct {
		name  string
		count int
	}
	top := make([]targetCount, 0, len(targets))
	for name, count := range targets {
		top = append(top, targetCount{name, count})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].count != top[j].count {
			return top[i].count > top[j].count
		}
		return top[i].name < top[j].name
	})
	for _, row := range top[:min(10, len(top))] {
		fmt.Fprintf(out, "target=%q count=%d\n", row.name, row.count)
	}
}
