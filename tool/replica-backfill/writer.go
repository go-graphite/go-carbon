package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/go-graphite/go-carbon/helper/heartbeat"
)

type point struct {
	name  string
	value float64
	ts    int64
}

type writeReq struct {
	pts  []point
	done chan error
}

type writerStats struct {
	points, bytes, flushes, reconnects, pauses atomic.Int64
	pausedNanos                                atomic.Int64
}

// writer sends points to the local plaintext receiver in large rate-limited
// flushes. A batch is acknowledged once all its points are written to the
// socket, so the checkpoint never passes points that were not sent.
type writer struct {
	addr       string
	flushPts   int
	limiter    *rate.Limiter // points per second
	gate       *cacheGate    // optional
	timeout    time.Duration
	retries    int
	backoff    time.Duration
	maxBackoff time.Duration
	in         chan writeReq
	stopped    chan struct{}
	conn       net.Conn
	buf        []byte
	bufPts     int
	waiters    []chan error
	stats      writerStats
}

func newWriter(addr string, pointsPerSecond, flushPts int, gate *cacheGate, retries int, backoff, maxBackoff time.Duration) *writer {
	flushPts = max(flushPts, 1)
	lim := rate.NewLimiter(rate.Inf, flushPts)
	if pointsPerSecond > 0 {
		lim = rate.NewLimiter(rate.Limit(pointsPerSecond), max(flushPts, 1))
	}
	return &writer{
		addr: addr, flushPts: flushPts, limiter: lim, gate: gate,
		timeout: 30 * time.Second, retries: retries, backoff: backoff, maxBackoff: maxBackoff,
		in: make(chan writeReq), stopped: make(chan struct{}),
	}
}

// write queues points and waits until they are written to the socket.
func (w *writer) write(ctx context.Context, pts []point) error {
	if len(pts) == 0 {
		return nil
	}
	req := writeReq{pts: pts, done: make(chan error, 1)}
	select {
	case w.in <- req:
	case <-w.stopped:
		return errors.New("writer stopped")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run serves write requests until ctx ends. A failed flush that exhausts its
// retries is fatal: it fails every waiter and later writes.
func (w *writer) run(ctx context.Context) error {
	defer close(w.stopped)
	defer func() {
		if w.conn != nil {
			_ = w.conn.Close()
		}
	}()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	fail := func(err error) error {
		for _, d := range w.waiters {
			d <- err
		}
		w.waiters = nil
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case req := <-w.in:
			for _, p := range req.pts {
				w.buf = append(w.buf, p.name...)
				w.buf = append(w.buf, ' ')
				w.buf = strconv.AppendFloat(w.buf, p.value, 'g', -1, 64)
				w.buf = append(w.buf, ' ')
				w.buf = strconv.AppendInt(w.buf, p.ts, 10)
				w.buf = append(w.buf, '\n')
				w.bufPts++
				if w.bufPts >= w.flushPts {
					if err := w.flush(ctx); err != nil {
						req.done <- err
						return fail(err)
					}
				}
			}
			w.waiters = append(w.waiters, req.done)
			if w.bufPts == 0 {
				w.ack()
			}
		case <-tick.C:
			if w.bufPts > 0 {
				if err := w.flush(ctx); err != nil {
					return fail(err)
				}
			}
			w.ack()
		}
	}
}

func (w *writer) ack() {
	for _, d := range w.waiters {
		d <- nil
	}
	w.waiters = w.waiters[:0]
}

func (w *writer) flush(ctx context.Context) error {
	if w.gate != nil {
		paused, err := w.gate.wait(ctx)
		if err != nil {
			return err
		}
		if paused > 0 {
			w.stats.pauses.Add(1)
			w.stats.pausedNanos.Add(int64(paused))
		}
	}
	if err := w.limiter.WaitN(ctx, w.bufPts); err != nil {
		return err
	}
	delay := w.backoff
	for attempt := 0; ; attempt++ {
		err := w.send()
		if err == nil {
			break
		}
		if w.conn != nil {
			_ = w.conn.Close()
			w.conn = nil
		}
		if attempt >= w.retries {
			return fmt.Errorf("write to %s: %w", w.addr, err)
		}
		log.Printf("write to %s failed, retrying in %s: %v", w.addr, delay, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(2*delay, w.maxBackoff)
	}
	w.stats.points.Add(int64(w.bufPts))
	w.stats.bytes.Add(int64(len(w.buf)))
	w.stats.flushes.Add(1)
	w.buf, w.bufPts = w.buf[:0], 0
	return nil
}

// send writes the whole buffer. After a partial write the buffer is resent
// on a new connection: duplicate points carry the same value, so repeating
// them is harmless.
func (w *writer) send() error {
	if w.conn == nil {
		c, err := net.DialTimeout("tcp", w.addr, w.timeout)
		if err != nil {
			return err
		}
		if w.stats.flushes.Load() > 0 {
			w.stats.reconnects.Add(1)
		}
		w.conn = c
	}
	if err := w.conn.SetWriteDeadline(time.Now().Add(w.timeout)); err != nil {
		return err
	}
	_, err := w.conn.Write(w.buf)
	return err
}

// cacheGate pauses writes while the local cache is above a fraction of its
// limit, read from the go-carbon heartbeat file. A missing, stale or clean
// (stopped) heartbeat also pauses: the cache state is unknown then.
type cacheGate struct {
	path     string
	pauseAt  float64
	resumeAt float64
	maxAge   time.Duration
	poll     time.Duration
	now      func() time.Time
	paused   bool
	// pause state for the metrics pusher: the current pause's start (zero
	// while writing) and the total of finished pauses.
	mu    sync.Mutex
	since time.Time
	total time.Duration
}

func (g *cacheGate) setPaused(paused bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch {
	case paused && g.since.IsZero():
		g.since = g.now()
	case !paused && !g.since.IsZero():
		g.total += g.now().Sub(g.since)
		g.since = time.Time{}
	}
}

// pausedFor reports whether writes are paused now and the total paused time,
// including the current pause.
func (g *cacheGate) pausedFor() (bool, time.Duration) {
	if g == nil {
		return false, 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.since.IsZero() {
		return false, g.total
	}
	return true, g.total + g.now().Sub(g.since)
}

func (g *cacheGate) state() (fill float64, why string) {
	s, err := heartbeat.Read(g.path)
	switch {
	case err != nil:
		return 0, err.Error()
	case s.Clean:
		return 0, "go-carbon stopped"
	}
	maxAge := g.maxAge
	if maxAge <= 0 {
		maxAge = time.Duration(3*max(s.IntervalSeconds, 10)) * time.Second
	}
	if age := g.now().Sub(s.Updated); age > maxAge {
		return 0, fmt.Sprintf("heartbeat is %s old", age.Round(time.Second))
	}
	if s.CacheMaxSize <= 0 {
		return 0, "cache max-size unknown"
	}
	return float64(s.CacheSize) / float64(s.CacheMaxSize), ""
}

// wait blocks until the cache has room and returns how long it paused.
func (g *cacheGate) wait(ctx context.Context) (time.Duration, error) {
	began := g.now()
	logged := ""
	for waited := false; ; waited = true {
		fill, why := g.state()
		limit := g.pauseAt
		if g.paused {
			limit = g.resumeAt
		}
		if why == "" && fill < limit {
			g.paused = false
			g.setPaused(false)
			if !waited {
				return 0, nil
			}
			return g.now().Sub(began), nil
		}
		g.paused = true
		g.setPaused(true)
		msg := why
		if msg == "" {
			msg = fmt.Sprintf("cache %.0f%% full (pause at %.0f%%, resume below %.0f%%)", 100*fill, 100*g.pauseAt, 100*g.resumeAt)
		}
		if msg != logged && (why != "" || logged == "") {
			log.Printf("writes paused: %s", msg)
			logged = msg
		}
		select {
		case <-ctx.Done():
			g.setPaused(false)
			return g.now().Sub(began), ctx.Err()
		case <-time.After(g.poll):
		}
	}
}
