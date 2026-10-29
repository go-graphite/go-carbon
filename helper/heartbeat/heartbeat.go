// Package heartbeat records when a go-carbon process was last alive and whether
// it stopped with its cache preserved. A later start that finds no clean stop
// records the window whose cached points may be lost, so a backfill tool can
// read it (tool/replica-backfill --since-unclean-start).
//
// The file is small JSON, replaced atomically. Failures never affect ingest.
package heartbeat

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Version is the file format version.
const Version = 1

// Window describes one run that ended without preserving its cache.
type Window struct {
	// LastAlive is the last heartbeat of the run that ended uncleanly. Its
	// cached points, up to LossWindowSeconds older than this, may be lost.
	LastAlive         time.Time `json:"last_alive"`
	PreviousPID       int       `json:"previous_pid"`
	PreviousStart     time.Time `json:"previous_start"`
	LossWindowSeconds int64     `json:"loss_window_seconds"`
	// Restarted is the start of the run that detected the unclean stop, and
	// Resumed the moment that run accepted input (zero until then).
	Restarted time.Time `json:"restarted"`
	Resumed   time.Time `json:"resumed,omitempty"`
}

// Range returns the window of possibly missing points. loss replaces a zero
// recorded loss window; margin widens both ends. Until falls back to now while
// the restarted run has not resumed input.
func (w Window) Range(loss, margin time.Duration, now time.Time) (from, until time.Time) {
	if w.LossWindowSeconds > 0 {
		loss = time.Duration(w.LossWindowSeconds) * time.Second
	}
	from = w.LastAlive.Add(-loss - margin)
	until = now
	if !w.Resumed.IsZero() {
		until = w.Resumed.Add(margin)
	}
	if until.After(now) {
		until = now
	}
	return from, until
}

// State is the heartbeat file content.
type State struct {
	Version         int       `json:"version"`
	PID             int       `json:"pid"`
	Started         time.Time `json:"started"`
	Ready           time.Time `json:"ready,omitempty"`
	Updated         time.Time `json:"updated"`
	IntervalSeconds float64   `json:"interval_seconds"`
	// Clean is set once the cache is safely dumped; the process then stops
	// updating the file.
	Clean        bool      `json:"clean"`
	CleanAt      time.Time `json:"clean_at,omitempty"`
	CacheSize    int64     `json:"cache_size"`
	CacheMaxSize int64     `json:"cache_max_size"`
	// Unclean is the most recent unclean stop detected at a start. It is kept
	// across later runs until another unclean stop replaces it.
	Unclean *Window `json:"unclean,omitempty"`
}

// Read loads a heartbeat file.
func Read(path string) (State, error) {
	var s State
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("heartbeat %s: %w", path, err)
	}
	if s.Version != Version {
		return s, fmt.Errorf("heartbeat %s: unsupported version %d", path, s.Version)
	}
	return s, nil
}

// Options configure a heartbeat.
type Options struct {
	Path     string
	Interval time.Duration
	// LossWindow is how long points may stay cached before persistence, for
	// example the cache writeout-max-delay. It is recorded with an unclean stop.
	LossWindow time.Duration
	// Cache optionally samples the cache size and limit on each beat.
	Cache func() (size, maxSize int64)
	Now   func() time.Time
}

// Heartbeat periodically rewrites the file until Stop or MarkClean.
type Heartbeat struct {
	opts    Options
	mu      sync.Mutex
	state   State
	unclean *Window
	stopped bool
	stop    chan struct{}
	done    chan struct{}
	// writeErrors counts failed writes; lastBeat is the last successful one
	// in unix nanoseconds.
	writeErrors atomic.Int64
	lastBeat    atomic.Int64
}

// Start reads the previous state, records an unclean stop if the previous
// run did not mark itself clean, writes the first beat and starts ticking.
// A previous file that cannot be parsed is replaced; the returned error then
// reports it, but the heartbeat still runs.
func Start(o Options) (*Heartbeat, error) {
	if o.Path == "" {
		return nil, errors.New("heartbeat path is empty")
	}
	if o.Interval <= 0 {
		o.Interval = 10 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	now := o.Now()
	h := &Heartbeat{
		opts: o,
		state: State{
			Version: Version, PID: os.Getpid(), Started: now, Updated: now,
			IntervalSeconds: o.Interval.Seconds(),
		},
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	prev, readErr := Read(o.Path)
	switch {
	case readErr == nil && !prev.Clean && !prev.Updated.IsZero():
		h.unclean = &Window{
			LastAlive: prev.Updated, PreviousPID: prev.PID, PreviousStart: prev.Started,
			LossWindowSeconds: int64(o.LossWindow / time.Second), Restarted: now,
		}
		if u := prev.Unclean; u != nil && u.Resumed.IsZero() {
			// The previous run itself followed an unclean stop and never
			// resumed input: that earlier gap is still open, so keep its start.
			h.unclean.LastAlive, h.unclean.PreviousPID, h.unclean.PreviousStart = u.LastAlive, u.PreviousPID, u.PreviousStart
		}
		h.state.Unclean = h.unclean
	case readErr == nil:
		h.state.Unclean = prev.Unclean
	case errors.Is(readErr, os.ErrNotExist):
		readErr = nil
	}
	h.sampleLocked()
	if err := write(o.Path, &h.state); err != nil {
		return nil, err
	}
	h.lastBeat.Store(now.UnixNano())
	go h.loop()
	return h, readErr
}

// Unclean returns the unclean stop detected by this start, or nil.
func (h *Heartbeat) Unclean() *Window {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.unclean == nil {
		return nil
	}
	w := *h.unclean
	return &w
}

func (h *Heartbeat) loop() {
	defer close(h.done)
	t := time.NewTicker(h.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-t.C:
			_ = h.beat(nil, false)
		}
	}
}

func (h *Heartbeat) sampleLocked() {
	if h.opts.Cache != nil {
		h.state.CacheSize, h.state.CacheMaxSize = h.opts.Cache()
	}
}

// beat rewrites the file after applying change, unless already stopped. A
// final beat marks the heartbeat stopped under the same lock.
func (h *Heartbeat) beat(change func(now time.Time), final bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return nil
	}
	now := h.opts.Now()
	h.state.Updated = now
	h.sampleLocked()
	if change != nil {
		change(now)
	}
	if final {
		h.stopped = true
		close(h.stop)
	}
	if err := write(h.opts.Path, &h.state); err != nil {
		h.writeErrors.Add(1)
		return err
	}
	h.lastBeat.Store(now.UnixNano())
	return nil
}

// WriteErrors counts failed heartbeat writes.
func (h *Heartbeat) WriteErrors() int64 { return h.writeErrors.Load() }

// LastBeat returns the time of the last successful write.
func (h *Heartbeat) LastBeat() time.Time { return time.Unix(0, h.lastBeat.Load()) }

// Recorded returns the unclean stop window kept in the file, detected by this
// start or carried from an earlier one, or nil.
func (h *Heartbeat) Recorded() *Window {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state.Unclean == nil {
		return nil
	}
	w := *h.state.Unclean
	return &w
}

// Ready records that input is accepted again.
func (h *Heartbeat) Ready() error {
	if h == nil {
		return nil
	}
	return h.beat(func(now time.Time) {
		h.state.Ready = now
		if h.unclean != nil {
			h.unclean.Resumed = now
		}
	}, false)
}

func (h *Heartbeat) finish(clean bool) error {
	if h == nil {
		return nil
	}
	err := h.beat(func(now time.Time) {
		if clean {
			h.state.Clean, h.state.CleanAt = true, now
		}
	}, true)
	<-h.done
	return err
}

// MarkClean records that the cache is preserved and stops beating.
func (h *Heartbeat) MarkClean() error { return h.finish(true) }

// Stop writes a last beat without the clean mark and stops beating. It is a
// no-op after MarkClean.
func (h *Heartbeat) Stop() error { return h.finish(false) }

func write(path string, s *State) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, err = tmp.Write(append(b, '\n'))
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err != nil {
		_ = os.Remove(name)
	}
	return err
}
