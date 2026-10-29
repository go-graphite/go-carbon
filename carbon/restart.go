package carbon

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/internal/recovery"
)

// processStarted approximates when this process started: package variables
// are initialized before main runs.
var processStarted = time.Now()

// restartRecordName is the file a dump stop leaves in dump.path for its
// successor: when it stopped, when it stopped reading input and serving
// reads, and when it exited. Releases without it ignore the file.
const restartRecordName = ".restart-handoff.json"

// restartRecord is written twice: before the stop releases its successor,
// and again just before the process exits, with Exited set.
type restartRecord struct {
	Version int
	PID     int
	// Dump is the cache dump file the stop wrote. The successor trusts a
	// record only while that file is still there to restore.
	Dump        string
	StopStarted time.Time
	// InputClosed is when the process stopped reading input: input
	// listeners closed, or handed to the successor.
	InputClosed time.Time
	// ReadsStopped is when the process stopped serving reads: listeners
	// closed, or the successor took the read listener over.
	ReadsStopped time.Time
	Exited       time.Time
}

func writeRestartRecord(dir string, r *restartRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".restart-handoff-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	_, err = tmp.Write(data)
	if err = errors.Join(err, tmp.Close()); err != nil {
		return err
	}
	// Timing data: atomic, but it need not survive a power loss.
	return os.Rename(tmp.Name(), filepath.Join(dir, restartRecordName))
}

func readRestartRecord(dir string) (*restartRecord, error) {
	data, err := os.ReadFile(filepath.Join(dir, restartRecordName))
	if err != nil {
		return nil, err
	}
	var r restartRecord
	if err = json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if r.Version != 1 {
		return nil, errors.New("unknown restart record version")
	}
	return &r, nil
}

// restartMetrics describe the restart this process came from, and are set
// once per start.
type restartMetrics struct {
	inputUnavailable, readsUnavailable, stop, start, dumpBytes, restore prometheus.Gauge
	restarts                                                            *prometheus.CounterVec
	registered                                                          sync.Once
}

func newRestartMetrics() *restartMetrics {
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	}
	return &restartMetrics{
		inputUnavailable: gauge("restart_input_unavailable_seconds", "Last restart: from the previous process closing input to this one opening its receivers. Without a handoff record, from the previous process's last write to the input log or dump."),
		readsUnavailable: gauge("restart_reads_unavailable_seconds", "Last restart: from the previous process stopping reads to this one serving them; zero when reads were handed over. Without a handoff record, measured like the input gap."),
		stop:             gauge("restart_stop_seconds", "Last restart: the previous process's stop, from the stop signal to its exit."),
		start:            gauge("restart_start_seconds", "This process's start, from process start to ready."),
		dumpBytes:        gauge("restart_dump_bytes", "Size of the dump this process restored: cache dump plus the input file written during it."),
		restore:          gauge("restart_restore_seconds", "Time to restore the dump fully into the cache."),
		restarts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "restarts_total",
			Help: "Starts by how the previous process ended: handoff (a dump stop that recorded its timings), replay (no record, and the input log held points to replay: a crash, or a stop without a dump), clean (a dump without that record), unclean (none of these)."},
			[]string{"kind"}),
	}
}

func (m *restartMetrics) register(r prometheus.Registerer) {
	m.registered.Do(func() {
		r.MustRegister(m.inputUnavailable, m.readsUnavailable, m.stop, m.start, m.dumpBytes, m.restore, m.restarts)
	})
}

// restartState tracks this process's side of a restart: what it found at
// start and when it opened reads and input, and, while it stops, the record
// it leaves for its successor.
type restartState struct {
	mu   sync.Mutex
	prev *restartRecord
	kind string
	// lastAlive is, without a record, the previous process's last write to
	// the input log or dump directory: about when its input stopped.
	lastAlive    time.Time
	replayed     bool
	readsServing time.Time
	inputOpened  time.Time
	out          *restartRecord
}

// noteReadsServing and noteInputOpened keep the first time of this start.
func (s *restartState) noteReadsServing() {
	s.mu.Lock()
	if s.readsServing.IsZero() {
		s.readsServing = time.Now()
	}
	s.mu.Unlock()
}

func (s *restartState) noteReplayed() {
	s.mu.Lock()
	s.replayed = true
	s.mu.Unlock()
}

func (s *restartState) noteInputOpened() {
	s.mu.Lock()
	if s.inputOpened.IsZero() {
		s.inputOpened = time.Now()
	}
	s.mu.Unlock()
}

// restartRecordWait bounds how long a successor waits for its predecessor's
// final record, written just before that process exits.
var restartRecordWait = 2 * time.Minute

// beginRestart classifies this start; finishRestart counts it once the input
// log replay is known. It runs before the input log opens, so the log's file
// times are still the previous process's. Assumes app.Lock().
func (app *App) beginRestart() {
	if app.restartMetrics == nil {
		app.restartMetrics = newRestartMetrics()
	}
	m := app.restartMetrics
	if app.Config.Prometheus.Enabled {
		m.register(app.PromRegisterer)
	}
	kind := "unclean"
	if dir := app.Config.Dump.Path; app.Config.Dump.Enabled && dir != "" {
		prev, err := readRestartRecord(dir)
		if err == nil && prev.Dump != "" {
			if _, statErr := os.Stat(filepath.Join(dir, prev.Dump)); statErr != nil {
				prev = nil // left by a start that already restored its dump
			}
		} else {
			prev = nil
		}
		switch {
		case prev != nil:
			kind = "handoff"
			app.restart.mu.Lock()
			app.restart.prev = prev
			app.restart.mu.Unlock()
		case hasDump(dir):
			kind = "clean"
		}
		if prev == nil && err == nil {
			_ = os.Remove(filepath.Join(dir, restartRecordName))
		}
	}
	app.restart.mu.Lock()
	app.restart.kind = kind
	if app.restart.prev == nil {
		app.restart.lastAlive = app.lastAlive()
	}
	app.restart.mu.Unlock()
}

// lastAlive returns the newest modification time among dump files and input
// log files, or zero.
func (app *App) lastAlive() time.Time {
	var newest time.Time
	scan := func(dir string, keep func(name string) bool) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.IsDir() || !keep(e.Name()) {
				continue
			}
			if info, err := e.Info(); err == nil && info.ModTime().After(newest) {
				newest = info.ModTime()
			}
		}
	}
	if app.Config.Dump.Enabled && app.Config.Dump.Path != "" {
		scan(app.Config.Dump.Path, recovery.IsDumpFile)
	}
	if app.Config.InputLog.Enabled && app.Config.InputLog.Path != "" {
		scan(app.Config.InputLog.Path, func(string) bool { return true })
	}
	return newest
}

// hasDump reports whether dir holds a dump generation to restore.
func hasDump(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if recovery.IsDumpFile(e.Name()) {
			return true
		}
	}
	return false
}

// finishRestart runs once Start succeeded: it sets the start duration and,
// after a handoff, waits for the predecessor's final record in the
// background to set the gaps.
func (app *App) finishRestart() {
	m := app.restartMetrics
	m.start.Set(time.Since(processStarted).Seconds())
	s := &app.restart
	s.mu.Lock()
	prev, kind, lastAlive := s.prev, s.kind, s.lastAlive
	if prev == nil && s.replayed {
		kind = "replay"
	}
	reads, input := s.readsServing, s.inputOpened
	s.mu.Unlock()
	m.restarts.WithLabelValues(kind).Inc()
	if prev == nil {
		// No record: the gap runs from the previous process's last write, or
		// at least from this process's start.
		if lastAlive.IsZero() {
			lastAlive = processStarted
		}
		m.inputUnavailable.Set(restartGap(lastAlive, input))
		m.readsUnavailable.Set(restartGap(lastAlive, reads))
		zapwriter.Logger("restart").Info("previous process left no handoff record", zap.String("kind", kind),
			zap.Time("last_alive", lastAlive),
			zap.Float64("input_unavailable_seconds", restartGap(lastAlive, input)),
			zap.Float64("reads_unavailable_seconds", restartGap(lastAlive, reads)),
			zap.Float64("start_seconds", time.Since(processStarted).Seconds()))
		return
	}
	dir := app.Config.Dump.Path
	go func() {
		deadline := time.Now().Add(restartRecordWait)
		for prev.Exited.IsZero() && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
			if r, err := readRestartRecord(dir); err == nil && r.PID == prev.PID && r.Dump == prev.Dump {
				prev = r
			}
		}
		// Remove the predecessor's record only: a stop of this process may
		// have replaced it by its own already.
		if r, err := readRestartRecord(dir); err == nil && r.Dump == prev.Dump {
			_ = os.Remove(filepath.Join(dir, restartRecordName))
		}
		s.mu.Lock()
		reads, input := s.readsServing, s.inputOpened
		s.mu.Unlock()
		gap := restartGap
		m.inputUnavailable.Set(gap(prev.InputClosed, input))
		m.readsUnavailable.Set(gap(prev.ReadsStopped, reads))
		if !prev.Exited.IsZero() {
			m.stop.Set(gap(prev.StopStarted, prev.Exited))
		}
		zapwriter.Logger("restart").Info("previous process handed over",
			zap.Float64("input_unavailable_seconds", gap(prev.InputClosed, input)),
			zap.Float64("reads_unavailable_seconds", gap(prev.ReadsStopped, reads)),
			zap.Float64("previous_stop_seconds", gap(prev.StopStarted, prev.Exited)),
			zap.Float64("start_seconds", time.Since(processStarted).Seconds()),
			zap.Bool("final_record", !prev.Exited.IsZero()))
	}()
}

// restartGap returns the seconds from from to to, or zero when either is
// unknown.
func restartGap(from, to time.Time) float64 {
	if from.IsZero() || to.IsZero() {
		return 0
	}
	return max(to.Sub(from).Seconds(), 0)
}

func (app *App) setRestoredDump(bytes int64) {
	if app.restartMetrics != nil {
		app.restartMetrics.dumpBytes.Set(float64(bytes))
	}
}

func (app *App) setRestoreDuration(d time.Duration) {
	if app.restartMetrics != nil {
		app.restartMetrics.restore.Set(d.Seconds())
	}
}

// recordStop updates the record this stop leaves and writes it. Assumes
// the caller is the stopping goroutine.
func (app *App) recordStop(update func(*restartRecord)) {
	s := &app.restart
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.out == nil {
		return
	}
	update(s.out)
	if err := writeRestartRecord(app.Config.Dump.Path, s.out); err != nil {
		zapwriter.Logger("restart").Warn("restart record not written", zap.Error(err))
	}
}

// RecordStopExit completes the restart record of a successful dump stop. Call
// it just before the process exits.
func (app *App) RecordStopExit() {
	app.recordStop(func(r *restartRecord) { r.Exited = time.Now() })
}
