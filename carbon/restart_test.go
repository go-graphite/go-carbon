package carbon

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// metricValue returns the value of the gathered metric name with the given
// label pairs, and whether it was found.
func metricValue(t *testing.T, app *App, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := app.PromRegistry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	next:
		for _, m := range f.GetMetric() {
			have := map[string]string{}
			for _, l := range m.GetLabel() {
				have[l.GetName()] = l.GetValue()
			}
			for k, v := range labels {
				if have[k] != v {
					continue next
				}
			}
			return value(m), true
		}
	}
	return 0, false
}

func value(m *dto.Metric) float64 {
	switch {
	case m.Gauge != nil:
		return m.Gauge.GetValue()
	case m.Counter != nil:
		return m.Counter.GetValue()
	case m.Histogram != nil:
		return float64(m.Histogram.GetSampleCount())
	}
	return 0
}

// startRestored starts an app and waits for its background restore, which
// would otherwise race a dump the test writes next.
func startRestored(t *testing.T, config string) *App {
	t.Helper()
	app := startApp(t, config)
	if done := app.storeRestoreDone; done != nil {
		<-done
	}
	return app
}

func restartTestConfig(t *testing.T, root string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "dump"), 0755); err != nil {
		t.Fatal(err)
	}
	return inputLogTestConfig(t, root, func(cfg *Config) {
		cfg.Dump.Enabled = true
		cfg.Prometheus.Enabled = true
		cfg.Prometheus.Labels = map[string]string{"host": "h1"}
	})
}

// A start after a dump stop reports the handoff it received: the stop's
// duration, the input gap, the dump it restored and how it started.
func TestRestartMetricsAfterHandoff(t *testing.T) {
	root := t.TempDir()
	config := restartTestConfig(t, root)
	first := startRestored(t, config)
	if v, ok := metricValue(t, first, "restarts_total", map[string]string{"kind": "unclean", "host": "h1"}); !ok || v != 1 {
		t.Fatal("first start is not counted unclean", v, ok)
	}
	addInputLogTestPoints(first)
	if err := first.DumpStop(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	first.RecordStopExit()
	first.Stop()
	rec, err := readRestartRecord(filepath.Join(root, "dump"))
	if err != nil || rec.InputClosed.IsZero() || rec.ReadsStopped.IsZero() || rec.Exited.Before(rec.StopStarted.Add(20*time.Millisecond)) {
		t.Fatal("restart record", rec, err)
	}
	dumpInfo, err := os.Stat(filepath.Join(root, "dump", rec.Dump))
	if err != nil {
		t.Fatal(err)
	}

	second := startRestored(t, config)
	defer second.Stop()
	labels := map[string]string{"host": "h1"}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, _ := metricValue(t, second, "restart_stop_seconds", labels); v > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restart_stop_seconds not set")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if v, ok := metricValue(t, second, "restarts_total", map[string]string{"kind": "handoff", "host": "h1"}); !ok || v != 1 {
		t.Fatal("handoff start not counted", v, ok)
	}
	if v, _ := metricValue(t, second, "restart_stop_seconds", labels); v < 0.02 {
		t.Fatal("stop seconds", v)
	}
	if v, _ := metricValue(t, second, "restart_input_unavailable_seconds", labels); v <= 0 || v > 30 {
		t.Fatal("input gap", v)
	}
	if v, _ := metricValue(t, second, "restart_start_seconds", labels); v <= 0 {
		t.Fatal("start seconds", v)
	}
	// The ordered restore runs in the background without whisper.
	for {
		if v, _ := metricValue(t, second, "restart_dump_bytes", labels); v >= float64(dumpInfo.Size()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restart_dump_bytes not set")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := metricValue(t, second, "restart_restore_seconds", labels); !ok {
		t.Fatal("restore seconds missing")
	}
	if _, err := os.Stat(filepath.Join(root, "dump", restartRecordName)); !os.IsNotExist(err) {
		t.Fatal("restart record kept after it was used", err)
	}
	checkInputLogTestPoints(t, second)
}

// A dump without its record is a clean restart; a record whose dump is gone
// is stale and ignored.
func TestRestartKindCleanAndStaleRecord(t *testing.T) {
	root := t.TempDir()
	config := restartTestConfig(t, root)
	first := startRestored(t, config)
	addInputLogTestPoints(first)
	if err := first.DumpStop(); err != nil {
		t.Fatal(err)
	}
	first.Stop()
	dir := filepath.Join(root, "dump")
	rec, err := readRestartRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, restartRecordName)); err != nil {
		t.Fatal(err)
	}
	second := startRestored(t, config)
	if v, ok := metricValue(t, second, "restarts_total", map[string]string{"kind": "clean"}); !ok || v != 1 {
		t.Fatal("clean start not counted", v, ok)
	}
	deadline := time.Now().Add(10 * time.Second)
	for hasDump(dir) {
		if time.Now().After(deadline) {
			t.Fatal("dump not restored")
		}
		time.Sleep(10 * time.Millisecond)
	}
	second.Stop()
	// Left by a successor that already restored its dump.
	if err = writeRestartRecord(dir, rec); err != nil {
		t.Fatal(err)
	}
	third := startRestored(t, config)
	defer third.Stop()
	// The second instance stopped without a dump, so its log may still hold
	// points to replay.
	if v, ok := metricValue(t, third, "restarts_total", map[string]string{"kind": "handoff"}); ok && v != 0 {
		t.Fatal("stale record counted as a handoff", v)
	}
	unclean, _ := metricValue(t, third, "restarts_total", map[string]string{"kind": "unclean"})
	replay, _ := metricValue(t, third, "restarts_total", map[string]string{"kind": "replay"})
	if unclean+replay != 1 {
		t.Fatal("stale record start not counted", unclean, replay)
	}
	if _, err := os.Stat(filepath.Join(dir, restartRecordName)); !os.IsNotExist(err) {
		t.Fatal("stale restart record kept", err)
	}
}

// A start after a crash finds no handoff record but replays the input log:
// it counts as a replay, and its gaps run from the log's last write.
func TestRestartKindReplayAfterCrash(t *testing.T) {
	root := t.TempDir()
	config := restartTestConfig(t, root)
	first := startRestored(t, config)
	addInputLogTestPoints(first)
	if err := first.inputLog.Load().Sync(); err != nil {
		t.Fatal(err)
	}
	first.inputLog.Swap(nil).Kill() // the crash
	first.Stop()
	logDir := filepath.Join(root, "input-log")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatal(err)
	}
	crashed := time.Now().Add(-30 * time.Second)
	for _, e := range entries {
		if err := os.Chtimes(filepath.Join(logDir, e.Name()), crashed, crashed); err != nil {
			t.Fatal(err)
		}
	}
	second := startRestored(t, config)
	defer second.Stop()
	checkInputLogTestPoints(t, second)
	if v, ok := metricValue(t, second, "restarts_total", map[string]string{"kind": "replay"}); !ok || v != 1 {
		t.Fatal("crash start not counted as a replay", v, ok)
	}
	// The gap runs from the backdated last write to receivers opening.
	if v, _ := metricValue(t, second, "restart_input_unavailable_seconds", nil); v < 30 || v > time.Since(crashed).Seconds() {
		t.Fatal("input gap", v, time.Since(crashed))
	}
}
