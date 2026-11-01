package carbonserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMetricStoreIndexUpdateErrorClassification(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		for _, test := range []struct {
			name         string
			err          error
			cancellation bool
		}{
			{"canceled", context.Canceled, true},
			{"wrapped canceled", fmt.Errorf("list catalog: %w", context.Canceled), true},
			{"joined canceled", errors.Join(context.Canceled, context.Canceled), true},
			{"deadline", context.DeadlineExceeded, false},
			{"wrapped deadline", fmt.Errorf("list catalog: %w", context.DeadlineExceeded), false},
			{"io error", os.ErrPermission, false},
			{"canceled with io error", errors.Join(context.Canceled, os.ErrPermission), false},
			{"wrapped canceled with io error", fmt.Errorf("merge: %w", errors.Join(context.Canceled, os.ErrPermission)), false},
			{"canceled with deadline", errors.Join(context.Canceled, context.DeadlineExceeded), false},
			{"cancellation text", errors.New("context canceled"), false},
		} {
			t.Run(fmt.Sprintf("stopping=%t/%s", stopping, test.name), func(t *testing.T) {
				l := NewCarbonserverListener(nil)
				logger, logs := metricStoreIndexTestLogger()
				l.logger = logger
				registry := prom.NewRegistry()
				l.InitPrometheus(registry)
				if stopping {
					l.PauseIndexUpdates()
				}
				l.reportMetricStoreIndexUpdateError(test.err)
				wantLevel, wantMessage, wantFailures := zapcore.ErrorLevel, "failed to update shared metric-store index", uint64(1)
				if stopping && test.cancellation {
					wantLevel, wantMessage, wantFailures = zapcore.InfoLevel, "shared metric-store index update stopped by shutdown", 0
				}
				checkMetricStoreIndexLog(t, logs, wantLevel, wantMessage, test.err)
				checkMetricStoreIndexUpdateFailures(t, l, registry, wantFailures)
			})
		}
	}
}

func TestMappedMetricStoreIndexShutdownCancellation(t *testing.T) {
	db, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Create(context.Background(), store.MetricConfig{
		Name: "test.metric", Retentions: []store.Retention{{Step: 1, Count: 60}}, AggregationMethod: store.Average,
	}); err != nil {
		t.Fatal(err)
	}
	l := NewCarbonserverListener(nil)
	logger, logs := metricStoreIndexTestLogger()
	l.logger = logger
	l.SetMetricStore(db)
	l.SetTrieIndex(true)
	l.SetWhisperData(t.TempDir())
	l.SetFileListCache(filepath.Join(t.TempDir(), "files.gz"))
	l.SetFileListCacheVersion(2)
	scratch := t.TempDir()
	l.SetIncrementalIndexScratch(scratch)
	l.PauseIndexUpdates()
	// Enter the builder directly to exercise the cancellation an in-flight
	// update observes, past updateMetricStoreIndex's initial pause check.
	_, err = l.loadMappedMetricStoreCatalog(db)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("mapped index error = %v, want context.Canceled", err)
	}
	l.reportMetricStoreIndexUpdateError(err)
	checkMetricStoreIndexLog(t, logs, zapcore.InfoLevel, "shared metric-store index update stopped by shutdown", err)
	if got := atomic.LoadUint64(&l.metrics.MetricStoreIndexUpdateErrors); got != 0 {
		t.Fatalf("planned stop counted %d failures", got)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("index scratch after stop = %v, %v", entries, err)
	}
	if l.CurrentFileIndex() != nil {
		t.Fatal("canceled build published an index")
	}
}

func TestMetricStoreIndexUpdateLogsIOFailure(t *testing.T) {
	db, err := store.Open(t.TempDir(), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	l := NewCarbonserverListener(nil)
	logger, logs := metricStoreIndexTestLogger()
	l.logger = logger
	l.SetMetricStore(db)
	l.SetTrieIndex(true)
	l.SetFileListCache(filepath.Join(t.TempDir(), "files.gz"))
	l.SetFileListCacheVersion(2)
	// A regular file cannot hold the temporary catalog runs.
	badDirectory := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(badDirectory, nil, 0600); err != nil {
		t.Fatal(err)
	}
	l.SetIncrementalIndexScratch(badDirectory)
	l.updateFileList("", nil, nil)
	checkMetricStoreIndexLog(t, logs, zapcore.ErrorLevel, "failed to update shared metric-store index", nil)
	if got := atomic.LoadUint64(&l.metrics.MetricStoreIndexUpdateErrors); got != 1 {
		t.Fatalf("index failures = %d, want 1", got)
	}
	l.initializeCaches()
	var failures float64 = -1
	l.Stat(func(name string, value float64) {
		if name == "metric_store_index_update_errors" {
			failures = value
		}
	})
	if failures != 1 {
		t.Fatalf("exported index failures = %v, want 1", failures)
	}
}

func metricStoreIndexTestLogger() (*zap.Logger, *bytes.Buffer) {
	logs := new(bytes.Buffer)
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.Lock(zapcore.AddSync(logs)), zap.InfoLevel)
	return zap.New(core), logs
}

func checkMetricStoreIndexLog(t *testing.T, logs *bytes.Buffer, level zapcore.Level, message string, err error) {
	t.Helper()
	var entry struct {
		Level   string `json:"level"`
		Message string `json:"msg"`
		Error   string `json:"error"`
	}
	if decodeErr := json.Unmarshal(logs.Bytes(), &entry); decodeErr != nil {
		t.Fatalf("expected exactly one log entry: %s: %v", logs, decodeErr)
	}
	if entry.Level != level.String() || entry.Message != message || entry.Error == "" {
		t.Fatalf("log = %s, want one %s: %s with error", logs, level, message)
	}
	if err != nil && entry.Error != err.Error() {
		t.Fatalf("logged error = %s, want %v", entry.Error, err)
	}
}

func checkMetricStoreIndexUpdateFailures(t *testing.T, l *CarbonserverListener, registry *prom.Registry, want uint64) {
	t.Helper()
	if got := atomic.LoadUint64(&l.metrics.MetricStoreIndexUpdateErrors); got != want {
		t.Fatalf("index update failures = %d, want %d", got, want)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "metric_store_index_update_errors_total" {
			if got := family.Metric[0].GetCounter().GetValue(); got != float64(want) {
				t.Fatalf("exported index update failures = %v, want %d", got, want)
			}
			return
		}
	}
	t.Fatal("missing index update failure counter")
}
