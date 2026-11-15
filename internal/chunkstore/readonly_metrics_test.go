package chunkstore

import (
	"context"
	"testing"
	"time"
)

func TestReadOnlyPebbleWriteMetrics(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	m := createTestMetric(t, s, "read.metrics")
	if err = s.UpdateMany(context.Background(), m.Name, []Point{{Timestamp: time.Now().Unix() - 1, Value: 7}}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.List(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Fetch(context.Background(), m.Name, int(time.Now().Unix()-60), int(time.Now().Unix())); err != nil {
		t.Fatal(err)
	}
	metrics := s.db.Metrics()
	if metrics.WAL.BytesIn != 0 || metrics.WAL.BytesWritten != 0 || metrics.Flush.Count != 0 || metrics.Compact.Count != 0 {
		t.Fatalf("read-only Pebble write metrics: WAL=%+v flush=%+v compact=%+v", metrics.WAL, metrics.Flush, metrics.Compact)
	}
	if s.walSync != nil || s.syncStop != nil || s.compactionStop != nil || s.relayoutStop != nil || s.ingest != nil || s.checkpoint.enabled {
		t.Fatal("read-only open started a writer")
	}
}
