package chunkstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

// Creation refuses names longer than the intake limit, and the
// explicit purge removes such names admitted before the limit was lowered.
func TestMetricNameLimit(t *testing.T) {
	if MaxMetricNameBytes != 4096 {
		t.Fatalf("MaxMetricNameBytes = %d, want 4096", MaxMetricNameBytes)
	}
	s, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	fits := strings.Repeat("a", MaxMetricNameBytes)
	createTestMetric(t, s, fits)
	config := MetricConfig{Name: fits + "a", Retentions: []Retention{{Step: 60, Count: 256}}, AggregationMethod: Average}
	if _, err := s.Create(context.Background(), config); !errors.Is(err, ErrNameTooLong) {
		t.Fatalf("over-limit create: %v, want ErrNameTooLong", err)
	}
	if err := s.PlantCatalogNameForTest(context.Background(), config.Name, config); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PurgeInvalidNames(context.Background()); err != nil || n != 1 {
		t.Fatalf("purge removed=%d err=%v, want 1", n, err)
	}
	if _, err := s.Metadata(context.Background(), fits); err != nil {
		t.Fatalf("name at the limit purged: %v", err)
	}
}
