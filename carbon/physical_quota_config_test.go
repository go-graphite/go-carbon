package carbon

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/persister"
)

func TestPebbleChunkIgnorePhysicalQuotasConfig(t *testing.T) {
	if NewConfig().Whisper.PebbleChunkIgnorePhysicalQuotas {
		t.Fatal("physical quotas ignored by default")
	}
	for _, backend := range []string{"files", "pebble-chunk"} {
		for _, tt := range []struct {
			name string
			text string
			want bool
		}{
			{name: "default"},
			{name: "disabled", text: "pebble-chunk-ignore-physical-quotas = false"},
			{name: "enabled", text: "pebble-chunk-ignore-physical-quotas = true", want: true},
		} {
			t.Run(backend+"/"+tt.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "carbon.conf")
				text := fmt.Sprintf("[whisper]\nstorage-backend = %q\n%s\n", backend, tt.text)
				if err := os.WriteFile(path, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
				cfg, err := ReadConfig(path)
				if err != nil {
					t.Fatal(err)
				}
				if cfg.Whisper.PebbleChunkIgnorePhysicalQuotas != tt.want {
					t.Fatalf("pebble-chunk-ignore-physical-quotas = %t, want %t", cfg.Whisper.PebbleChunkIgnorePhysicalQuotas, tt.want)
				}
				raw := persister.Quota{
					Pattern: "namespace.*", Namespaces: 2, Metrics: 3, LogicalSize: 4,
					PhysicalSize: 5, DataPoints: 6, Throughput: 7,
					DroppingPolicy: "new", StatMetricPrefix: "custom",
				}
				cfg.Whisper.Quotas = persister.WhisperQuotas{raw, {Pattern: "/", PhysicalSize: 1}}
				err = validateStorageConfig(cfg)
				if backend == "pebble-chunk" && !tt.want {
					if err == nil || !strings.Contains(err.Error(), "physical-size") {
						t.Fatalf("physical quota accepted without opt-in: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				quotas := cfg.getCarbonserverQuotas(2 * time.Minute)
				want := carbonserver.Quota{
					Pattern: raw.Pattern, Namespaces: 2, Metrics: 3, LogicalSize: 4,
					PhysicalSize: 5, DataPoints: 6, Throughput: 14,
					DroppingPolicy: carbonserver.QDPNew, StatMetricPrefix: "custom",
				}
				physicalOnly := carbonserver.Quota{Pattern: "/", PhysicalSize: 1, DroppingPolicy: carbonserver.QDPNew}
				if backend == "pebble-chunk" && tt.want {
					want.PhysicalSize, physicalOnly.PhysicalSize = 0, 0
				}
				if len(quotas) != 2 || *quotas[0] != want || *quotas[1] != physicalOnly {
					t.Fatalf("effective quotas = %v, want [%v %v]", quotas, &want, &physicalOnly)
				}
				if cfg.Whisper.Quotas[0] != raw || cfg.Whisper.Quotas[1].PhysicalSize != 1 {
					t.Fatal("conversion changed the configured quota values")
				}
			})
		}
	}
}
