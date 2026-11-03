package carbon

import (
	"os"
	"path/filepath"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestRelayoutSpaceConfig(t *testing.T) {
	cfg := NewConfig()
	if cfg.Whisper.StoreTimeBucketsRelayoutMinFree != store.DefaultRelayoutMinFreeBytes || cfg.Whisper.StoreTimeBucketsRelayoutMaxDebt != store.DefaultRelayoutMaxCompactionDebt {
		t.Fatal("unexpected relayout pressure defaults")
	}
	path := filepath.Join(t.TempDir(), "go-carbon.conf")
	for i, body := range []string{
		"store-time-buckets-relayout-min-free-bytes = 123456789\nstore-time-buckets-relayout-max-compaction-debt = 987654321\n",
		"store-time-buckets-relayout-min-free-bytes = 0\n",
		"store-time-buckets-relayout-max-compaction-debt = 0\n",
		"store-time-buckets-relayout-min-free-bytes = -1\n",
		"store-time-buckets-relayout-max-compaction-debt = -1\n",
	} {
		if err := os.WriteFile(path, []byte("[whisper]\n"+body), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := ReadConfig(path)
		if err == nil {
			err = validateStorageConfig(got)
		}
		if i == 0 {
			if err != nil {
				t.Fatal(err)
			}
			if got.Whisper.StoreTimeBucketsRelayoutMinFree != 123456789 || got.Whisper.StoreTimeBucketsRelayoutMaxDebt != 987654321 {
				t.Fatal("limits not decoded")
			}
		} else if err == nil {
			t.Fatalf("invalid pressure limit accepted: %s", body)
		}
	}
}
