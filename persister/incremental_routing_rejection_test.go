package persister

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestIncrementalRejectedNameMatchesOriginalPolicy(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		name := "original"
		if incremental {
			name = "incremental"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			cache := &fakeCache{}
			p := newOOOTestPersister(t, filepath.Join(root, "source"), cache)
			if err := os.MkdirAll(p.rootPath, 0700); err != nil {
				t.Fatal(err)
			}
			if incremental {
				destination := filepath.Join(root, "shared")
				db, err := store.Open(destination, withStageIngest(store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: p.rootPath}))
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				p.SetMetricStore(db)
				if err := p.SetIncrementalMigration(db, destination); err != nil {
					t.Fatal(err)
				}
			}
			var logs bytes.Buffer
			p.logger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.DebugLevel))
			p.createLogger = p.logger
			// Count the legacy drop operation separately from successful writes.
			drops := 0
			pop := p.popConfirm
			p.popConfirm = func(metric string) (*points.Points, bool) {
				drops++
				batch, ok := pop(metric)
				if ok {
					cache.confirm(batch)
				}
				return batch, ok
			}
			for _, metric := range []string{
				"valid." + strings.Repeat("x", 252), // .wsp makes the leaf 256 bytes
				"valid." + strings.Repeat("é", 126), // limits apply to UTF-8 bytes
				strings.Repeat(strings.Repeat("x", 200)+".", 21) + "leaf",
			} {
				cache.add(metric, time.Now().Unix()-10, 123)
				p.store(metric)
				if cache.data[metric] != nil {
					t.Fatal("rejected metric remained queued")
				}
			}
			if drops != 3 || strings.Count(logs.String(), `"msg":"metric name exceeds limit"`) != 3 || strings.Contains(logs.String(), `"level":"error"`) {
				t.Fatalf("rejection changed: drops=%d logs=%s", drops, logs.String())
			}
			// A real source-open failure must still be reported and retain data.
			if err := os.WriteFile(filepath.Join(p.rootPath, "blocked"), []byte("not a directory"), 0600); err != nil {
				t.Fatal(err)
			}
			cache.add("blocked.metric", time.Now().Unix()-10, 456)
			p.store("blocked.metric")
			if cache.data["blocked.metric"] == nil || drops != 3 || !strings.Contains(logs.String(), `"level":"error"`) {
				t.Fatal("source-open failure was hidden or its points discarded")
			}
		})
	}
}
