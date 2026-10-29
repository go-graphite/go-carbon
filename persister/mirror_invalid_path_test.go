package persister

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMirroredInvalidPathsMatchOrdinaryDiscard(t *testing.T) {
	for name, metric := range map[string]string{
		"long component": strings.Repeat("x", maxFilenameLength+1) + ".value",
		"long path":      strings.Repeat("component.", maxPathLength/10+10) + "value",
	} {
		t.Run(name, func(t *testing.T) {
			type outcome struct{ confirmed, notConfirmed, storageErrors int }
			var ordinary outcome
			for _, mirror := range []bool{false, true} {
				root := t.TempDir()
				c := &fakeCache{}
				p := newOOOTestPersister(t, filepath.Join(root, "original"), c)
				p.popConfirm = func(metric string) (*points.Points, bool) {
					v, exists := c.pop(metric)
					if exists {
						c.confirm(v)
					}
					return v, exists
				}
				var logs bytes.Buffer
				p.logger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.ErrorLevel))
				if mirror {
					db, err := openTestStore(t, filepath.Join(root, "exact"), store.Options{ExactBlocks: true})
					if err != nil {
						t.Fatal(err)
					}
					defer func() { _ = db.Close() }()
					p.SetMetricStore(db)
					p.SetMirrorFiles(true)
					p.SetMirrorImportMissing(true)
					p.SetMirrorState(&MirrorState{ScratchDir: root})
				}
				c.add(metric, time.Now().Unix()-3, 42)
				p.store(metric)
				got := outcome{c.confirmedPoints(), c.notConfirmedPoints(), bytes.Count(logs.Bytes(), []byte("\n"))}
				if !mirror {
					ordinary = got
				} else if got != ordinary {
					t.Fatalf("mirror rejection differs from ordinary files: got %+v, ordinary %+v", got, ordinary)
				}
				if got.confirmed != 1 || got.notConfirmed != 0 {
					t.Fatalf("invalid input retained in cache: %+v", got)
				}
			}
		})
	}
}
