package persister

import (
	"bytes"
	"context"
	"fmt"
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

func TestLongNamesPersistAndReopen(t *testing.T) {
	for _, size := range []int{300, store.MaxMetricNameBytes - len("long..value")} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			root := t.TempDir()
			c := &fakeCache{}
			p := newOOOTestPersister(t, filepath.Join(root, "unused"), c)
			opts := store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20}
			db, err := store.Open(filepath.Join(root, "db"), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			p.SetMetricStore(db)
			name := "long." + strings.Repeat("x", size) + ".value"
			c.add(name, time.Now().Unix()-2, 42)
			p.store(name)
			if c.confirmedPoints() != 1 || p.NameTooLongDroppedPoints() != 0 {
				t.Fatal("write was dropped or not confirmed")
			}
			want, err := db.Snapshot(context.Background(), name)
			if err != nil || len(want.Archives) == 0 || len(want.Archives[0].Points) != 1 || want.Archives[0].Points[0].Value != 42 {
				t.Fatalf("persisted points: %+v, %v", want, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(filepath.Join(root, "db"), opts)
			if err != nil {
				t.Fatal(err)
			}
			got, err := db.Snapshot(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			if got.Metadata.Name != name {
				t.Fatal("metric key changed")
			}
			if err := store.EqualSnapshots(want, got); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLongNamesWhisperStillRejectsLoudly(t *testing.T) {
	for _, size := range []int{300, 4096} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			c := &fakeCache{}
			p := newOOOTestPersister(t, t.TempDir(), c)
			p.popConfirm = func(metric string) (*points.Points, bool) {
				p, ok := c.pop(metric)
				if ok {
					c.confirm(p)
				}
				return p, ok
			}
			var logs bytes.Buffer
			p.logger = zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.ErrorLevel))
			name := "long." + strings.Repeat("x", size) + ".value"
			c.add(name, time.Now().Unix()-2, 42)
			p.store(name)
			if c.confirmedPoints() != 1 || p.NameTooLongDroppedPoints() != 1 || logs.Len() == 0 {
				t.Fatalf("silent filesystem rejection: confirmed=%d dropped=%d errors=%d", c.confirmedPoints(), p.NameTooLongDroppedPoints(), logs.Len())
			}
		})
	}
}

// Names over MaxMetricNameBytes are refused at ingest and counted,
// not stored.
func TestSharedStoreDropsNamesOverLimit(t *testing.T) {
	root := t.TempDir()
	c := &fakeCache{}
	p := newOOOTestPersister(t, filepath.Join(root, "unused"), c)
	db, err := store.Open(filepath.Join(root, "db"), store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	p.SetMetricStore(db)
	fits := "payments.by_uri." + strings.Repeat("q", store.MaxMetricNameBytes-len("payments.by_uri."))
	over := fits + "x"
	c.add(fits, time.Now().Unix()-2, 1)
	c.add(over, time.Now().Unix()-2, 2)
	p.store(fits)
	p.store(over)
	if c.confirmedPoints() != 2 || p.NameTooLongDroppedPoints() != 1 {
		t.Fatalf("confirmed=%d dropped=%d, want 2 and 1", c.confirmedPoints(), p.NameTooLongDroppedPoints())
	}
	if _, err := db.Snapshot(context.Background(), fits); err != nil {
		t.Fatalf("name at the limit not stored: %v", err)
	}
	if _, err := db.Snapshot(context.Background(), over); err == nil {
		t.Fatal("name over the limit was stored")
	}
}
