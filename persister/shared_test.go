package persister

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

type failedSharedStore struct {
	calls *[]string
	err   error
}

func (f failedSharedStore) Metadata(context.Context, string) (store.Metadata, error) {
	return store.Metadata{}, nil
}
func (f failedSharedStore) Create(context.Context, store.MetricConfig) (store.Metadata, error) {
	return store.Metadata{}, nil
}
func (f failedSharedStore) UpdateMany(context.Context, string, []points.Point) error {
	*f.calls = append(*f.calls, "write")
	return f.err
}

func TestSharedWriteAcknowledgesOnlySuccessfulCommit(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want []string
	}{{"commit", nil, []string{"write", "confirm"}}, {"failed sync", errors.New("sync failed"), []string{"write", "requeue"}}} {
		t.Run(tt.name, func(t *testing.T) {
			var calls []string
			batch := &points.Points{Metric: "a.b", Data: []points.Point{{Timestamp: time.Now().Unix(), Value: 7}}}
			p := NewWhisper(t.TempDir(), nil, NewWhisperAggregation(), nil, func(string) (*points.Points, bool) { return batch, true }, func(*points.Points) { calls = append(calls, "confirm") }, nil)
			p.SetRequeue(func(p *points.Points) {
				if p != batch {
					t.Fatal("requeued another batch")
				}
				calls = append(calls, "requeue")
			})
			p.SetMetricStore(failedSharedStore{&calls, tt.err})
			p.store("a.b")
			if !reflect.DeepEqual(calls, tt.want) {
				t.Fatalf("calls=%v want=%v", calls, tt.want)
			}
		})
	}
}

func TestSharedWriterCreatesAndPersistsWithoutMetricFiles(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "db")
	db, err := store.Open(dbPath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	r := whisper.NewRetention(1, 120)
	schemas := WhisperSchemas{{Name: "all", Pattern: regexp.MustCompile(".*"), Retentions: whisper.Retentions{&r}}}
	now := time.Now().Unix()
	batch := &points.Points{Metric: "long.metric", Data: []points.Point{{Timestamp: now - 2, Value: 1}, {Timestamp: now - 1, Value: 2}}}
	confirmed := false
	p := NewWhisper(root, schemas, NewWhisperAggregation(), nil, func(string) (*points.Points, bool) { return batch, true }, func(*points.Points) { confirmed = true }, nil)
	p.SetMetricStore(db)
	p.store(batch.Metric)
	if !confirmed {
		t.Fatal("successful write not confirmed")
	}
	if _, err := os.Stat(filepath.Join(root, "long", "metric.wsp")); !os.IsNotExist(err) {
		t.Fatalf("metric file was created: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(dbPath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snap, err := db.Snapshot(context.Background(), batch.Metric)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Archives) != 1 || len(snap.Archives[0].Points) != 2 {
		t.Fatalf("recovered snapshot=%+v", snap)
	}
	if snap.Archives[0].Points[0].Value != 1 || snap.Archives[0].Points[1].Value != 2 {
		t.Fatalf("recovered points=%v", snap.Archives[0].Points)
	}
}
