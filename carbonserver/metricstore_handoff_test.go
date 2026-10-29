package carbonserver

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/points"
	whisper "github.com/go-graphite/go-whisper"
)

// Accepted points must remain visible when a writer confirms its batch between
// a read's persistent snapshot and its cache lookup.
func TestIncrementalReadCannotLoseConfirmedCacheBatch(t *testing.T) {
	t.Parallel()
	root, destination := t.TempDir(), t.TempDir()
	const metric = "handoff"
	now := time.Now().Unix()
	w, err := whisper.CreateWithOptions(filepath.Join(root, metric+".wsp"), whisper.NewRetentionsNoPointer([]whisper.Retention{whisper.NewRetention(1, 3600)}), whisper.Average, .5, &whisper.Options{Compressed: true, OutOfOrder: true, FLock: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now - 100), Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(destination, store.Options{ExactBlocks: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: root})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := cache.New()
	queue := make(chan string, 1)
	done := make(chan struct{})
	recv := func(exit chan bool) string {
		select {
		case name := <-queue:
			return name
		case <-exit:
			return ""
		}
	}
	confirm := func(batch *points.Points) { c.Confirm(batch); close(done) }
	p := persister.NewWhisper(root, nil, persister.NewWhisperAggregation(), recv, c.PopNotConfirmed, confirm, c.Pop)
	p.SetMetricStore(db)
	if err := p.SetIncrementalMigration(db, destination); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ConvertFile(context.Background(), metric); err != nil {
		t.Fatal(err)
	}
	c.Add(points.OnePoint(metric, 42, now-10))
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	listener := newMetricStoreTestListener(t, c, db, false)
	listener.cacheGet = func(name string) []points.Point {
		queue <- name
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
		}
		return c.Get(name)
	}
	response, err := listener.fetchFromDisk(metric, int32(now-20), int32(now))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not resume after read")
	}
	found := false
	for _, point := range response.CacheData {
		if point.Timestamp == now-10 && point.Value == 42 {
			found = true
		}
	}
	for i, value := range response.Timeseries.Values() {
		if int64(response.Timeseries.FromTime())+int64(i)*int64(response.Timeseries.Step()) == now-10 && value == 42 {
			found = true
		}
	}
	if !found {
		t.Fatal("accepted point disappeared between persistent snapshot and confirmed cache batch")
	}
}
