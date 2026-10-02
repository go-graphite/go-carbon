package persister

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
)

func TestStoreRechecksBatchAfterLock(t *testing.T) {
	c := cache.New()
	if err := c.SetWriteoutBatching(8, time.Hour); err != nil {
		t.Fatal(err)
	}
	const metric = "stale.batch"
	now := time.Now().Unix()
	for i := 0; i < 8; i++ {
		c.Add(points.OnePoint(metric, float64(i), now))
	}
	queued := c.WriteoutQueue().Get(nil)
	root := t.TempDir()
	retentions, err := ParseRetentionDefs("1s:1h")
	if err != nil {
		t.Fatal(err)
	}
	p := NewWhisper(root, WhisperSchemas{{Name: "all", Pattern: regexp.MustCompile(".*"), Retentions: retentions}}, NewWhisperAggregation(), c.WriteoutQueue().Get, c.PopNotConfirmed, c.Confirm, c.Pop)
	p.SetWriteoutReady(c.WriteoutReady)
	p.SetRequeue(c.Requeue)
	lock := &p.storeMutex[storeMutexIndex(metric)]
	lock.Lock()
	done := make(chan struct{})
	go func() { p.store(queued); close(done) }()
	// The queued name referred to a batch already handled by a preceding writer.
	prior, _ := c.PopNotConfirmed(metric)
	c.Confirm(prior)
	c.Add(points.OnePoint(metric, 99, now))
	lock.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("store did not finish")
	}
	if c.Size() != 1 {
		t.Fatal("stale queued name flushed the replacement batch")
	}
	if _, err := os.Stat(filepath.Join(root, "stale", "batch.wsp")); !os.IsNotExist(err) {
		t.Fatalf("file opened/created before eligibility: %v", err)
	}
	for i := 0; i < 7; i++ {
		c.Add(points.OnePoint(metric, float64(i), now))
	}
	p.store(metric)
	if !c.IsEmpty() {
		t.Fatal("eligible replacement batch was not persisted")
	}
}
