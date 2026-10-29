package persister

import (
	"context"
	"testing"
	"time"
)

func TestIncrementalPriorityHintsOnlySuccessfulOriginalWrites(t *testing.T) {
	p, cache, db, _ := incrementalPersisterFixture(t)
	defer db.Close()
	var hints []string
	p.SetIncrementalMigrationPriority(func(name string) { hints = append(hints, name) })
	now := time.Now().Unix()
	cache.add("metric", now-2, 77)
	p.store("metric")
	if len(hints) != 1 || hints[0] != "metric" {
		t.Fatalf("successful source write did not issue exactly one hint: %v", hints)
	}
	p.store("metric") // No cached points, so this is not another write.
	if len(hints) != 1 {
		t.Fatal("empty write issued a conversion hint")
	}
	if _, err := p.ConvertFile(context.Background(), "metric"); err != nil {
		t.Fatal(err)
	}
	cache.add("metric", now-1, 88)
	p.store("metric")
	cache.add("brand.new", now-1, 99)
	p.store("brand.new")
	if len(hints) != 1 {
		t.Fatal("shared-only writes re-enqueued conversion work")
	}
	p.SetIncrementalMigrationPriority(nil)
	if p.incrementalPriority.Load() != nil {
		t.Fatal("stopping conversion retained the old callback")
	}
}
