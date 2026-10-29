package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// Ingested entries publish under their own stripes: a busy stripe delays only
// its entry, and the conflict rule still holds against the catalog and within
// the table.
func TestCommitIngestedStagesPerStripe(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	awaitCatalogMembership(t, s)
	config := func(name string) MetricConfig {
		return MetricConfig{Name: name, Retentions: []Retention{{Step: 60, Count: 1440}}, AggregationMethod: Average, XFilesFactor: .5}
	}
	existing, err := s.Create(context.Background(), config("ingest.existing"))
	if err != nil {
		t.Fatal(err)
	}
	busy := "ingest.busy"
	free := ""
	for i := 0; free == ""; i++ {
		if name := fmt.Sprintf("ingest.free%d", i); s.metricStripe(name) != s.metricStripe(busy) {
			free = name
		}
	}
	names := []string{free, busy, existing.Name, "ingest.dup", "ingest.dup"}
	entries := make([]*fileIngestEntry, len(names))
	for i, name := range names {
		entries[i] = &fileIngestEntry{m: Metadata{MetricConfig: config(name), ID: 1000 + uint64(i), Generation: 1, Revision: 1, LastUpdate: time.Unix(1, 0)}}
	}
	unlock := s.lockMetric(busy)
	done := make(chan struct{})
	go func() {
		s.commitIngestedStages(entries)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if m, err := metadataFrom(s.db, free); err == nil && m.ID == 1000 {
			break
		}
		if time.Now().After(deadline) {
			unlock()
			t.Fatal("an entry on a free stripe waited for another entry's busy stripe")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("entry committed while its stripe was held")
	default:
	}
	unlock()
	<-done
	for i, want := range []error{nil, nil, ErrConflict, nil, ErrConflict} {
		if !errors.Is(entries[i].err, want) {
			t.Fatalf("%s: err %v, want %v", names[i], entries[i].err, want)
		}
		if want == nil && entries[i].result.ID != entries[i].m.ID {
			t.Fatalf("%s: result ID %d, want %d", names[i], entries[i].result.ID, entries[i].m.ID)
		}
	}
	for name, id := range map[string]uint64{free: 1000, busy: 1001, existing.Name: existing.ID, "ingest.dup": 1003} {
		if m, err := metadataFrom(s.db, name); err != nil || m.ID != id {
			t.Fatalf("%s: catalog ID %d (%v), want %d", name, m.ID, err, id)
		}
	}
}

// Names no Whisper file could hold are refused before they become catalog
// keys: over 4000 bytes, or a node over 255 bytes. Tags after ';' are not
// path nodes.
func TestCreateRejectsNamesTooLongForWhisper(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	config := func(name string) MetricConfig {
		return MetricConfig{Name: name, Retentions: []Retention{{Step: 60, Count: 1440}}, AggregationMethod: Average, XFilesFactor: .5}
	}
	long := func(n int) string { return strings.Repeat("x", n) }
	for name, want := range map[string]error{
		"a." + long(255) + ".b":                  nil,
		"a." + long(256) + ".b":                  ErrNameTooLong,
		strings.Repeat(long(200)+".", 20)[:3999]: nil,
		strings.Repeat(long(200)+".", 21)[:4001]: ErrNameTooLong,
		"tagged.metric;job=" + long(1000):        nil,
	} {
		if _, err := s.Create(context.Background(), config(name)); !errors.Is(err, want) {
			t.Fatalf("name of %d bytes: err %v, want %v", len(name), err, want)
		}
	}
}

// Metrics stored under names the limit now refuses are deleted by the purge;
// others stay.
func TestPurgeInvalidNamesDeletesOnlyRefusedNames(t *testing.T) {
	t.Parallel()
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	config := MetricConfig{Name: "ok.metric", Retentions: []Retention{{Step: 60, Count: 1440}}, AggregationMethod: Average, XFilesFactor: .5}
	kept, err := s.Create(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	// Plant pre-limit catalog records directly, as an older build wrote them:
	// enough that several share a stripe and the rest span many stripes.
	var bad []Metadata
	stripes := map[uint64]int{}
	for i := 0; i < 5000; i++ {
		m := kept
		m.Name, m.ID = fmt.Sprintf("general.flink.%d.%s.count", i, strings.Repeat("x", 300)), kept.ID+1+uint64(i)
		encoded, err := s.encodeCatalog(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.db.Set(catalogKey(m.Name), encoded, nil); err != nil {
			t.Fatal(err)
		}
		if err := s.db.Set(revisionKey(m), revisionBytes(m), nil); err != nil {
			t.Fatal(err)
		}
		bad = append(bad, m)
		stripes[s.metricStripe(m.Name)]++
	}
	if len(stripes) == len(bad) {
		t.Fatal("no two planted names share a stripe")
	}
	n, err := s.PurgeInvalidNames(context.Background())
	if err != nil || n != len(bad) {
		t.Fatalf("purged %d (%v), want %d", n, err, len(bad))
	}
	if st := s.Stats(); st.PurgeFound != uint64(len(bad)) || st.PurgeRemoved != uint64(len(bad)) || st.PurgeRunning {
		t.Fatalf("purge progress found %d removed %d running %v, want %d %d false", st.PurgeFound, st.PurgeRemoved, st.PurgeRunning, len(bad), len(bad))
	}
	for _, m := range bad {
		if _, err := metadataFrom(s.db, m.Name); !errors.Is(err, ErrNotFound) {
			t.Fatalf("refused name survived the purge: %v", err)
		}
		if _, closer, err := s.db.Get(revisionKey(m)); err == nil {
			closer.Close()
			t.Fatalf("revision of %d survived the purge", m.ID)
		}
	}
	if m, err := metadataFrom(s.db, kept.Name); err != nil || m.ID != kept.ID {
		t.Fatalf("valid metric lost: %v", err)
	}
}

// A purge that finds nothing still compacts the catalog: an earlier run may have
// deleted the long names and stopped before compacting them out of the tables.
func TestPurgeInvalidNamesCompactsAfterInterruptedRun(t *testing.T) {
	s := openFileMigrationStore(t, vfs.NewMem())
	defer closeTestStore(t, s)
	name := "general.flink." + strings.Repeat("x", 300) + ".count"
	if err := s.db.Set(catalogKey(name), []byte("stale"), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Delete(catalogKey(name), nil); err != nil {
		t.Fatal(err)
	}
	before := s.db.Metrics().Compact.Count
	if n, err := s.PurgeInvalidNames(context.Background()); err != nil || n != 0 {
		t.Fatalf("purged %d (%v), want 0", n, err)
	}
	if after := s.db.Metrics().Compact.Count; after == before {
		t.Fatal("purge that found no names did not compact the catalog")
	}
}
