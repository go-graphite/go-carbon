package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
)

// recheckFrozen uses only read-only DB handles after the owning harness stops.
// It neither opens writable journals nor acquires the classic input-log lock.
func recheckFrozen(root, casePath, base string, shards int, quiet ...bool) error {
	var saved struct {
		Stage                     string
		Names                     []candidate
		From, Until               int64
		Attempt                   int
		SinceFirstSnapshotSeconds float64 `json:"since_first_snapshot_seconds"`
		Context                   struct {
			When string `json:"when_cet"`
		}
	}
	b, err := os.ReadFile(casePath)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(b, &saved); err != nil {
		return err
	}
	if root == "" || shards < 1 || (saved.Stage != "comparison" && saved.Stage != "l3_comparison") || len(saved.Names) == 0 || saved.Until <= saved.From {
		return errors.New("existing shadow root, positive shards and a saved comparison required")
	}
	for _, n := range saved.Names {
		if n.ID == 0 || n.Name == "" {
			return errors.New("saved candidate identity missing")
		}
	}
	when, err := time.Parse(time.RFC3339, saved.Context.When)
	if err != nil {
		return err
	}
	// The serial producer catalog survives close in both the raw L2 tree and
	// the integrated daemon tree, whose writer-lock implementations differ.
	if _, err = os.Stat(filepath.Join(root, "placement-group-00000001", "journal", "producer-000", "catalog")); err != nil {
		return err
	}
	job := compareJob{Names: saved.Names, From: saved.From, Until: saved.Until, Attempt: saved.Attempt + 1, FirstSnapshot: when.Add(-time.Duration(saved.SinceFirstSnapshotSeconds * float64(time.Second))), PostClose: true}
	if len(quiet) > 0 {
		job.SkipObservations = quiet[0]
	}
	if saved.Stage == "l3_comparison" {
		job.Layer = 3
	}
	var dbs []*pebble.DB
	defer func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	}()
	for i := 0; i < shards; i++ {
		dir := filepath.Join(root, "placement-group-00000001", "core", fmt.Sprintf("owner-%03d", i))
		db, e := pebble.Open(dir, &pebble.Options{ReadOnly: true, ErrorIfNotExists: true})
		if e != nil {
			for _, snapshot := range job.Snapshots {
				_ = snapshot.Close()
			}
			return e
		}
		dbs = append(dbs, db)
		job.Snapshots = append(job.Snapshots, db.NewSnapshot())
	}
	sums := &totals{}
	compare(job, base, sums)
	emit(map[string]any{"stage": "recheck_totals", "context": contextLoad(), "totals": sums, "source_cursor": "not observed; databases opened read-only after writer stop"})
	if sums.Points == 0 || sums.Mismatches > 0 || sums.Failures > 0 {
		return fmt.Errorf("frozen recheck incomplete or differing: %+v", *sums)
	}
	return nil
}
