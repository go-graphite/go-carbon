package convert

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

func checkpointFixture(t *testing.T) (*converter, entry, identity) {
	t.Helper()
	root := t.TempDir()
	source, err := physicalPath(filepath.Join(root, "source"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(source, 0750); err != nil {
		t.Fatal(err)
	}
	w, err := whisper.Create(filepath.Join(source, "metric.wsp"), whisper.MustParseRetentionDefs("1s:1h"), whisper.Last, 0)
	if err != nil {
		t.Fatal(err)
	}
	ts := int(time.Now().Unix()) - 10
	err = w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: ts, Value: math.Float64frombits(0x8000000000000000)}, {Time: ts - 1, Value: 42}})
	if closeErr := w.Close(); err != nil || closeErr != nil {
		t.Fatalf("fixture: %v %v", err, closeErr)
	}
	destination, err := physicalPath(filepath.Join(root, "store"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := chunkstore.Open(destination, chunkstore.Options{ExactBlocks: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	id := identity{Source: source, Copy: filepath.Join(root, "copy"), SourceID: "checkpoint-fixture", Destination: destination, Live: true}
	return &converter{db: db, source: source, copyRoot: id.Copy, verify: true, live: true}, entry{Metric: "metric", Path: "metric.wsp"}, id
}

func TestBaselineCheckpointUpgradesOldPartialWithoutReimport(t *testing.T) {
	c, e, id := checkpointFixture(t)
	ctx := context.Background()
	if count, resumed, err := c.convert(ctx, e); err != nil || resumed || count != 2 {
		t.Fatalf("old baseline: %d %v %v", count, resumed, err)
	}
	before, err := c.db.Snapshot(ctx, e.Metric)
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := baselineCaptureStamp(filepath.Join(id.Copy, e.Path))
	if err != nil {
		t.Fatal(err)
	}
	proof, err := openBaselineCheckpoint(id.Destination, id)
	if err != nil {
		t.Fatal(err)
	}
	c.checkpoint = proof
	for pass := 0; pass < 2; pass++ {
		count, resumed, err := c.convert(ctx, e)
		if err != nil || !resumed || count != 2 || proof.skipped.Load() != uint64(pass) {
			t.Fatalf("resume %d: %d %v %v skipped=%d", pass, count, resumed, err, proof.skipped.Load())
		}
	}
	afterStamp, err := baselineCaptureStamp(filepath.Join(id.Copy, e.Path))
	if err != nil || stamp != afterStamp {
		t.Fatalf("upgrade recaptured existing history: %v", err)
	}
	after, err := c.db.Snapshot(ctx, e.Metric)
	if err != nil || equalSnapshots(before, after) != nil || !reflect.DeepEqual(before.Metadata, after.Metadata) {
		t.Fatalf("upgrade changed existing data or generation: %v", err)
	}
	if err := proof.close(); err != nil {
		t.Fatal(err)
	}
	proof, err = openBaselineCheckpoint(id.Destination, id)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.close()
	c.checkpoint = proof
	if count, resumed, err := c.convert(ctx, e); err != nil || !resumed || count != 2 || proof.skipped.Load() != 1 {
		t.Fatalf("durable receipt not reused: %d %v %v", count, resumed, err)
	}
	wrong := id
	wrong.SourceID = "another-corpus"
	if p, err := openBaselineCheckpoint(id.Destination, wrong); err == nil {
		_ = p.close()
		t.Fatal("checkpoint accepted another dataset")
	}
}

func TestBaselineCheckpointRejectsChangedCaptureAndDestination(t *testing.T) {
	c, e, id := checkpointFixture(t)
	proof, err := openBaselineCheckpoint(id.Destination, id)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.close()
	c.checkpoint = proof
	ctx := context.Background()
	if _, _, err := c.convert(ctx, e); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(id.Copy, e.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := whisper.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(time.Now().Unix()) - 20, Value: 99}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.convert(ctx, e); err == nil {
		t.Fatal("modified retained history was trusted")
	}
	if proof.skipped.Load() != 0 {
		t.Fatal("modified capture reused a receipt")
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.convert(ctx, e); err != nil {
		t.Fatalf("restored capture could not be verified: %v", err)
	}
	changedSnapshot, err := c.db.Snapshot(ctx, e.Metric)
	if err != nil {
		t.Fatal(err)
	}
	changedSnapshot.Archives[0].Points = append(changedSnapshot.Archives[0].Points, chunkstore.Point{Timestamp: time.Now().Unix() - 30, Value: 101})
	if _, err := c.db.Replace(ctx, changedSnapshot); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.convert(ctx, e); err == nil {
		t.Fatal("changed destination revision was trusted")
	}
}

func TestBaselineCheckpointSidecarChangeAndTornReceipt(t *testing.T) {
	c, e, id := checkpointFixture(t)
	proof, err := openBaselineCheckpoint(id.Destination, id)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.close()
	c.checkpoint = proof
	ctx := context.Background()
	if _, _, err := c.convert(ctx, e); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(id.Copy, e.Path)
	initial, err := baselineCaptureStamp(path)
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.db.Metadata(ctx, e.Metric)
	if err != nil {
		t.Fatal(err)
	}
	sidecar := whisper.OutOfOrderSidecarPath(path)
	if err := os.WriteFile(sidecar, []byte("incomplete sidecar"), 0600); err != nil {
		t.Fatal(err)
	}
	changed, err := baselineCaptureStamp(path)
	if err != nil || changed == initial {
		t.Fatalf("sidecar addition missed: %v", err)
	}
	if _, matched, err := proof.match(e.Metric, m, changed); err != nil || matched {
		t.Fatalf("changed OOO capture trusted: %v", err)
	}
	if err := os.Remove(sidecar); err != nil {
		t.Fatal(err)
	}
	if err := proof.db.Set([]byte(e.Metric), []byte{1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, matched, err := proof.match(e.Metric, m, initial); err != nil || matched {
		t.Fatalf("torn receipt trusted: %v", err)
	}
	if _, _, err := c.convert(ctx, e); err != nil {
		t.Fatalf("torn receipt did not trigger successful verification: %v", err)
	}
	c.verify = false
	if _, _, err := c.convert(ctx, e); err == nil {
		t.Fatal("unverified converter accepted checkpoint mode")
	}
}

// Freeze all stat fields, including ctime, to model a write within a filesystem
// timestamp tick deterministically. Resetting mtime alone cannot model this.
func TestBaselineCheckpointRejectsChangedBytesWithIdenticalStat(t *testing.T) {
	for _, sidecar := range []bool{false, true} {
		t.Run(map[bool]string{false: "main", true: "sidecar"}[sidecar], func(t *testing.T) {
			c, e, id := checkpointFixture(t)
			ctx := context.Background()
			if _, _, err := c.convert(ctx, e); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(id.Copy, e.Path)
			changedPath := path
			if sidecar {
				changedPath = whisper.OutOfOrderSidecarPath(path)
				if err := os.WriteFile(changedPath, []byte("original sidecar"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			infos := map[string]os.FileInfo{}
			for _, name := range []string{path, whisper.OutOfOrderSidecarPath(path)} {
				info, err := os.Lstat(name)
				if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				infos[name] = info
			}
			frozenStat := func(name string) (os.FileInfo, error) {
				if info := infos[name]; info != nil {
					return info, nil
				}
				return nil, os.ErrNotExist
			}
			before, err := baselineCaptureStampWithStat(path, frozenStat)
			if err != nil {
				t.Fatal(err)
			}
			proof, err := openBaselineCheckpoint(id.Destination, id)
			if err != nil {
				t.Fatal(err)
			}
			defer proof.close()
			m, err := c.db.Metadata(ctx, e.Metric)
			if err != nil {
				t.Fatal(err)
			}
			if err := proof.verified(e.Metric, m, before, 2); err != nil {
				t.Fatal(err)
			}
			if _, matched, err := proof.match(e.Metric, m, before); err != nil || !matched {
				t.Fatalf("unchanged receipt: %v %v", matched, err)
			}
			f, err := os.OpenFile(changedPath, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			b := []byte{0}
			if _, err := f.ReadAt(b, 0); err != nil {
				t.Fatal(err)
			}
			b[0] ^= 1
			if _, err := f.WriteAt(b, 0); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := baselineCaptureStampWithStat(path, frozenStat)
			if err != nil {
				t.Fatal(err)
			}
			if before == after {
				t.Fatal("changed bytes with identical stat were trusted")
			}
			if _, matched, err := proof.match(e.Metric, m, after); err != nil || matched {
				t.Fatalf("changed content reused receipt: %v %v", matched, err)
			}
		})
	}
}

func TestBaselineCheckpointRejectsStatOnlyReceipt(t *testing.T) {
	c, e, id := checkpointFixture(t)
	if _, _, err := c.convert(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	proof, err := openBaselineCheckpoint(id.Destination, id)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.close()
	m, err := c.db.Metadata(context.Background(), e.Metric)
	if err != nil {
		t.Fatal(err)
	}
	stamp, err := baselineCaptureStamp(filepath.Join(id.Copy, e.Path))
	if err != nil {
		t.Fatal(err)
	}
	if err := proof.verified(e.Metric, m, stamp, 2); err != nil {
		t.Fatal(err)
	}
	data, closer, err := proof.db.Get([]byte(e.Metric))
	if err != nil {
		t.Fatal(err)
	}
	old := append([]byte(nil), data...)
	closer.Close()
	old[0] = 1
	if err := proof.db.Set([]byte(e.Metric), old, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if _, matched, err := proof.match(e.Metric, m, stamp); err != nil || matched {
		t.Fatalf("stat-only receipt reused: %v %v", matched, err)
	}
}
