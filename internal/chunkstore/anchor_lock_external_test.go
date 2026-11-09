package chunkstore_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type anchorLockIO map[string][]byte

func (m anchorLockIO) Get(k []byte) ([]byte, error) {
	v, ok := m[string(k)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return bytes.Clone(v), nil
}
func (m anchorLockIO) Set(k, v []byte) error { m[string(k)] = bytes.Clone(v); return nil }
func (m anchorLockIO) Delete(k []byte) error { delete(m, string(k)); return nil }
func (m anchorLockIO) DeletePrefix(p []byte) error {
	for k := range m {
		if strings.HasPrefix(k, string(p)) {
			delete(m, k)
		}
	}
	return nil
}
func (m anchorLockIO) Visit(p []byte, f func([]byte, []byte) error) error {
	var keys []string
	for k := range m {
		if strings.HasPrefix(k, string(p)) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	for _, k := range keys {
		if err := f([]byte(k), m[k]); err != nil {
			return err
		}
	}
	return nil
}

const anchorLockBase = int64(1793983860)

// Exactly 581 physical points: slots 0..18 are correct and the other 562
// are displaced by one. Include float payloads a numerical comparison misses.
func anchorLockFixture(t testing.TB, name string) store.TransferMetric {
	t.Helper()
	r := store.Retention{Step: 60, Count: 600}
	cfg := store.MetricConfig{Name: name, Retentions: []store.Retention{r}, AggregationMethod: store.Sum}
	points := make([]store.Point, 581)
	for i := range points {
		points[i] = store.Point{Timestamp: anchorLockBase + int64(i)*60, Value: float64(i) / 7}
	}
	points[0].Value = math.Copysign(0, -1)
	points[1].Value = math.Float64frombits(0x7ff8000000000042)
	points[2].Value = math.Inf(-1)
	kv := anchorLockIO{}
	snap := store.Snapshot{Metadata: store.Metadata{MetricConfig: cfg}, Archives: []store.Archive{{Retention: r, Points: points}}}
	if err := (&whisperio.ContinuationCodec{}).RestoreArchives(context.Background(), kv, snap, func() time.Time { return time.Unix(anchorLockBase+600*60, 0) }); err != nil {
		t.Fatal(err)
	}
	var prefix []byte
	for k := range kv {
		if len(k) == 20 && k[9] == 'p' {
			prefix = bytes.Clone([]byte(k)[:16])
			break
		}
	}
	if prefix == nil {
		t.Fatal("no classic groups")
	}
	_ = kv.DeletePrefix(prefix)
	groups := map[int]*store.PointBlock{}
	for i, p := range points {
		slot := i
		if i >= 19 {
			slot++
		}
		g := groups[slot/128]
		if g == nil {
			g = &store.PointBlock{}
			groups[slot/128] = g
		}
		g.Present[slot%128/64] |= 1 << uint(slot%64)
		g.Points[slot%128] = p
	}
	for i, g := range groups {
		_ = kv.Set(binary.BigEndian.AppendUint32(bytes.Clone(prefix), uint32(i)), store.EncodePointBlock(g))
	}
	out := store.TransferMetric{Config: cfg, Version: store.TransferVersion{Name: name}, Continuation: true}
	_ = kv.Visit(nil, func(k, v []byte) error { out.Keys = append(out.Keys, store.RelayoutKV{Key: k, Value: v}); return nil })
	return out
}

type anchorLockCodec struct {
	*whisperio.ContinuationCodec
	afterPlan func()
}

func (c *anchorLockCodec) PlanArchiveAnchors(ctx context.Context, kv store.ContinuationIO) (store.AnchorRepairPlan, error) {
	p, err := c.ContinuationCodec.PlanArchiveAnchors(ctx, kv)
	if c.afterPlan != nil {
		c.afterPlan()
	}
	return p, err
}

func anchorLockStore(t testing.TB, c *anchorLockCodec, cached bool) *store.Store {
	t.Helper()
	o := store.Options{ExactBlocks: true, Continuation: c, CacheSize: 1 << 20, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(anchorLockBase+600*60, 0) }}
	if cached {
		o.MetadataCacheSize = 4 << 20
	}
	s, err := store.Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestAnchorLock581Oracle(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, racing := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%v/race=%v", cached, racing), func(t *testing.T) {
				ctx := context.Background()
				var outputs [][]store.TransferMetric
				for _, oracle := range []bool{true, false} {
					c := &anchorLockCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
					s := anchorLockStore(t, c, cached)
					r := anchorLockFixture(t, "anchor.fixture")
					if _, err := s.ImportTransfer(ctx, []store.TransferMetric{r}); err != nil {
						t.Fatal(err)
					}
					dry, err := s.RepairArchiveAnchors(ctx, r.Config.Name, false)
					if err != nil || len(dry.Archives) != 1 || dry.Archives[0].Points != 581 || dry.Archives[0].Misplaced != 562 {
						t.Fatalf("fixture %+v: %v", dry, err)
					}
					before, err := s.Snapshot(ctx, r.Config.Name)
					if err != nil {
						t.Fatal(err)
					}
					if racing {
						c.afterPlan = func() {
							c.afterPlan = nil
							if err := s.UpdateManyForArchive(ctx, r.Config.Name, []store.Point{{Timestamp: anchorLockBase + 10*60, Value: math.Float64frombits(0x8000000000000000)}}, 36000); err != nil {
								t.Fatal(err)
							}
							before, err = s.Snapshot(ctx, r.Config.Name)
							if err != nil {
								t.Fatal(err)
							}
						}
					}
					var got store.AnchorRepairResult
					if oracle {
						got, err = s.RepairArchiveAnchorsOracleForTest(ctx, r.Config.Name, true)
					} else {
						got, err = s.RepairArchiveAnchors(ctx, r.Config.Name, true)
					}
					if err != nil || !got.Applied {
						t.Fatalf("apply %+v: %v", got, err)
					}
					if racing && got.Retries != 1 {
						t.Fatalf("race not retried: %+v", got)
					}
					after, err := s.Snapshot(ctx, r.Config.Name)
					if err != nil {
						t.Fatal(err)
					}
					if err := store.EqualSnapshots(before, after); err != nil {
						t.Fatal(err)
					}
					wire, err := s.ExportTransfer(ctx, []string{r.Config.Name})
					if err != nil {
						t.Fatal(err)
					}
					outputs = append(outputs, wire)
					t.Logf("oracle=%v lock_ns=%d retries=%d", oracle, got.LockNanos, got.Retries)
				}
				if !reflect.DeepEqual(outputs[0], outputs[1]) {
					t.Fatal("output differs from original implementation")
				}
			})
		}
	}
}

func BenchmarkAnchorLock581(b *testing.B) {
	ctx := context.Background()
	c := &anchorLockCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
	s := anchorLockStore(b, c, false)
	r := anchorLockFixture(b, "anchor.bench")
	var maxLock int64
	var totalLock int64
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		r.Config.Name = fmt.Sprintf("anchor.bench.%d", maxLock+int64(totalLock))
		r.Version.Name = r.Config.Name
		if _, err := s.ImportTransfer(ctx, []store.TransferMetric{r}); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		got, err := s.RepairArchiveAnchors(ctx, r.Config.Name, true)
		if err != nil || !got.Applied {
			b.Fatalf("apply %+v: %v", got, err)
		}
		maxLock = max(maxLock, got.LockNanos)
		totalLock += got.LockNanos
	}
	b.ReportMetric(float64(maxLock), "max-lock-ns")
	b.ReportMetric(float64(totalLock)/float64(b.N), "lock-ns/op")
}

type anchorReadDelayFS struct {
	vfs.FS
	armed atomic.Bool
	reads atomic.Uint64
}
type anchorReadDelayFile struct {
	vfs.File
	fs *anchorReadDelayFS
}

func (f *anchorReadDelayFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	x, err := f.FS.Open(name, opts...)
	if err == nil && strings.HasSuffix(name, ".sst") {
		x = &anchorReadDelayFile{File: x, fs: f}
	}
	return x, err
}
func (f *anchorReadDelayFile) ReadAt(p []byte, off int64) (int, error) {
	if f.fs.armed.Load() {
		f.fs.reads.Add(1)
		time.Sleep(60 * time.Millisecond)
	}
	return f.File.ReadAt(p, off)
}

// A slow metadata table reproduces the tail independently of archive size.
// Delays start only after preparation, so the original implementation holds
// the stripe across them. The candidate must perform no table reads there.
func TestAnchorLockColdMetadata(t *testing.T) {
	ctx := context.Background()
	for _, oracle := range []bool{true, false} {
		t.Run(fmt.Sprintf("oracle=%v", oracle), func(t *testing.T) {
			fs := &anchorReadDelayFS{FS: vfs.NewMem()}
			c := &anchorLockCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
			o := store.Options{ExactBlocks: true, Continuation: c, CacheSize: 1, MemTableSize: 1 << 20, Now: func() time.Time { return time.Unix(anchorLockBase+600*60, 0) }}
			s, err := store.Open("db", o.WithFS(fs))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r := anchorLockFixture(t, "anchor.cold")
			if _, err := s.ImportTransfer(ctx, []store.TransferMetric{r}); err != nil {
				t.Fatal(err)
			}
			if err := s.Flush(); err != nil {
				t.Fatal(err)
			}
			c.afterPlan = func() { fs.armed.Store(true) }
			var got store.AnchorRepairResult
			if oracle {
				got, err = s.RepairArchiveAnchorsOracleForTest(ctx, r.Config.Name, true)
			} else {
				got, err = s.RepairArchiveAnchors(ctx, r.Config.Name, true)
			}
			fs.armed.Store(false)
			if err != nil || !got.Applied {
				t.Fatalf("apply %+v: %v", got, err)
			}
			reads := fs.reads.Load()
			t.Logf("oracle=%v post_prepare_table_reads=%d lock_ns=%d", oracle, reads, got.LockNanos)
			if oracle && (reads == 0 || got.LockNanos < int64(60*time.Millisecond)) {
				t.Fatal("cold-read regression was not reproduced")
			}
			if !oracle && reads != 0 {
				t.Fatalf("publication did %d table reads", reads)
			}
		})
	}
}

func TestAnchorLockRetryCapNeverPublishesStalePlan(t *testing.T) {
	ctx := context.Background()
	c := &anchorLockCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
	s := anchorLockStore(t, c, false)
	r := anchorLockFixture(t, "anchor.contended")
	if _, err := s.ImportTransfer(ctx, []store.TransferMetric{r}); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	var before []store.TransferMetric
	c.afterPlan = func() {
		attempts++
		if err := s.UpdateManyForArchive(ctx, r.Config.Name, []store.Point{{Timestamp: anchorLockBase + 10*60, Value: float64(attempts)}}, 36000); err != nil {
			t.Fatal(err)
		}
		var err error
		before, err = s.ExportTransfer(ctx, []string{r.Config.Name})
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.RepairArchiveAnchors(ctx, r.Config.Name, true)
	if !errors.Is(err, store.ErrConflict) || got.Applied || attempts != 4 || got.Retries != 3 {
		t.Fatalf("retry cap: %+v attempts=%d: %v", got, attempts, err)
	}
	after, err := s.ExportTransfer(ctx, []string{r.Config.Name})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed repair partially published")
	}
}

func TestAnchorLockRetriesDeleteRecreate(t *testing.T) {
	ctx := context.Background()
	c := &anchorLockCodec{ContinuationCodec: &whisperio.ContinuationCodec{}}
	s := anchorLockStore(t, c, false)
	r := anchorLockFixture(t, "anchor.recreated")
	if _, err := s.ImportTransfer(ctx, []store.TransferMetric{r}); err != nil {
		t.Fatal(err)
	}
	c.afterPlan = func() {
		c.afterPlan = nil
		if err := s.Delete(ctx, r.Config.Name); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ImportTransfer(ctx, []store.TransferMetric{r}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.RepairArchiveAnchors(ctx, r.Config.Name, true)
	if err != nil || !got.Applied || got.Retries != 1 || got.Version.ID == 1 {
		t.Fatalf("recreate: %+v: %v", got, err)
	}
	probe, err := s.RepairArchiveAnchors(ctx, r.Config.Name, false)
	if err != nil || probe.Archives[0].Changed {
		t.Fatalf("recreated generation not repaired: %+v %v", probe, err)
	}
}
