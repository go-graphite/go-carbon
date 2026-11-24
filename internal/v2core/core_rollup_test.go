package v2core

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
	whisper "github.com/go-graphite/go-whisper"
)

func rollupTestPolicy(t *testing.T, name, def string, method sealrollup.Method, xff float32) (RollupPolicy, []byte) {
	t.Helper()
	p := RollupPolicy{Name: partsformat.ID(name), Group: "test", Config: sealrollup.Config{Method: method, XFilesFactor: xff}}
	for _, r := range whisper.MustParseRetentionDefs(def) {
		p.Config.Archives = append(p.Config.Archives, sealrollup.Archive{Step: int64(r.SecondsPerPoint()), Count: int64(r.NumberOfPoints())})
	}
	encoded, err := EncodeRollupPolicy(p)
	if err != nil {
		t.Fatal(err)
	}
	return p, encoded
}

func TestCoreRollupClassicRestart(t *testing.T) {
	t.Parallel()
	for _, def := range []string{"1s:1d", "1s:1d,1m:30d,1h:1y,1d:10y", "1m:30d,1h:1y,1d:10y", "1m:14d,30m:2y"} {
		for method := sealrollup.Average; method <= sealrollup.First; method++ {
			for _, xff := range []float32{0, .5, 1} {
				t.Run(fmt.Sprintf("%s/%d/%g", def, method, xff), func(t *testing.T) {
					t.Parallel()
					policy, encoded := rollupTestPolicy(t, "test.metric", def, method, xff)
					clock := int64(1794700800)
					root := t.TempDir()
					path := filepath.Join(t.TempDir(), "classic.wsp")
					classic, err := whisper.CreateWithOptions(path, whisper.MustParseRetentionDefs(def), whisper.AggregationMethod(method), xff, &whisper.Options{Sparse: true, Now: func() time.Time { return time.Unix(clock, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					defer classic.Close()
					// Existing engine supplies the independent creation/activity/revision oracle.
					native, err := chunkstore.Open("native", chunkstore.Options{ExactBlocks: true, Now: func() time.Time { return time.Unix(clock, 0) }, CacheSize: 1 << 20, MemTableSize: 1 << 20}.WithFS(vfs.NewMem()))
					if err != nil {
						t.Fatal(err)
					}
					defer native.Close()
					cfgNative := chunkstore.MetricConfig{Name: "test.metric", AggregationMethod: chunkstore.AggregationMethod(method), XFilesFactor: xff}
					for _, a := range policy.Config.Archives {
						cfgNative.Retentions = append(cfgNative.Retentions, chunkstore.Retention{Step: int(a.Step), Count: int(a.Count)})
					}
					if _, err = native.Create(context.Background(), cfgNative); err != nil {
						t.Fatal(err)
					}
					cfg := coreTestConfig()
					cfg.Shards = 1
					cfg.Producers = 1
					cfg.Rollups = true
					cfg.MaxRows = 1024
					core, err := Open(root, cfg, Hooks{})
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if core != nil {
							_ = core.Close()
						}
					}()
					step := policy.Config.Archives[0].Step
					start := clock - 120*step
					var dense []Row
					for i := int64(0); i < 90; i++ {
						dense = append(dense, Row{ID: 7, Time: start + i*step, Bits: math.Float64bits(float64(i%17) - 8), Seq: uint64(i + 1)})
					}
					bits := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1}
					late := []Row{{ID: 7, Time: start + step, Bits: math.Float64bits(7), Seq: 100}, {ID: 7, Time: start + step, Bits: math.Float64bits(9), Seq: 101}}
					for i, b := range bits {
						late = append(late, Row{ID: 7, Time: start + int64(i+2)*step, Bits: b, Seq: uint64(i + 102)})
					}
					advance := policy.Config.Archives[0].Step * policy.Config.Archives[0].Count
					groups := [][]Row{dense, late, {{ID: 7, Time: start + advance, Bits: math.Float64bits(17), Seq: 201}}, {{ID: 7, Time: start + step, Bits: math.Float64bits(127), Seq: 202}}, {{ID: 7, Time: clock - 2*step, Bits: math.Float64bits(11), Seq: 203}}}
					clocks := []int64{clock, clock, clock + advance, clock + advance, clock - 10*step}
					archiveClock := int64(0)
					for i, rows := range groups {
						clock = clocks[i]
						archiveClock = max(archiveClock, clock)
						ptrs := make([]*whisper.TimeSeriesPoint, len(rows))
						np := make([]chunkstore.Point, len(rows))
						for j, p := range rows {
							ptrs[j] = &whisper.TimeSeriesPoint{Time: int(p.Time), Value: math.Float64frombits(p.Bits)}
							np[j] = chunkstore.Point{Timestamp: p.Time, Value: math.Float64frombits(p.Bits)}
						}
						if err = classic.UpdateMany(ptrs); err != nil {
							t.Fatal(err)
						}
						if err = native.UpdateMany(context.Background(), "test.metric", np); err != nil {
							t.Fatal(err)
						}
						if err = core.AppendBatchesPipelined(0, []Batch{{ID: 7, Order: uint64(i + 1), Clock: clock, Policy: encoded, Points: rows}}, uint64(i+1), false); err != nil {
							t.Fatal(err)
						}
						// Original grouping stays unchanged while seal and restart cuts vary.
						if i%2 == 0 || i == len(groups)-1 {
							if err = core.AppendBatches(0, nil, uint64(i+1), true); err != nil {
								t.Fatal(err)
							}
							snap := core.DBs()[0].NewSnapshot()
							err = ScanRollups(snap, func(p RollupPolicy, m RollupMetadata, records []sealrollup.Record) error {
								checkCoreClassicSlots(t, path, p.Config, records, archiveClock)
								for _, span := range []int64{120 * step, p.Config.Archives[0].Step*p.Config.Archives[0].Count + step} {
									from, until := clock-span, clock
									series, err := classic.Fetch(int(from), int(until))
									if err != nil {
										return err
									}
									if series == nil {
										continue
									}
									archive := 0
									for archive < len(p.Config.Archives)-1 && p.Config.Archives[archive].Step*p.Config.Archives[archive].Count < clock-max(from, clock-p.Config.Archives[len(p.Config.Archives)-1].Step*p.Config.Archives[len(p.Config.Archives)-1].Count) {
										archive++
									}
									lookup := map[int64]uint64{}
									for _, r := range records {
										if r.Archive == archive {
											lookup[r.Sample.Time] = r.Sample.Bits
										}
									}
									for j, value := range series.Values() {
										stamp := int64(series.FromTime() + j*series.Step())
										// A rewound classic clock still renders slots v2 retired.
										if rollupRecordExpiry(p.Config, sealrollup.Record{Delta: sealrollup.Delta{Archive: archive, Sample: sealrollup.Sample{Time: stamp}}}) < archiveClock {
											continue
										}
										bits, ok := lookup[stamp]
										if !ok {
											bits = math.Float64bits(math.NaN())
										}
										if bits != math.Float64bits(value) {
											t.Fatalf("render archive%d at%d classic%x core%x", archive, stamp, math.Float64bits(value), bits)
										}
									}
								}
								ns, err := native.Snapshot(context.Background(), "test.metric")
								if err != nil {
									return err
								}
								if m.Revision != ns.Metadata.Revision || m.LastUpdate != ns.Metadata.LastUpdate.UnixNano() {
									t.Fatalf("activity got revision%d last%d want revision%d last%d", m.Revision, m.LastUpdate, ns.Metadata.Revision, ns.Metadata.LastUpdate.UnixNano())
								}
								if m.Frontier.Order != uint64(i+1) {
									t.Fatal("lost original order", m)
								}
								return nil
							})
							_ = snap.Close()
							if err != nil {
								t.Fatal(err)
							}
							if err = core.Close(); err != nil {
								t.Fatal(err)
							}
							core = nil
							core, err = Open(root, cfg, Hooks{})
							if err != nil {
								t.Fatal(err)
							}
						}
					}
				})
			}
		}
	}
}

// Classic keeps expired slots physically; v2 age retirement may drop them, or
// expose an older copy, once their expiry precedes the durable ArchiveClock.
// Neither is readable, so only retained slots are compared.
func checkCoreClassicSlots(t *testing.T, path string, c sealrollup.Config, records []sealrollup.Record, archiveClock int64) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	type key struct {
		a    int
		slot int64
	}
	stored := map[key]sealrollup.Record{}
	for _, r := range records {
		stored[key{r.Archive, r.Slot}] = r
	}
	seen := 0
	for ai, a := range c.Archives {
		off := int(binary.BigEndian.Uint32(data[16+ai*12:]))
		raw := data[off : off+int(a.Count)*12]
		for i := 0; i < len(raw); i += 12 {
			ts := int64(binary.BigEndian.Uint32(raw[i:]))
			if ts == 0 {
				continue
			}
			bits := binary.BigEndian.Uint64(raw[i+4:])
			slot := (ts / a.Step) % a.Count
			expired := func(ts int64) bool {
				return rollupRecordExpiry(c, sealrollup.Record{Delta: sealrollup.Delta{Archive: ai, Sample: sealrollup.Sample{Time: ts}}}) < archiveClock
			}
			r, ok := stored[key{ai, slot}]
			if expired(ts) && (!ok || expired(r.Sample.Time)) {
				delete(stored, key{ai, slot})
				continue
			}
			if !ok || r.Sample.Time != ts || r.Sample.Bits != bits {
				t.Fatalf("archive%d slot%d classic(%d,%x) core%+v", ai, slot, ts, bits, r)
			}
			seen++
		}
	}
	if seen != len(stored) {
		t.Fatalf("occupied core slots%d classic%d", len(stored), seen)
	}
}

func TestCoreRollupFailureDoesNotPublish(t *testing.T) {
	policy, encoded := rollupTestPolicy(t, "test.metric", "1s:120,1m:1d", sealrollup.Sum, 0)
	r := newCoreRollups()
	valid := Batch{ID: 7, Order: 1, Clock: 10000, Policy: encoded, Points: []Row{{ID: 7, Time: 9990, Bits: math.Float64bits(1), Seq: 1}}}
	plan, err := r.plan(SealInput{Batches: []Batch{valid}})
	if err != nil {
		t.Fatal(err)
	}
	// Interned immutable definitions may be shared before ingest. Mutable
	// slots/frontiers are published only by the successful commit callback.
	if len(rollupTestIDs(r)) != 0 {
		t.Fatal("planner published before ingest")
	}
	if plan.Abort != nil {
		plan.Abort()
	}
	if len(rollupTestIDs(r)) != 0 {
		t.Fatal("abort published state")
	}
	plan, err = r.plan(SealInput{Batches: []Batch{valid}})
	if err != nil {
		t.Fatal(err)
	}
	plan.Commit()
	before := r.metaOf(7)
	changed := policy
	changed.Config.Method = sealrollup.Max
	wrong, err := EncodeRollupPolicy(changed)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Batch{valid, {ID: 7, Order: 2, Clock: 10000, Policy: wrong, Points: valid.Points}, {ID: 8, Order: 2, Clock: 10000, Policy: encoded, Points: []Row{{ID: 8, Time: -1}}}} {
		if _, err = r.plan(SealInput{Batches: []Batch{bad}}); err == nil {
			t.Fatal("invalid application accepted")
		}
		if r.metaOf(7).Frontier != before.Frontier || r.metaOf(7).Revision != before.Revision || len(rollupTestIDs(r)) != 1 {
			t.Fatal("failed planner changed durable view")
		}
	}
}

func TestCoreRollupIngestFailureDoesNotPublish(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "db")
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pebble.Open(dir, &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r := newCoreRollups()
	_, policy := rollupTestPolicy(t, "test.metric", "1s:120,1m:1d", sealrollup.Sum, 0)
	cfg := coreTestConfig()
	cfg.Shards = 1
	cfg.Producers = 1
	owner := &coreOwner{db: db, dir: root, cfg: cfg, plan: r.plan}
	rows := []Row{{ID: 7, Time: 9990, Bits: math.Float64bits(3), Seq: 1}}
	batches := []Batch{{ID: 7, Order: 1, Clock: 10000, Policy: policy, Points: rows}}
	if _, err = owner.sealBatches(rows, len(rows), rows[0].Time, PendingBatches(batches), Frontier{Version: 1, Config: cfg, Cursors: []uint64{1}, NextPart: 1}); err == nil {
		t.Fatal("read-only ingest succeeded")
	}
	if len(rollupTestIDs(r)) != 0 {
		t.Fatal("ingest failure published slots/frontier/activity")
	}
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	if err = ScanRollups(snapshot, func(RollupPolicy, RollupMetadata, []sealrollup.Record) error {
		t.Fatal("failed ingest persisted archives")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, closer, err := db.Get(coreMetaKey); err == nil {
		_ = closer.Close()
		t.Fatal("failed ingest persisted core frontier")
	}
}
