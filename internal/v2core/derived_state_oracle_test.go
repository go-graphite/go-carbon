package v2core

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// TestDerivedStateOracle asks which per-name envelope fields a part could stop
// carrying. After every seal it rebuilds each touched name's state from the new
// part's data runs, the part sequence, the part clock and the current policy
// rules only, then compares that with the envelope the planner wrote. Fields
// that must stay carried are reported with their first counterexample.
func TestDerivedStateOracle(t *testing.T) {
	if coreTestProcess(t) {
		return
	}
	t.Setenv("CORE_MERGE_WORKERS", "0")
	type tally struct {
		Checked, ExtraParts                                 int
		MaxTimeExact, MaxTimeAligned, MaxTimeBad            int
		ThroughExact, ThroughEquivalent, ThroughBad         int
		OrderEqualsRecordMax, OrderDiffers                  int
		AfterViolations                                     int
		RevisionBad, LastUpdateBad, PolicyBad, ResetBad     int
		EnvelopeOnly, EnvelopeOnlyExpired, EnvelopeOnlyIdle int
		EnvelopeOnlyOther, EnvelopeOnlyNewName              int
		Examples                                            map[string]string
	}
	type scenario struct {
		name    string
		moving  bool // Hooks.Now follows the seal clock: idle closure and expiry run
		history bool // the TestRollupHistoryDifferential row pattern
		schema  string
		xff     float32
	}
	scenarios := []scenario{
		{name: "history/1s:1h,1m:1d,1h:1y", history: true, schema: "1s:1h,1m:1d,1h:1y", xff: .1},
		{name: "history/10s:6h,5m:7d,1h:1y,1d:10y", history: true, schema: "10s:6h,5m:7d,1h:1y,1d:10y", xff: .1},
		{name: "edges/10s:6h,5m:7d,1h:1y", moving: true, schema: "10s:6h,5m:7d,1h:1y", xff: .5},
	}
	report := map[string]*tally{}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			tl := &tally{Examples: map[string]string{}}
			report[sc.name] = tl
			example := func(field, format string, args ...any) {
				if _, ok := tl.Examples[field]; !ok {
					tl.Examples[field] = fmt.Sprintf(format, args...)
				}
			}
			names := map[uint64]string{1: "oracle.one", 2: "oracle.two", 3: "oracle.three", 5: "oracle.five"}
			policies := map[uint64]RollupPolicy{}
			encoded := map[uint64][]byte{}
			for id, n := range names {
				method := sealrollup.Average
				if id == 2 {
					method = sealrollup.Last
				}
				xff := sc.xff
				if id == 2 {
					xff = .1
				}
				policies[id], encoded[id] = rollupTestPolicy(t, n, sc.schema, method, xff)
			}
			// The receiving store's current rules. oracle.three was created
			// under the 0.5 xFF rule that the rules no longer say (classic keeps a
			// whisper file's creation-time schema).
			current := map[string]sealrollup.Config{}
			for _, p := range policies {
				current[string(p.Name)] = p.Config
			}
			if sc.moving {
				c := policies[3].Config
				c.XFilesFactor = 0
				current["oracle.three"] = c
			}
			cfg := coreTestConfig()
			cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 1, 1, true, 1024
			root := t.TempDir()
			now := int64(1794700800)
			var clock atomic.Int64
			clock.Store(1)
			expireAt := int64(math.MaxInt64)
			hooks := Hooks{Now: func() time.Time { return time.Unix(clock.Load(), 0) }}
			if sc.moving {
				hooks.Expire = func(p RollupPolicy, m RollupMetadata, at time.Time) bool {
					return string(p.Name) == "oracle.two" && at.Unix() >= expireAt && !m.Expired
				}
				hooks.ExpiryTTL = expiryWindow(time.Nanosecond) // every idle name is asked; Expire picks oracle.two
			}
			e, err := Open(root, cfg, hooks)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if e != nil {
					_ = e.Close()
				}
			}()

			type derived struct {
				maxTime, through, lastUpdate int64
				parts                        uint64
				prev                         RollupMetadata
				seen                         bool
			}
			state := map[string]*derived{}
			seq := map[v2part.PartID]int{}
			var order uint64
			bits := []uint64{0x7ff8000000000042, 1 << 63, 0, 0x7ff0000000000000, 0xfff0000000000000, 1, math.Float64bits(7)}
			for epoch := int64(0); epoch < 40; epoch++ {
				now += 600
				if sc.moving {
					clock.Store(now)
				}
				var batches []Batch
				add := func(id uint64, at int64, rows []Row) {
					order++
					for i := range rows {
						rows[i].ID = id
					}
					batches = append(batches, Batch{ID: id, Policy: encoded[id], Order: order, Clock: at, Points: rows})
				}
				var rows []Row
				if sc.history {
					for j := int64(0); j < 60; j++ {
						rows = append(rows, Row{Time: now - 600 + j*10, Bits: bits[(j+epoch)%int64(len(bits))], Seq: uint64(j + 1)})
					}
					for _, age := range []int64{120, 1800, 3500, 7200, 18000, 100000} {
						stamp := now - age
						rows = append(rows, Row{Time: stamp, Bits: math.Float64bits(float64(epoch)), Seq: 100}, Row{Time: stamp, Bits: bits[epoch%int64(len(bits))], Seq: 101})
					}
					add(1, now, rows)
				} else {
					// oracle.one: unaligned, sparse (every fourth epoch fails xFF 0.5),
					// late and duplicate writes; epoch 10 brings only points older than
					// every retention; its clock trails the seal by 5 s.
					if epoch == 10 {
						rows = append(rows, Row{Time: now - 2*366*86400 + 3, Bits: 7, Seq: 1})
					} else {
						for j := int64(0); j < 60; j++ {
							if epoch%4 == 0 && j%5 != 0 || epoch%4 != 0 && (j/6+epoch)%3 == 0 {
								continue
							}
							rows = append(rows, Row{Time: now - 600 + j*10 + 3, Bits: bits[(j+epoch)%int64(len(bits))], Seq: uint64(j + 1)})
						}
						for _, age := range []int64{120, 1800, 7200, 100000} {
							stamp := now - age + 7
							rows = append(rows, Row{Time: stamp, Bits: math.Float64bits(float64(epoch)), Seq: 100}, Row{Time: stamp, Bits: bits[epoch%int64(len(bits))], Seq: 101})
						}
					}
					add(1, now-5, rows)
					// oracle.two: active, then idle (idle closure), expired at epoch
					// 20 and re-incarnated at epoch 25.
					if epoch < 4 || epoch >= 25 {
						add(2, now, []Row{{Time: now - 300, Bits: math.Float64bits(float64(epoch)), Seq: 1}, {Time: now - 10, Bits: 1, Seq: 2}})
					}
					if epoch == 18 {
						expireAt = now + 1200
					}
					// oracle.three: born from backfill older than archive 0 only.
					if epoch == 12 {
						add(3, now, []Row{{Time: now - 2*86400 + 13, Bits: 3, Seq: 1}, {Time: now - 2*86400 - 290, Bits: 4, Seq: 2}})
					} else if epoch > 12 {
						add(3, now, []Row{{Time: now - 17, Bits: 5, Seq: 1}})
					}
					// oracle.five: born from points older than every retention.
					if epoch == 14 {
						add(5, now, []Row{{Time: now - 3*366*86400, Bits: 9, Seq: 1}})
					}
				}
				if err = e.AppendBatches(0, batches, order, true); err != nil {
					t.Fatal(err)
				}
				partClock := int64(0)
				for _, b := range batches {
					partClock = max(partClock, b.Clock)
				}

				// Group the new parts' KVs by name.
				snap := e.DBs()[0].NewSnapshot()
				it, err := newArchiveIter(context.Background(), snap, nil)
				if err != nil {
					t.Fatal(err)
				}
				type entry struct {
					meta    *RollupMetadata
					records []sealrollup.Record
				}
				// Background maintenance may publish idle-closure or expiry parts
				// between two observations, so group by part.
				parts := map[v2part.PartID]map[string]*entry{}
				for valid := it.First(); valid; valid = it.Next() {
					key := it.Key()
					if len(key) < 4 || string(key[:4]) != "GCP\x03" {
						continue
					}
					k, err := v2part.Decode(key)
					if err != nil {
						t.Fatal(err)
					}
					if s, ok := seq[k.Part]; ok && s != int(epoch) {
						continue
					}
					seq[k.Part] = int(epoch)
					n := string(k.Metric)
					// Monthly partitions of one seal are one logical part.
					logical := k.Part
					binary.BigEndian.PutUint64(logical[:8], binary.BigEndian.Uint64(k.Part[:8])^uint64(k.Partition))
					entries := parts[logical]
					if entries == nil {
						entries = map[string]*entry{}
						parts[logical] = entries
					}
					en := entries[n]
					if en == nil {
						en = &entry{}
						entries[n] = en
					}
					if k.Archive == v2part.MetadataArchive {
						m, err := DecodeRollupMetadata(k.Metric, k.Group, it.Value())
						if err != nil {
							t.Fatal(err)
						}
						en.meta = &m
						continue
					}
					rs, err := v2part.DecodeSlots(k.Archive, k.BlockStart, it.Value())
					if err != nil {
						t.Fatal(err)
					}
					en.records = append(en.records, rs...)
				}
				if err = it.Close(); err != nil {
					t.Fatal(err)
				}
				snap.Close()

				// Order the parts published since the last observation. A real part
				// carries its publication sequence; here the newest envelope stands
				// in for it, which only matters when maintenance published extra parts.
				type published struct {
					entries map[string]*entry
					newest  metricMetadata
				}
				var order []published
				for _, entries := range parts {
					var p published
					p.entries = entries
					for _, en := range entries {
						if en.meta != nil && compactMetadata(*en.meta).after(p.newest) {
							p.newest = compactMetadata(*en.meta)
						}
					}
					order = append(order, p)
				}
				sort.Slice(order, func(i, j int) bool { return order[j].newest.after(order[i].newest) })
				if len(order) > 1 {
					tl.ExtraParts += len(order) - 1
				}
				for _, p := range order {
					entries := p.entries
					keys := make([]string, 0, len(entries))
					for n := range entries {
						keys = append(keys, n)
					}
					sort.Strings(keys)
					for _, n := range keys {
						en := entries[n]
						if en.meta == nil {
							t.Fatalf("epoch %d %s: data without envelope", epoch, n)
						}
						m := *en.meta
						tl.Checked++
						cfgNow := current[n]
						var stored sealrollup.Config
						if err := json.Unmarshal(m.Policy, &struct{ Config *sealrollup.Config }{&stored}); err != nil {
							t.Fatal(err)
						}
						d := state[n]
						if d == nil {
							d = &derived{}
							state[n] = d
						}
						where := fmt.Sprintf("epoch %d %s", epoch, n)
						if len(en.records) == 0 {
							tl.EnvelopeOnly++
							switch {
							case m.Expired || m.ResetOrder != 0 && !d.prev.Expired && d.prev.ResetOrder != m.ResetOrder:
								tl.EnvelopeOnlyExpired++
								example("envelope-only/expired", "%s: Expired=%v ResetOrder=%d, no data runs", where, m.Expired, m.ResetOrder)
							case !d.seen:
								tl.EnvelopeOnlyNewName++
								example("envelope-only/new-name", "%s: name exists (Revision %d, LastUpdate %d) but no point landed in any archive", where, m.Revision, m.LastUpdate)
							default:
								tl.EnvelopeOnlyOther++
								example("envelope-only/other", "%s: Order %d -> %d, MaxTime %d -> %d, no data runs (points past every retention)", where, d.prev.Frontier.Order, m.Frontier.Order, d.prev.Frontier.MaxTime, m.Frontier.MaxTime)
							}
						}
						// MaxTime: newest record time over the name's runs so far.
						var recMax int64
						var recOrder uint64
						var top sealrollup.Record
						for _, r := range en.records {
							recMax = max(recMax, r.Sample.Time)
							if r.Order > recOrder {
								recOrder, top = r.Order, r
							}
						}
						d.maxTime = max(d.maxTime, recMax)
						s0 := stored.Archives[0].Step
						switch {
						case d.maxTime == m.Frontier.MaxTime:
							tl.MaxTimeExact++
						case d.maxTime == m.Frontier.MaxTime-m.Frontier.MaxTime%s0:
							tl.MaxTimeAligned++
							example("maxtime/aligned-only", "%s: envelope %d, slots give %d (same %ds slot)", where, m.Frontier.MaxTime, d.maxTime, s0)
						default:
							tl.MaxTimeBad++
							example("maxtime/bad", "%s: envelope %d, slots give %d", where, m.Frontier.MaxTime, d.maxTime)
						}
						if len(en.records) > 0 {
							if m.Frontier.Order == recOrder {
								tl.OrderEqualsRecordMax++
							} else {
								tl.OrderDiffers++
								example("order/differs", "%s: envelope Order %d, max record order %d (archive %d, time %d, correction %v)", where, m.Frontier.Order, recOrder, top.Archive, top.Sample.Time, top.Correction)
							}
							d.lastUpdate = partClock * 1e9
							d.parts++
						}
						if d.seen && !compactMetadata(m).after(compactMetadata(d.prev)) {
							tl.AfterViolations++
							example("after/violation", "%s: newer part's envelope is not after() the older one", where)
						}
						if d.lastUpdate != m.LastUpdate {
							tl.LastUpdateBad++
							example("lastupdate/bad", "%s: envelope %d, part clock gives %d", where, m.LastUpdate, d.lastUpdate)
						}
						if d.parts != m.Revision {
							tl.RevisionBad++
							example("revision/bad", "%s: envelope %d, parts with data %d", where, m.Revision, d.parts)
						}
						if want, _ := sealrollup.PolicyDigest(cfgNow); want != m.Frontier.Policy {
							tl.PolicyBad++
							example("policy/bad", "%s: stored xFF %.1f, current rules xFF %.1f", where, stored.XFilesFactor, cfgNow.XFilesFactor)
						}
						if m.Expired || m.ResetOrder != 0 {
							tl.ResetBad++
							example("reset/not-in-data", "%s: Expired=%v ResetOrder=%d", where, m.Expired, m.ResetOrder)
						}
						d.prev, d.seen = m, true
					}
				}
				if epoch == 17 || epoch == 31 {
					if err = e.Close(); err != nil {
						t.Fatal(err)
					}
					if e, err = Open(root, cfg, hooks); err != nil {
						t.Fatal(err)
					}
				}
			}
			if tl.AfterViolations != 0 {
				t.Errorf("part sequence disagrees with after(): %d", tl.AfterViolations)
			}
			if sc.history {
				// Aligned, single-clock corpus: everything but Revision derives.
				if tl.MaxTimeBad+tl.MaxTimeAligned+tl.ThroughBad+tl.LastUpdateBad+tl.PolicyBad+tl.ResetBad+tl.EnvelopeOnly != 0 {
					t.Errorf("history corpus expected fully derivable except Revision: %+v", *tl)
				}
			}
			var lines []string
			for k, v := range tl.Examples {
				lines = append(lines, k+": "+v)
			}
			sort.Strings(lines)
			t.Logf("%s: %+v\n%s", sc.name, func() tally { c := *tl; c.Examples = nil; return c }(), strings.Join(lines, "\n"))
		})
	}
	if out := os.Getenv("DERIVED_ORACLE_OUT"); out != "" {
		b, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(out, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestDerivedStateOracleMerged checks the restart/recovery view with background
// merges on: per name, the newest envelope across every live part (what
// scanRollups keeps) against state derived from all live runs.
func TestDerivedStateOracleMerged(t *testing.T) {
	_, policy := rollupTestPolicy(t, "merged.metric", "10s:6h,5m:7d,1h:1y,1d:10y", sealrollup.Average, .1)
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups, cfg.MaxRows = 1, 1, true, 1024
	root := t.TempDir()
	hooks := Hooks{Now: func() time.Time { return time.Unix(1, 0) }}
	e, err := Open(root, cfg, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if e != nil {
			_ = e.Close()
		}
	}()
	now := int64(1794700800)
	var order uint64
	var exact, aligned, bad, throughBad, minParts, maxParts int
	minParts = math.MaxInt
	for epoch := int64(0); epoch < 60; epoch++ {
		now += 600
		order++
		var rows []Row
		for j := int64(0); j < 60; j++ {
			rows = append(rows, Row{ID: 1, Time: now - 600 + j*10 + 3, Bits: math.Float64bits(float64(j)), Seq: uint64(j + 1)})
		}
		for _, age := range []int64{120, 1800, 3500, 7200, 18000, 100000} {
			rows = append(rows, Row{ID: 1, Time: now - age, Bits: 1, Seq: 100}, Row{ID: 1, Time: now - age, Bits: 2, Seq: 101})
		}
		if err = e.AppendBatches(0, []Batch{{ID: 1, Policy: policy, Order: order, Clock: now, Points: rows}}, order, true); err != nil {
			t.Fatal(err)
		}
		snap := e.DBs()[0].NewSnapshot()
		it, err := newArchiveIter(context.Background(), snap, nil)
		if err != nil {
			t.Fatal(err)
		}
		var newest RollupMetadata
		var recMax int64
		parts := map[v2part.PartID]bool{}
		for valid := it.First(); valid; valid = it.Next() {
			key := it.Key()
			if len(key) < 4 || string(key[:4]) != "GCP\x03" {
				continue
			}
			k, err := v2part.Decode(key)
			if err != nil {
				t.Fatal(err)
			}
			if k.Archive == v2part.MetadataArchive {
				m, err := DecodeRollupMetadata(k.Metric, k.Group, it.Value())
				if err != nil {
					t.Fatal(err)
				}
				logical := k.Part
				binary.BigEndian.PutUint64(logical[:8], binary.BigEndian.Uint64(k.Part[:8])^uint64(k.Partition))
				parts[logical] = true
				if compactMetadata(m).after(compactMetadata(newest)) {
					newest = m
				}
				continue
			}
			rs, err := v2part.DecodeSlots(k.Archive, k.BlockStart, it.Value())
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rs {
				recMax = max(recMax, r.Sample.Time)
			}
		}
		if err = it.Close(); err != nil {
			t.Fatal(err)
		}
		snap.Close()
		minParts, maxParts = min(minParts, len(parts)), max(maxParts, len(parts))
		switch {
		case recMax == newest.Frontier.MaxTime:
			exact++
		case recMax == newest.Frontier.MaxTime-newest.Frontier.MaxTime%10:
			aligned++
		default:
			bad++
			t.Logf("epoch %d: newest envelope MaxTime %d, live runs give %d", epoch, newest.Frontier.MaxTime, recMax)
		}
		if false {
			throughBad++
		}
		if epoch == 23 || epoch == 41 {
			if err = e.Close(); err != nil {
				t.Fatal(err)
			}
			if e, err = Open(root, cfg, hooks); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("merged: seals 60, live parts %d..%d, MaxTime exact %d aligned %d bad %d, ClosedThrough != MaxTime %d", minParts, maxParts, exact, aligned, bad, throughBad)
	if bad != 0 || throughBad != 0 {
		t.Errorf("merged state not derivable: bad %d throughBad %d", bad, throughBad)
	}
	if maxParts >= 60 {
		t.Errorf("no merge happened: %d live parts", maxParts)
	}
}
