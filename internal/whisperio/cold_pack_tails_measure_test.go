//go:build darwin || linux

package whisperio

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/sstable"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type tailStock struct {
	SSTBytes, DataBytes, Tombstones    uint64
	OrdinaryPhysical, ColdTailPhysical float64
}
type tailMeasurement struct {
	Family                                                                       string
	Retentions                                                                   []store.Retention
	Metrics                                                                      int
	AgesHours                                                                    []int
	Before, Control, After                                                       tailStock
	PackedBlocks, PackedPoints, InputBytes, OutputBytes, ReadBytes               uint64
	CPUNanos                                                                     int64
	CPUNanosPerInputByte, SettledSavingPct                                       float64
	ResumedSeries, AppendPoints, ThawedUnexpired, ReusedExpired, PreservedPacked int
	ResumeCPUNanos                                                               int64
}

func tailCPU(t *testing.T) int64 {
	t.Helper()
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		t.Fatal(err)
	}
	return (u.Utime.Sec+u.Stime.Sec)*1e9 + int64(u.Utime.Usec+u.Stime.Usec)*1000
}
func TestColdPackTailsMeasurement(t *testing.T) {
	familyName := os.Getenv("COLD_TAILS_MEASURE")
	if familyName == "" {
		t.Skip("opt-in settled SST measurement")
	}
	family := map[string]int{"integer": 0, "float": 1, "constant": 2}[familyName]
	now := time.Unix(1793700000, 0)
	ctx := context.Background()
	cfg := store.MetricConfig{Retentions: []store.Retention{{Step: 1, Count: 86400}, {Step: 10, Count: 25920}, {Step: 60, Count: 20160}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average, XFilesFactor: .5}
	ages := []int{0, 6, 23, 25, 36, 60, 96, 60}
	codec := &tailSeedCodec{ContinuationCodec: ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true}, seeds: map[string]mapContinuationIO{}}
	for i, age := range ages {
		cfg.Name = fmt.Sprintf("tails.measure.%02d", i)
		limit := 90000
		if i == 7 {
			limit = 64
		}
		if family == 2 {
			limit = min(limit, 12000)
		}
		codec.seeds[cfg.Name] = tailSeed(t, codec, cfg, now, time.Duration(age)*time.Hour, limit, false, family)
	}
	opts := coldOptions(codec)
	opts.TimeBuckets = false
	opts.Now = func() time.Time { return now }
	opts.CacheSize = 16 << 20
	opts.MemTableSize = 8 << 20
	opts.RelayoutMinFreeBytes = 1
	opts.RelayoutMaxCompactionDebt = 1 << 40
	path := filepath.Join(t.TempDir(), "store")
	db := tailOpen(t, path, opts)
	defer func() { _ = db.Close() }()
	names := []string{}
	for i := range 16 {
		cfg.Name = fmt.Sprintf("tails.measure.%02d", i)
		codec.seeds[cfg.Name] = codec.seeds[fmt.Sprintf("tails.measure.%02d", i%8)]
		names = append(names, cfg.Name)
		if _, err := db.Create(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	}
	db = tailRelayout(t, db, path, &opts)
	settle := func(d *store.Store) {
		t.Helper()
		for _, n := range []string{"a.settle", "z.settle"} {
			c := cfg
			c.Name = n
			if _, err := d.Create(ctx, c); err != nil {
				t.Fatal(err)
			}
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		for _, n := range []string{"a.settle", "z.settle"} {
			if err := d.Delete(ctx, n); err != nil {
				t.Fatal(err)
			}
		}
		for range 7 {
			if err := d.Compact(); err != nil {
				t.Fatal(err)
			}
		}
	}
	stock := func(d *store.Store, label string) tailStock {
		t.Helper()
		s := tailStock{}
		cp := filepath.Join(t.TempDir(), "checkpoint")
		if err := d.Checkpoint(cp); err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(cp)
		files, err := filepath.Glob(filepath.Join(cp, "*.sst"))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range files {
			stat, err := os.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			s.SSTBytes += uint64(stat.Size())
			f, err := os.Open(p)
			if err != nil {
				t.Fatal(err)
			}
			r, err := sstable.NewSimpleReadable(f)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := sstable.NewReader(r, sstable.ReaderOptions{}, sstable.Mergers{"go-carbon.chunks.exact.v1": nil})
			if err != nil {
				t.Fatal(err)
			}
			s.Tombstones += reader.Properties.NumDeletions
			layout, err := reader.Layout()
			if err != nil {
				t.Fatal(err)
			}
			for _, b := range layout.Data {
				s.DataBytes += b.Length + 5
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if bin := os.Getenv("COLD_TAILS_INVENTORY"); bin != "" {
			out := filepath.Join(t.TempDir(), "inventory.json")
			cmd := exec.CommandContext(ctx, bin, "inventory", "-sst-dir", cp, "-read-mb-s", "0", "-as-of", now.UTC().Format(time.RFC3339), "-out", out)
			if data, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("inventory: %v %s", err, data)
			}
			data, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			if dest := os.Getenv("COLD_TAILS_RESULT"); dest != "" {
				if err := os.WriteFile(dest+"-"+label+"-inventory.json", data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			var inv struct {
				Classes  map[string]struct{ PhysicalBytes float64 }
				Ordinary map[string]struct {
					Layout, Position, Temperature string
					PhysicalBytes                 float64
				}
			}
			if err := json.Unmarshal(data, &inv); err != nil {
				t.Fatal(err)
			}
			s.OrdinaryPhysical = inv.Classes["ordinary"].PhysicalBytes
			for _, row := range inv.Ordinary {
				if row.Layout == "metric-prefixed" && row.Position == "newest-observed/open-candidate" && row.Temperature == "cold" {
					s.ColdTailPhysical += row.PhysicalBytes
				}
			}
		}
		if s.Tombstones != 0 {
			t.Fatalf("unsettled stock: %+v", s)
		}
		return s
	}
	oracle := func(d, c *store.Store) {
		t.Helper()
		for _, name := range names {
			a, err := d.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			b, err := c.Snapshot(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, a, b, false)
		}
	}
	tailSweep(t, db, false, "exact")
	settle(db)
	receipt := tailMeasurement{Family: familyName, Retentions: cfg.Retentions, Metrics: len(names), AgesHours: ages, Before: stock(db, "before")}
	controlPath := filepath.Join(t.TempDir(), "control")
	if err := db.Checkpoint(controlPath); err != nil {
		t.Fatal(err)
	}
	control := tailOpen(t, controlPath, opts)
	defer control.Close()
	tailSweep(t, control, false, "exact")
	settle(control)
	receipt.Control = stock(control, "control")
	old := map[string]map[string]blockDescriptor{}
	for _, n := range names {
		old[n] = tailDescriptors(t, db, n)
	}
	st := db.ColdPackStats()
	start := tailCPU(t)
	tailSweep(t, db, true, "exact")
	receipt.CPUNanos = tailCPU(t) - start
	end := db.ColdPackStats()
	receipt.PackedBlocks = end.Blocks - st.Blocks
	receipt.PackedPoints = end.Points - st.Points
	receipt.InputBytes = end.InputBytes - st.InputBytes
	receipt.OutputBytes = end.OutputBytes - st.OutputBytes
	receipt.ReadBytes = end.ReadBytes - st.ReadBytes
	if receipt.InputBytes == 0 {
		t.Fatal("no tails packed")
	}
	receipt.CPUNanosPerInputByte = float64(receipt.CPUNanos) / float64(receipt.InputBytes)
	settle(db)
	receipt.After = stock(db, "after")
	receipt.SettledSavingPct = 100 * (1 - float64(receipt.After.SSTBytes)/float64(receipt.Control.SSTBytes))
	oracle(db, control)
	st = db.ColdPackStats()
	tailSweep(t, db, true, "exact")
	if db.ColdPackStats().Blocks != st.Blocks {
		t.Fatal("repeat repacked unchanged image")
	}
	// Record only newly packed, formerly metric-prefixed tails.
	type packed struct {
		name, key string
		last      int64
		archive   int
	}
	packedTails := []packed{}
	for _, name := range names {
		kv := tailKeys(t, db, name)
		for key, d := range tailDescriptors(t, db, name) {
			if old[name][key].bucketed || d.packed == 0 {
				continue
			}
			id := binary.BigEndian.Uint64([]byte(key)[1:9])
			a := int(binary.BigEndian.Uint16([]byte(key)[10:12]))
			b := int(binary.BigEndian.Uint32([]byte(key)[12:]))
			value := kv[string(d.groupKey(id, a, b, int((d.count-1)/d.packed)))]
			points, err := store.DecodeSealedPoints(value)
			if err != nil {
				t.Fatal(err)
			}
			packedTails = append(packedTails, packed{name, key, points[len(points)-1].Timestamp, a})
		}
	}
	if uint64(len(packedTails)) != receipt.PackedBlocks {
		t.Fatal("extra non-tail packing in measured pass")
	}
	start = tailCPU(t)
	for batch := range 4 {
		now = now.Add(960 * time.Second)
		for i, name := range names {
			if i%8 != 0 && i%8 != 4 {
				continue
			}
			if batch == 0 && i%8 == 4 {
				receipt.ResumedSeries++
			}
			input := make([]store.Point, 960)
			for j := range input {
				input[j] = store.Point{Timestamp: now.Unix() - int64(960-j), Value: float64(800000 + batch*1000 + j)}
			}
			for _, d := range []*store.Store{db, control} {
				if err := d.UpdateMany(ctx, name, input); err != nil {
					t.Fatal(err)
				}
			}
			receipt.AppendPoints += len(input)
		}
	}
	receipt.ResumeCPUNanos = tailCPU(t) - start
	oracle(db, control)
	for _, p := range packedTails {
		d := tailDescriptors(t, db, p.name)[p.key]
		if d.packed != 0 {
			receipt.PreservedPacked++
			continue
		}
		r := cfg.Retentions[p.archive]
		if p.last < now.Unix()-int64(r.Step)*int64(r.Count) {
			receipt.ReusedExpired++
		} else {
			receipt.ThawedUnexpired++
		}
	}
	resumedHeads := map[string]map[string]blockDescriptor{}
	for i, name := range names {
		if i%8 == 0 || i%8 == 4 {
			resumedHeads[name] = tailDescriptors(t, db, name)
		}
	}
	tailSweep(t, db, true, "exact")
	for name, descs := range resumedHeads {
		for key, d := range tailDescriptors(t, db, name) {
			if descs[key].packed == 0 && d.packed != 0 {
				t.Fatal("active/resumed head resealed", name)
			}
		}
	}
	oracle(db, control)
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
	if p := os.Getenv("COLD_TAILS_RESULT"); p != "" {
		if err := os.WriteFile(p, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
