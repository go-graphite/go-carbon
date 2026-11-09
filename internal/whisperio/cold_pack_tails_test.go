package whisperio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	whisper "github.com/go-graphite/go-whisper"
)

type tailSeedCodec struct {
	ContinuationCodec
	seeds map[string]mapContinuationIO
}

func (c *tailSeedCodec) Create(ctx context.Context, kv store.ContinuationIO, m store.Metadata, now func() time.Time) error {
	if seed, ok := c.seeds[m.Name]; ok {
		return seed.Visit(nil, kv.Set)
	}
	return c.ContinuationCodec.Create(ctx, kv, m, now)
}

// Real CWhisper geometry and headers, imported in the pre-relayout layout.
func tailSeed(t *testing.T, codec *tailSeedCodec, cfg store.MetricConfig, now time.Time, age time.Duration, countLimit int, bufferActive bool, family int) mapContinuationIO {
	t.Helper()
	defs := make(whisper.Retentions, len(cfg.Retentions))
	for i, r := range cfg.Retentions {
		rdef := whisper.NewRetention(int(r.Step), int(r.Count))
		defs[i] = &rdef
	}
	path := filepath.Join(t.TempDir(), "seed.wsp")
	seedNow := now.Add(-age)
	clock := func() time.Time { return seedNow }
	src, err := whisper.CreateWithOptions(path, defs, whisper.Average, cfg.XFilesFactor, &whisper.Options{Compressed: true, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	for a, r := range cfg.Retentions {
		count := min(int(r.Count), countLimit)
		end := now.Add(-age).Unix() / int64(r.Step) * int64(r.Step)
		points := make([]whisper.TimeSeriesPoint, count)
		ptr := make([]*whisper.TimeSeriesPoint, count)
		state := uint64(8144 + a)
		for i := range points {
			state ^= state << 13
			state ^= state >> 7
			state ^= state << 17
			value := float64(1000 + i*3 + int(state%8))
			if family == 1 {
				value = float64(state>>11) * 0x1p-53 * 1000
			}
			if family == 3 {
				value = float64(state%4) / 3
			}
			if family == 2 {
				value = 42
			}
			points[i] = whisper.TimeSeriesPoint{Time: int(end - int64(count-i)*int64(r.Step)), Value: value}
			ptr[i] = &points[i]
		}
		if err := src.UpdateManyForArchive(ptr, src.Retentions()[a].MaxRetention()); err != nil {
			t.Fatal(err)
		}
	}
	if bufferActive {
		seedNow = now
		if err := src.UpdateMany([]*whisper.TimeSeriesPoint{{Time: int(now.Unix()) - 60, Value: 123}, {Time: int(now.Unix()) - 120, Value: 124}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	seed := mapContinuationIO{}
	if err := codec.Import(context.Background(), seed, path, store.Metadata{MetricConfig: cfg}, func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	return seed
}
func tailSweep(t *testing.T, db *store.Store, tails bool, mode string) {
	t.Helper()
	if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: mode, PackTails: tails, WriteWindow: 24 * time.Hour, CPUFraction: 1, BytesPerSecond: 1 << 40, MaxCompactionDebt: 1 << 40, MinFreeBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if err := db.RunColdPack(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := db.ColdPackStats(); st.Errors != 0 || st.Skipped != 0 {
		t.Fatalf("sweep: %+v", st)
	}
}
func tailKeys(t *testing.T, db *store.Store, name string) mapContinuationIO {
	t.Helper()
	r, err := db.ExportTransfer(context.Background(), []string{name})
	if err != nil {
		t.Fatal(err)
	}
	kv := mapContinuationIO{}
	for _, p := range r[0].Keys {
		kv[string(p.Key)] = bytes.Clone(p.Value)
	}
	return kv
}
func tailDescriptors(t *testing.T, db *store.Store, name string) map[string]blockDescriptor {
	t.Helper()
	out := map[string]blockDescriptor{}
	for k, v := range tailKeys(t, db, name) {
		if len(k) == 16 && k[0] == 'f' && k[9] == 'b' {
			d, err := decodeBlockDescriptor(v)
			if err != nil {
				t.Fatal(err)
			}
			out[k] = d
		}
	}
	return out
}
func tailOpen(t *testing.T, path string, opts store.Options) *store.Store {
	t.Helper()
	db, err := store.Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	return db
}
func tailRelayout(t *testing.T, db *store.Store, path string, opts *store.Options) *store.Store {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.TimeBuckets = true
	db = tailOpen(t, path, *opts)
	if err := db.RelayoutContinuation(context.Background(), 1<<40); err != nil {
		t.Fatal(err)
	}
	return db
}
func TestColdPackTailsOracle(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(fmt.Sprint(direct), func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1793700000, 0)
			codec := &tailSeedCodec{ContinuationCodec: ContinuationCodec{DirectLateWrites: direct, ReuseBlockKeys: true}, seeds: map[string]mapContinuationIO{}}
			cfg := store.MetricConfig{Name: "tails.stopped", Retentions: []store.Retention{{Step: 60, Count: 43200}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average, XFilesFactor: .5}
			codec.seeds[cfg.Name] = tailSeed(t, codec, cfg, now, 48*time.Hour, 5000, false, 0)
			opts := coldOptions(codec)
			opts.TimeBuckets = false
			opts.Now = func() time.Time { return now }
			opts.RelayoutMinFreeBytes = 1
			path := t.TempDir()
			db := tailOpen(t, path, opts)
			defer func() { _ = db.Close() }()
			if _, err := db.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			db = tailRelayout(t, db, path, &opts)
			want, err := db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			tailSweep(t, db, false, "exact")
			before := tailDescriptors(t, db, cfg.Name)
			n := 0
			for _, d := range before {
				if !d.bucketed && d.count > 0 {
					n++
				}
			}
			if n != len(cfg.Retentions) {
				t.Fatalf("relayout left %d tails", n)
			}
			st := db.ColdPackStats()
			tailSweep(t, db, true, "exact")
			if db.TransferFormat() != 9 || db.ColdPackStats().Blocks-st.Blocks != uint64(n) {
				t.Fatalf("tails not packed in v9: %+v", db.ColdPackStats())
			}
			for k, d := range tailDescriptors(t, db, cfg.Name) {
				if !before[k].bucketed && (!d.bucketed || d.bucket != continuationHistoryBucket || d.packed != store.ColdPackPoints) {
					t.Fatal("wrong tail descriptor", d)
				}
			}
			marker, err := os.ReadFile(filepath.Join(path, "CHUNKSTORE"))
			if err != nil || string(marker) != "go-carbon-chunks-v9\n" {
				t.Fatal(string(marker), err)
			}
			got, err := db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, want, got, false)
			st = db.ColdPackStats()
			tailSweep(t, db, true, "exact")
			if db.ColdPackStats().Blocks != st.Blocks {
				t.Fatal("repeat packed unchanged image")
			}
			// Transfer to a reader with maintenance disabled preserves the same v9 data.
			r, err := db.ExportTransfer(ctx, []string{cfg.Name})
			if err != nil {
				t.Fatal(err)
			}
			targetOpts := opts
			targetOpts.ColdPack = store.ColdPackOptions{Mode: "exact"}
			target := tailOpen(t, t.TempDir(), targetOpts)
			defer target.Close()
			if _, err := target.ImportTransfer(ctx, r); err != nil {
				t.Fatal(err)
			}
			got, err = target.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, want, got, false)
			// Both direct corrections and sidecar folding use the existing thaw path.
			correction := []store.Point{{Timestamp: now.Add(-49 * time.Hour).Unix(), Value: 777}}
			control := tailOpen(t, t.TempDir(), opts)
			defer control.Close()
			if _, err := control.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			if err := db.UpdateMany(ctx, cfg.Name, correction); err != nil {
				t.Fatal(err)
			}
			if err := control.UpdateMany(ctx, cfg.Name, correction); err != nil {
				t.Fatal(err)
			}
			resumed := []store.Point{{Timestamp: now.Unix() - 60, Value: 1234}, {Timestamp: now.Unix() - 120, Value: 2345}}
			for _, d := range []*store.Store{db, control} {
				if err := d.UpdateMany(ctx, cfg.Name, resumed); err != nil {
					t.Fatal(err)
				}
			}
			want, err = control.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			got, err = db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, want, got, false)
			// Circular reuse and long gaps retain parity with the ordinary writer.
			now = now.Add(40 * 24 * time.Hour)
			for _, d := range []*store.Store{db, control} {
				if err := d.UpdateMany(ctx, cfg.Name, []store.Point{{Timestamp: now.Unix() - 60, Value: 999}}); err != nil {
					t.Fatal(err)
				}
			}
			want, err = control.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			got, err = db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, want, got, false)
			tailSweep(t, db, false, "unpack")
			if err := db.DowngradeColdPack(ctx); err != nil {
				t.Fatal(err)
			}
			if db.TransferFormat() != 7 {
				t.Fatal("did not downgrade")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = tailOpen(t, path, opts)
			got, err = db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, want, got, false)
		})
	}
}
func TestColdPackTailsProtectBufferedHead(t *testing.T) {
	for _, bucketed := range []bool{false, true} {
		t.Run(fmt.Sprint(bucketed), func(t *testing.T) {
			now := time.Unix(1793700000, 0)
			codec := &tailSeedCodec{seeds: map[string]mapContinuationIO{}}
			cfg := store.MetricConfig{Name: "tails.buffer", Retentions: []store.Retention{{Step: 60, Count: 43200}, {Step: 1800, Count: 35040}}, AggregationMethod: store.Average, XFilesFactor: .5}
			seed := tailSeed(t, codec, cfg, now, 48*time.Hour, 1000, true, 0)
			if bucketed {
				// Model the ordinary bucketed head left by a resumed/thawed packed tail.
				for key, raw := range seed {
					if len(key) != 16 || key[9] != 'b' || binary.BigEndian.Uint16([]byte(key)[10:12]) != 1 {
						continue
					}
					d, err := decodeBlockDescriptor(raw)
					if err != nil {
						t.Fatal(err)
					}
					prefix := key[:9] + "c" + key[10:]
					for k, v := range seed {
						if len(k) == 20 && k[:16] == prefix {
							seed[string(store.ContinuationBucketKey(continuationHistoryBucket, []byte(k)))] = v
							delete(seed, k)
						}
					}
					d.bucketed = true
					d.bucket = continuationHistoryBucket
					seed[key] = d.encode()
				}
			}
			codec.seeds[cfg.Name] = seed
			fs := newContinuationFiles(seed, true)
			src, err := whisper.OpenWithOptions(fs.main(), continuationOptions(fs, func() time.Time { return now }))
			if err != nil {
				t.Fatal(err)
			}
			physical, err := src.ContinuationLayout()
			if err != nil {
				t.Fatal(err)
			}
			bounds := append(src.StoredArchiveBounds(), src.BufferedRollupWindows()...)
			_ = src.Close()
			last := 0
			for _, b := range physical.Blocks {
				if b.Archive == 1 {
					last = max(last, b.Last)
				}
			}
			until := 0
			for _, b := range bounds {
				if b.Archive == 1 {
					until = max(until, b.Until)
				}
			}
			if int64(last) >= now.Add(-24*time.Hour).Unix() || int64(until) < now.Add(-24*time.Hour).Unix() {
				t.Fatalf("fixture lacks cold native tail behind hot buffer: last=%d until=%d", last, until)
			}
			opts := coldOptions(codec)
			opts.TimeBuckets = false
			opts.Now = func() time.Time { return now }
			opts.RelayoutMinFreeBytes = 1
			path := t.TempDir()
			db := tailOpen(t, path, opts)
			defer func() { _ = db.Close() }()
			if _, err := db.Create(context.Background(), cfg); err != nil {
				t.Fatal(err)
			}
			db = tailRelayout(t, db, path, &opts)
			before := tailDescriptors(t, db, cfg.Name)
			tailSweep(t, db, true, "exact")
			for k, d := range tailDescriptors(t, db, cfg.Name) {
				if binary.BigEndian.Uint16([]byte(k)[10:12]) == 1 && before[k].packed == 0 && d.packed != 0 {
					t.Fatal("sealed active buffered head")
				}
			}
		})
	}
}

func TestColdPackTailsWindowAndShortRange(t *testing.T) {
	for _, age := range []time.Duration{0, 24 * time.Hour, 24*time.Hour + time.Minute} {
		t.Run(age.String(), func(t *testing.T) {
			now := time.Unix(1793700000, 0)
			ctx := context.Background()
			cfg := store.MetricConfig{Name: "tails.short", Retentions: []store.Retention{{Step: 60, Count: 43200}}, AggregationMethod: store.Average}
			codec := &tailSeedCodec{seeds: map[string]mapContinuationIO{}}
			codec.seeds[cfg.Name] = tailSeed(t, codec, cfg, now, age, 64, false, 2)
			opts := coldOptions(codec)
			opts.TimeBuckets = false
			opts.Now = func() time.Time { return now }
			opts.RelayoutMinFreeBytes = 1
			path := t.TempDir()
			db := tailOpen(t, path, opts)
			defer func() { _ = db.Close() }()
			if _, err := db.Create(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			db = tailRelayout(t, db, path, &opts)
			before, err := db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			tailSweep(t, db, false, "exact")
			if db.ColdPackStats().Blocks != 0 {
				t.Fatal("default packed short tail")
			}
			tailSweep(t, db, true, "exact")
			packed := db.ColdPackStats().Blocks
			if age <= 24*time.Hour && packed != 0 || age > 24*time.Hour && packed != 1 {
				t.Fatalf("window/short guard age=%s packed=%d", age, packed)
			}
			after, err := db.Snapshot(ctx, cfg.Name)
			if err != nil {
				t.Fatal(err)
			}
			coldEqual(t, before, after, false)
		})
	}
}
