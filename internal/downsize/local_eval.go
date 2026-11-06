// bulk-copy-eval compares checkpoint export/import with the live-source transfer
// oracle on captured Whisper archives. It only opens private local databases.
package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type bulkEvalFixture struct{ Name, Source, Target string }
type result struct {
	Mode                                                               string
	Metrics, WireBytes                                                 int64
	Seconds, ExportSeconds, ImportSeconds, NamesPerSecond, MBPerSecond float64
}

func BulkEvalMain(args []string) error {
	flags := flag.NewFlagSet("resize bulk-eval", flag.ContinueOnError)
	fixtures := flags.String("fixtures", "", "JSON list of name/source/target paths; paths relative to the JSON file")
	sampleNames := flags.Int("sample-names", 0, "uniform name subset of a native capture for the differential oracle; full stream integrity is still checked (0=all)")
	capturePath := flags.String("native-capture", "", "capture-native metadata; retains production native block values/layout")
	parent := flags.String("scratch", "", "existing private scratch directory")
	repeat := flags.Int("repeat", 1, "repeat captured histories under distinct names (reported as replication)")
	batch := flags.Int("batch", 512, "metrics per measured import")
	workers := flags.Int("workers", 1, "bounded parallel export/import workers")
	sourceCache := flags.Int64("source-cache-bytes", 1<<30, "private donor block-cache bytes; use production-sized shards for native export")
	sourceShards := flags.Int("source-cache-shards", 128, "private donor block-cache shards (0=store size-based default)")
	sourceScratch := flags.String("source-scratch", "", "optional existing private directory on a separate donor disk")
	single := flags.Bool("single-oracle", false, "reference imports one name at a time")
	overlapPercent := flags.Int("overlap-percent", 0, "seed this percentage of survivor names from captured donor histories, then add survivor corrections (0 uses captured targets)")
	profilePrefix := flags.String("profile-prefix", "", "write CPU/mutex/block profiles of the checkpoint phase under this local prefix")
	native := flags.Bool("native", false, "measure the native owning-daemon SST engine; deferred combines are outside timing")
	combine := flags.Bool("combine-existing", false, "include exact native combines of present names in native timing")
	memory := flags.Bool("memory", false, "private in-memory Pebble fixtures (not physical-disk throughput)")
	epoch := flags.Int64("now", time.Now().Unix(), "fixed evaluation clock, Unix seconds")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *combine && !*native {
		return errors.New("--combine-existing requires --native")
	}
	if *sourceCache < 1 || *sourceShards < 0 || *sampleNames < 0 || (*sampleNames > 0 && *capturePath == "") || (*fixtures == "" && *capturePath == "") || (*fixtures != "" && *capturePath != "") || *parent == "" || *repeat < 1 || *batch < 1 || *workers < 1 || *overlapPercent < 0 || *overlapPercent > 100 {
		return errors.New("invalid evaluation options")
	}
	var input []bulkEvalFixture
	var capture nativeCapture
	if *capturePath != "" {
		if !*native || *repeat != 1 {
			return errors.New("native capture requires --native --repeat 1")
		}
		data, err := os.ReadFile(*capturePath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &capture); err != nil {
			return err
		}
		if capture.StoreFormat < 6 || capture.StoreFormat > 11 || len(capture.SHA256) != 64 || filepath.Base(capture.WireFile) != capture.WireFile {
			return errors.New("invalid native capture metadata")
		}
		for _, name := range capture.Names {
			input = append(input, bulkEvalFixture{Name: name})
		}
	} else {
		data, err := os.ReadFile(*fixtures)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &input); err != nil {
			return err
		}
	}
	if len(input) == 0 {
		return errors.New("empty fixtures")
	}
	root, err := os.MkdirTemp(*parent, "bulk-eval-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	donorRoot := root
	if *sourceScratch != "" {
		donorRoot, err = os.MkdirTemp(*sourceScratch, "bulk-eval-source-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(donorRoot)
	}
	clock := func() time.Time { return time.Unix(*epoch, 0) }
	ctx := context.Background()
	open := func(name string) (*store.Store, error) {
		directory, cacheBytes, cacheShards := root, int64(32<<20), 0
		if name == "donor" {
			directory, cacheBytes, cacheShards = donorRoot, *sourceCache, *sourceShards
		}
		originals := filepath.Join(directory, name+"-originals")
		fs := vfs.Default
		if *memory {
			fs = vfs.NewMem()
		}
		if err := fs.MkdirAll(originals, 0700); err != nil {
			return nil, err
		}
		return store.Open(filepath.Join(directory, name), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: originals, Now: clock, CacheSize: cacheBytes, CacheShards: cacheShards, MemTableSize: 16 << 20, SyncInterval: time.Second, ColdPack: store.ColdPackOptions{Mode: "exact", FloatCodec: true}}.WithFS(fs))
	}
	donor, err := open("donor")
	if err != nil {
		return err
	}
	defer donor.Close()
	baseline, err := open("reference")
	if err != nil {
		return err
	}
	defer baseline.Close()
	bulk, err := open("checkpoint-target")
	if err != nil {
		return err
	}
	defer bulk.Close()
	if *capturePath != "" {
		file, err := os.Open(filepath.Join(filepath.Dir(*capturePath), capture.WireFile))
		if err != nil {
			return err
		}
		defer file.Close()
		hash := sha256.New()
		stream, err := store.NewTransferStream(io.TeeReader(file, hash))
		if err != nil {
			return err
		}
		if stream.Count != len(capture.Names) {
			return errors.New("native capture name count differs")
		}
		planned := make(map[string]bool, len(capture.Names))
		for _, name := range capture.Names {
			if planned[name] {
				return errors.New("duplicate capture name")
			}
			planned[name] = true
		}
		selected := make(map[string]bool)
		if *sampleNames > 0 && *sampleNames < len(capture.Names) {
			for i := range *sampleNames {
				selected[capture.Names[i*len(capture.Names) / *sampleNames]] = true
			}
		}
		seen := make(map[string]bool, len(capture.Names))
		copied := make(map[string]bool, len(capture.Names))
		var records []store.TransferMetric
		var bytesBuffered int
		flush := func() error {
			if len(records) == 0 {
				return nil
			}
			_, err := donor.ImportTransfer(ctx, records)
			records = nil
			bytesBuffered = 0
			return err
		}
		// Seed with the reviewed importer, not the engine under test: a shared
		// native seeding bug must not erase the same points from both oracle sides.
		for range stream.Count {
			record, err := stream.Next()
			if err != nil {
				return err
			}
			name := record.Config.Name
			if !planned[name] || seen[name] {
				return errors.New("native capture names differ")
			}
			seen[name] = true
			if record.Version.ID == 0 || (len(selected) > 0 && !selected[name]) {
				continue
			}
			copied[name] = true
			for i, kv := range record.Keys {
				record.Keys[i] = store.RelayoutKV{Key: bytes.Clone(kv.Key), Value: bytes.Clone(kv.Value)}
				bytesBuffered += len(kv.Key) + len(kv.Value)
			}
			records = append(records, record)
			if len(records) >= 512 || bytesBuffered >= 4<<20 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		_, count, err := stream.Finish()
		if err != nil {
			return err
		}
		if count != capture.Bytes || hex.EncodeToString(hash.Sum(nil)) != capture.SHA256 {
			return errors.New("native capture integrity mismatch")
		}
		if err := flush(); err != nil {
			return err
		}
		input = nil
		for _, name := range capture.Names {
			if copied[name] {
				input = append(input, bulkEvalFixture{Name: name})
			}
		}
		if len(input) == 0 {
			return errors.New("native capture contains no committed history")
		}
	}
	seed := func(db *store.Store, name, path string) error {
		snapshot, err := whisperio.ReadSnapshotIn(ctx, name, path, root)
		if err != nil {
			return err
		}
		m, err := db.StageFileSnapshot(ctx, snapshot)
		if err != nil {
			return err
		}
		m, err = db.PublishFileOwnership(ctx, snapshot, m)
		if err != nil {
			return err
		}
		return db.CompleteFileCleanup(ctx, m)
	}
	var names []string
	var capturedPoints, overlap int
	for repetition := 0; repetition < *repeat; repetition++ {
		for rowIndex, row := range input {
			name := row.Name
			if *repeat > 1 {
				name = fmt.Sprintf("bulk_eval.r%d.%s", repetition, name)
			}
			source := filepath.Join(filepath.Dir(*fixtures), row.Source)
			if err := func() error {
				if *capturePath != "" {
					return nil
				}
				return seed(donor, name, source)
			}(); err != nil {
				return fmt.Errorf("seed donor %s: %w", name, err)
			}
			names = append(names, name)
			if repetition == 0 {
				snapshot, err := donor.Snapshot(ctx, name)
				if err != nil {
					return err
				}
				for _, a := range snapshot.Archives {
					capturedPoints += len(a.Points)
				}
			}
			forced := rowIndex*100/len(input) < *overlapPercent
			if row.Target != "" || forced {
				overlap++
				path := filepath.Join(filepath.Dir(*fixtures), row.Target)
				if forced {
					path = source
				}
				for _, db := range []*store.Store{baseline, bulk} {
					if err := func() error {
						if *capturePath == "" {
							return seed(db, name, path)
						}
						records, err := donor.ExportTransfer(ctx, []string{name})
						if err != nil {
							return err
						}
						_, err = db.ImportTransfer(ctx, records)
						return err
					}(); err != nil {
						return err
					}
					if forced {
						snapshot, err := db.Snapshot(ctx, name)
						if err != nil {
							return err
						}
						if len(snapshot.Archives[0].Points) > 0 {
							point := snapshot.Archives[0].Points[len(snapshot.Archives[0].Points)-1]
							point.Value = 123.456
							if err := db.UpdateMany(ctx, name, []store.Point{point}); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	var source *store.TransferSource
	if !*native {
		checkpoint := filepath.Join(root, "checkpoint")
		if err := donor.Checkpoint(checkpoint); err != nil {
			return err
		}
		source, err = store.OpenTransferSource(checkpoint, store.TransferSourceOptions{Continuation: &whisperio.ContinuationCodec{}, CacheBytes: 32 << 20, Now: clock})
		if err != nil {
			return err
		}
		defer source.Close()
	}
	type exporter interface {
		ExportTransferLimit(context.Context, []string, int64) ([]store.TransferMetric, error)
	}
	measure := func(mode string, from exporter, to *store.Store, batch int) (result, error) {
		began := time.Now()
		var wire, exportNanos, importNanos atomic.Int64
		jobs := make(chan []string)
		var wg sync.WaitGroup
		var first error
		var once sync.Once
		for range *workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for group := range jobs {
					start := time.Now()
					records, err := from.ExportTransferLimit(ctx, group, 160<<20)
					exportNanos.Add(time.Since(start).Nanoseconds())
					if err == nil {
						wire.Add(store.TransferWireSize2(records))
						start = time.Now()
						var proofs []store.TransferProof
						proofs, err = to.ImportTransfer(ctx, records)
						if err == nil {
							err = to.VerifyTransfer(ctx, proofs)
						}
						importNanos.Add(time.Since(start).Nanoseconds())
					}
					if err != nil {
						once.Do(func() { first = err })
					}
				}
			}()
		}
		for start := 0; start < len(names); start += batch {
			jobs <- names[start:min(start+batch, len(names))]
		}
		close(jobs)
		wg.Wait()
		seconds := time.Since(began).Seconds()
		return result{mode, int64(len(names)), wire.Load(), seconds, float64(exportNanos.Load()) / 1e9, float64(importNanos.Load()) / 1e9, float64(len(names)) / seconds, float64(wire.Load()) / seconds / 1e6}, first
	}
	oracleBatch := min(*batch, store.MaxTransferMetrics)
	if *single {
		oracleBatch = 1
	}
	reference, err := measure("reference", donor, baseline, oracleBatch)
	if err != nil {
		return err
	}
	var cpu *os.File
	if *profilePrefix != "" {
		cpu, err = os.Create(*profilePrefix + ".cpu")
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(cpu); err != nil {
			cpu.Close()
			return err
		}
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(1)
	}
	var local result
	var nativeDeferred []string
	if *native {
		local, nativeDeferred, err = measureNative(ctx, donor, bulk, names, *batch, *workers, *combine)
	} else {
		local, err = measure("checkpoint", source, bulk, *batch)
	}
	if cpu != nil {
		pprof.StopCPUProfile()
		_ = cpu.Close()
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
		for _, kind := range []string{"mutex", "block"} {
			f, profileErr := os.Create(*profilePrefix + "." + kind)
			if profileErr != nil {
				return profileErr
			}
			profileErr = errors.Join(pprof.Lookup(kind).WriteTo(f, 0), f.Close())
			if profileErr != nil {
				return profileErr
			}
		}
	}
	if err != nil {
		return err
	}
	// Keep reviewed deferred-combine work outside native throughput/profile.
	reviewedBatch := min(*batch, store.MaxTransferMetrics)
	for start := 0; start < len(nativeDeferred); start += reviewedBatch {
		group := nativeDeferred[start:min(start+reviewedBatch, len(nativeDeferred))]
		records, e := donor.ExportTransferLimit(ctx, group, 160<<20)
		if e != nil {
			return e
		}
		if _, e = bulk.ImportTransfer(ctx, records); e != nil {
			return e
		}
	}
	var archives, points int
	for _, name := range names {
		want, err := baseline.Snapshot(ctx, name)
		if err != nil {
			return err
		}
		got, err := bulk.Snapshot(ctx, name)
		if err != nil {
			return err
		}
		if err := store.EqualSnapshots(want, got); err != nil {
			return fmt.Errorf("differential %s: %w", name, err)
		}
		archives += len(want.Archives)
		for _, a := range want.Archives {
			points += len(a.Points)
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"source_cache_bytes": *sourceCache, "source_cache_shards": *sourceShards, "source_scratch": *sourceScratch, "captured_histories": len(input), "captured_points": capturedPoints, "replication": *repeat, "overlap_names": overlap, "workers": *workers, "batch": *batch, "single_name_oracle": *single, "reference": reference, "candidate": local, "native": *native, "memory_filesystem": *memory, "native_deferred_names": len(nativeDeferred), "native_capture": *capturePath, "native_stats": bulk.NativeStats(), "native_capture_names": len(capture.Names), "sample_names": *sampleNames, "equal_archives": archives, "equal_point_bits": points, "mismatches": 0, "temporary_stores_removed_on_exit": true})
}
