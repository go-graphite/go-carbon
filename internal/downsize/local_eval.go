// bulk-copy-eval compares checkpoint export/import with the live-source transfer
// oracle on captured Whisper archives. It only opens private local databases.
package downsize

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

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
	parent := flags.String("scratch", "", "existing private scratch directory")
	repeat := flags.Int("repeat", 1, "repeat captured histories under distinct names (reported as replication)")
	batch := flags.Int("batch", 512, "metrics per measured import")
	workers := flags.Int("workers", 1, "bounded parallel export/import workers")
	single := flags.Bool("single-oracle", false, "reference imports one name at a time")
	overlapPercent := flags.Int("overlap-percent", 0, "seed this percentage of survivor names from captured donor histories, then add survivor corrections (0 uses captured targets)")
	profilePrefix := flags.String("profile-prefix", "", "write CPU/mutex/block profiles of the checkpoint phase under this local prefix")
	epoch := flags.Int64("now", time.Now().Unix(), "fixed evaluation clock, Unix seconds")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *fixtures == "" || *parent == "" || *repeat < 1 || *batch < 1 || *batch > 10000 || *workers < 1 || *workers > 16 || *overlapPercent < 0 || *overlapPercent > 100 {
		return errors.New("invalid evaluation options")
	}
	data, err := os.ReadFile(*fixtures)
	if err != nil {
		return err
	}
	var input []bulkEvalFixture
	if err := json.Unmarshal(data, &input); err != nil {
		return err
	}
	if len(input) == 0 {
		return errors.New("empty fixtures")
	}
	root, err := os.MkdirTemp(*parent, "bulk-eval-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	clock := func() time.Time { return time.Unix(*epoch, 0) }
	ctx := context.Background()
	open := func(name string) (*store.Store, error) {
		originals := filepath.Join(root, name+"-originals")
		if err := os.Mkdir(originals, 0700); err != nil {
			return nil, err
		}
		return store.Open(filepath.Join(root, name), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: originals, Now: clock, CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: time.Second, ColdPack: store.ColdPackOptions{Mode: "exact", FloatCodec: true}})
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
			if err := seed(donor, name, source); err != nil {
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
					if err := seed(db, name, path); err != nil {
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
	checkpoint := filepath.Join(root, "checkpoint")
	if err := donor.Checkpoint(checkpoint); err != nil {
		return err
	}
	source, err := store.OpenTransferSource(checkpoint, store.TransferSourceOptions{Continuation: &whisperio.ContinuationCodec{}, CacheBytes: 32 << 20, Now: clock})
	if err != nil {
		return err
	}
	defer source.Close()
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
	oracleBatch := *batch
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
	local, err := measure("checkpoint", source, bulk, *batch)
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
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"captured_histories": len(input), "captured_points": capturedPoints, "replication": *repeat, "overlap_names": overlap, "workers": *workers, "batch": *batch, "single_name_oracle": *single, "reference": reference, "checkpoint": local, "equal_archives": archives, "equal_point_bits": points, "mismatches": 0, "temporary_stores_removed_on_exit": true})
}
