// ingest-eval opens only new private stores and times apply after input capture.
// It never connects to a daemon, reads configuration or opens an existing DB.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type fixture struct{ Name, Source, Target string }
type job struct {
	records []store.TransferMetric
	wire    string
	request store.NativeRange
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func counters() map[string]string {
	out := map[string]string{}
	for _, path := range []string{"/proc/stat", "/proc/diskstats", "/proc/net/dev", "/proc/self/io", "/proc/self/stat"} {
		if b, e := os.ReadFile(path); e == nil {
			out[path] = string(b)
		}
	}
	return out
}

func run() error {
	fixtures := flag.String("fixtures", "", "captured logical archive fixture JSON")
	parent := flag.String("scratch", "", "existing private scratch root")
	mode := flag.String("mode", "native", "native, legacy, or overlap")
	repeat := flag.Int("repeat", 32, "explicit history replication under distinct names")
	batch := flag.Int("batch", 128, "names per independent apply")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "apply workers")
	profile := flag.String("profile-prefix", "", "CPU, mutex and block profile prefix")
	epoch := flag.Int64("now", 1793986000, "frozen archive clock")
	flag.Parse()
	if *fixtures == "" || *parent == "" || *repeat < 1 || *batch < 1 || *batch > store.MaxTransferMetrics || *workers < 1 || (*mode != "native" && *mode != "legacy" && *mode != "overlap") {
		return errors.New("invalid options")
	}
	root, err := os.MkdirTemp(*parent, "ingest-eval-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	ctx := context.Background()
	open := func(name string) (*store.Store, error) {
		originals := filepath.Join(root, name+"-originals")
		if e := os.MkdirAll(originals, 0700); e != nil {
			return nil, e
		}
		return store.Open(filepath.Join(root, name), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: originals, Now: func() time.Time { return time.Unix(*epoch, 0) }, CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: time.Second, ColdPack: store.ColdPackOptions{Mode: "exact", FloatCodec: true}})
	}
	source, err := open("source")
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := open("target")
	if err != nil {
		return err
	}
	defer target.Close()
	seedTarget, err := open("seed-target")
	if err != nil {
		return err
	}
	defer seedTarget.Close()
	data, err := os.ReadFile(*fixtures)
	if err != nil {
		return err
	}
	var rows []fixture
	if err = json.Unmarshal(data, &rows); err != nil {
		return err
	}
	if len(rows) == 0 {
		return errors.New("empty capture")
	}
	seed := func(db *store.Store, name, path string) error {
		snap, e := whisperio.ReadSnapshotIn(ctx, name, filepath.Join(filepath.Dir(*fixtures), path), root)
		if e != nil {
			return e
		}
		m, e := db.StageFileSnapshot(ctx, snap)
		if e != nil {
			return e
		}
		m, e = db.PublishFileOwnership(ctx, snap, m)
		if e != nil {
			return e
		}
		return db.CompleteFileCleanup(ctx, m)
	}
	var originals, targets []store.TransferMetric
	for _, row := range rows {
		if err = seed(source, row.Name, row.Source); err != nil {
			return err
		}
		r, e := source.ExportTransfer(ctx, []string{row.Name})
		if e != nil {
			return e
		}
		originals = append(originals, r[0])
		path := row.Target
		if path == "" {
			path = row.Source
		}
		if err = seed(seedTarget, row.Name, path); err != nil {
			return err
		}
		r, e = seedTarget.ExportTransfer(ctx, []string{row.Name})
		if e != nil {
			return e
		}
		targets = append(targets, r[0])
	}
	var jobs []job
	var allNames []string
	for i := 0; i < *repeat*len(originals); i += *batch {
		var j job
		var seeds []store.TransferMetric
		end := min(i+*batch, *repeat*len(originals))
		for k := i; k < end; k++ {
			r := originals[k%len(originals)]
			r.Config.Name = fmt.Sprintf("ingest_eval.r%d.%s", k/len(originals), r.Config.Name)
			r.Version.Name = r.Config.Name
			if *mode == "native" {
				r.PointDigest = ""
				r.Counts = nil
			}
			j.records = append(j.records, r)
			allNames = append(allNames, r.Config.Name)
			seed := targets[k%len(targets)]
			seed.Config.Name = r.Config.Name
			seed.Version.Name = r.Config.Name
			seeds = append(seeds, seed)
		}
		if *mode == "overlap" {
			if _, err = target.ImportTransfer(ctx, seeds); err != nil {
				return err
			}
		}
		if *mode == "native" {
			j.wire = filepath.Join(root, fmt.Sprintf("input-%09d.transfer", i))
			f, e := os.Create(j.wire)
			if e != nil {
				return e
			}
			e = errors.Join(store.WriteTransfer2(f, j.records), f.Sync(), f.Close())
			if e != nil {
				return e
			}
			f, e = os.Open(j.wire)
			if e != nil {
				return e
			}
			stream, e := store.NewTransferStream(f)
			if e == nil {
				for range stream.Count {
					if _, e = stream.Next(); e != nil {
						break
					}
				}
			}
			if e == nil {
				_, _, e = stream.Finish()
			}
			e = errors.Join(e, f.Close())
			if e != nil {
				return e
			}
		}
		j.request = store.NativeRange{Operation: strings.Repeat("1", 64), Plan: strings.Repeat("2", 64), Stream: strings.Repeat("3", 64), Source: "private-captured-history", Start: int64(i), End: int64(end), Names: allNames[i:end]}
		jobs = append(jobs, j)
	}
	var cpu *os.File
	if *profile != "" {
		cpu, err = os.Create(*profile + ".cpu")
		if err != nil {
			return err
		}
		if err = pprof.StartCPUProfile(cpu); err != nil {
			return err
		}
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}
	before := counters()
	applyBefore := target.TransferApplyStats()
	began := time.Now()
	queue := make(chan job)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	var wireBytes int64
	for range *workers {
		wg.Go(func() {
			for j := range queue {
				var e error
				var n int64
				if *mode == "native" {
					f, openErr := os.Open(j.wire)
					e = openErr
					if e == nil {
						r, importErr := target.NativeImport(ctx, j.request, f)
						n = r.Bytes
						e = errors.Join(importErr, f.Close())
						if e == nil && r.CopiedCount != len(j.records) {
							e = errors.New("unexpected native deferral")
						}
					}
				} else {
					_, e = target.ImportTransfer(ctx, j.records)
					n = store.TransferWireSize2(j.records)
				}
				mu.Lock()
				wireBytes += n
				if first == nil && e != nil {
					first = e
				}
				mu.Unlock()
			}
		})
	}
	for _, j := range jobs {
		queue <- j
	}
	close(queue)
	wg.Wait()
	seconds := time.Since(began).Seconds()
	after := counters()
	applyAfter := target.TransferApplyStats()
	if cpu != nil {
		pprof.StopCPUProfile()
		_ = cpu.Close()
		runtime.SetBlockProfileRate(0)
		runtime.SetMutexProfileFraction(0)
		for _, kind := range []string{"block", "mutex"} {
			f, e := os.Create(*profile + "." + kind)
			if e != nil {
				return e
			}
			if e = errors.Join(pprof.Lookup(kind).WriteTo(f, 0), f.Close()); e != nil {
				return e
			}
		}
	}
	if first != nil {
		return first
	}
	// Exhaustive raw oracle for absent names: all encoded key/value bytes, not
	// just a logical rendering or sampled points. Overlap emits a logical-bit
	// fingerprint for comparison with the baseline executable on the same input.
	hash := sha256.New()
	var keys, points int64
	for _, j := range jobs {
		for _, want := range j.records {
			hash.Write([]byte(want.Config.Name))
			hash.Write([]byte{0})
			if *mode != "overlap" {
				got, e := target.ExportTransfer(ctx, []string{want.Config.Name})
				if e != nil {
					return e
				}
				if len(got[0].Keys) != len(want.Keys) {
					return errors.New("raw key count mismatch")
				}
				for i, kv := range got[0].Keys {
					if !bytes.Equal(kv.Key, want.Keys[i].Key) || !bytes.Equal(kv.Value, want.Keys[i].Value) {
						return fmt.Errorf("raw byte mismatch %s key%d", want.Config.Name, i)
					}
					hash.Write(kv.Key)
					hash.Write(kv.Value)
					keys++
				}
			} else {
				snapshot, e := target.Snapshot(ctx, want.Config.Name)
				if e != nil {
					return e
				}
				var word [8]byte
				for _, a := range snapshot.Archives {
					binary.BigEndian.PutUint64(word[:], uint64(a.Retention.Step))
					hash.Write(word[:])
					binary.BigEndian.PutUint64(word[:], uint64(a.Retention.Count))
					hash.Write(word[:])
					for _, p := range a.Points {
						binary.BigEndian.PutUint64(word[:], uint64(p.Timestamp))
						hash.Write(word[:])
						binary.BigEndian.PutUint64(word[:], math.Float64bits(p.Value))
						hash.Write(word[:])
						points++
					}
				}
			}
		}
	}
	// Keep response identity independent of scheduling order.
	stats := target.Stats()
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": *mode, "captured_histories": len(rows), "replication": *repeat, "names": len(allNames), "batch": *batch, "workers": *workers, "wire_bytes": wireBytes, "seconds": seconds, "MBps": float64(wireBytes) / seconds / 1e6, "output_sha256": hex.EncodeToString(hash.Sum(nil)), "equal_raw_keys": keys, "logical_point_bits": points, "mismatches": 0, "apply_before": applyBefore, "apply_after": applyAfter, "native_stats": target.NativeStats(), "ingest_l0_bytes": stats.IngestL0Bytes, "ingest_memtable_overlaps": stats.IngestMemtableOverlaps, "before": before, "after": after, "temporary_stores_removed_on_exit": true, "input_origin": "logical archives re-encoded by current codec, not native production layout"})
}
