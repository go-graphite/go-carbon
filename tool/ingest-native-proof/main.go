// ingest-native-proof applies captured donor bytes directly, without first
// seeding/re-exporting them through an importer that may repair metadata.
package main

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
	"strings"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/ingestoracle"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	metadata := flag.String("capture", "", "native capture metadata")
	parent := flag.String("scratch", "", "existing private scratch root")
	flag.Parse()
	if *metadata == "" || *parent == "" {
		return errors.New("capture and scratch required")
	}
	var capture struct {
		Source, WireFile, SHA256 string
		Names                    []string
		Bytes                    int64
		Captured                 time.Time
	}
	data, err := os.ReadFile(*metadata)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(data, &capture); err != nil {
		return err
	}
	if filepath.Base(capture.WireFile) != capture.WireFile || len(capture.Names) == 0 {
		return errors.New("invalid capture")
	}
	path := filepath.Join(filepath.Dir(*metadata), capture.WireFile)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	hash := sha256.New()
	stream, err := store.NewTransferStream(io.TeeReader(f, hash))
	if err != nil {
		f.Close()
		return err
	}
	if stream.Count != len(capture.Names) {
		f.Close()
		return errors.New("capture count mismatch")
	}
	planned := make(map[string]bool, len(capture.Names))
	for _, name := range capture.Names {
		if planned[name] {
			return errors.New("duplicate name")
		}
		planned[name] = true
	}
	var records []store.TransferMetric
	for range stream.Count {
		r, e := stream.Next()
		if e != nil {
			f.Close()
			return e
		}
		if !planned[r.Config.Name] {
			f.Close()
			return errors.New("unexpected name")
		}
		delete(planned, r.Config.Name)
		for i, kv := range r.Keys {
			r.Keys[i] = store.RelayoutKV{Key: bytes.Clone(kv.Key), Value: bytes.Clone(kv.Value)}
		}
		records = append(records, r)
	}
	_, size, err := stream.Finish()
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if size != capture.Bytes || hex.EncodeToString(hash.Sum(nil)) != capture.SHA256 {
		return errors.New("capture SHA mismatch")
	}
	root, err := os.MkdirTemp(*parent, "native-direct-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	clock := func() time.Time { return capture.Captured }
	open := func(name string) (*store.Store, error) {
		originals := filepath.Join(root, name+"-originals")
		if e := os.MkdirAll(originals, 0700); e != nil {
			return nil, e
		}
		db, err := store.Open(filepath.Join(root, name), store.Options{ExactBlocks: true, TimeBuckets: true, Continuation: &whisperio.ContinuationCodec{}, IncrementalSource: originals, Now: clock, CacheSize: 32 << 20, MemTableSize: 16 << 20, SyncInterval: time.Second, ColdPack: store.ColdPackOptions{Mode: "exact", FloatCodec: true}})
		if err != nil {
			return nil, err
		}
		// Fence v9 capability, then join/disable maintenance before any data
		// arrives. A repacker may preserve point bits while changing raw keys.
		if err := db.ConfigureColdPack(store.ColdPackOptions{Mode: "off", FloatCodec: true}); err != nil {
			db.Close()
			return nil, err
		}
		return db, nil
	}
	target, err := open("native")
	if err != nil {
		return err
	}
	defer target.Close()
	reference, err := open("reference")
	if err != nil {
		return err
	}
	defer reference.Close()
	ctx := context.Background()
	f, err = os.Open(path)
	if err != nil {
		return err
	}
	request := store.NativeRange{Operation: strings.Repeat("4", 64), Plan: strings.Repeat("5", 64), Stream: strings.Repeat("6", 64), Source: capture.Source, End: int64(len(capture.Names)), Names: capture.Names}
	began := time.Now()
	receipt, err := target.NativeImport(ctx, request, f)
	seconds := time.Since(began).Seconds()
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	var present []store.TransferMetric
	for _, r := range records {
		if r.Version.ID != 0 {
			present = append(present, r)
		}
	}
	for i := 0; i < len(present); i += 128 {
		if _, err = reference.ImportTransfer(ctx, present[i:min(i+128, len(present))]); err != nil {
			return err
		}
	}
	var keys, points, renderPoints int64
	for _, want := range present {
		got, e := target.ExportTransfer(ctx, []string{want.Config.Name})
		if e != nil {
			return e
		}
		if len(got) != 1 || len(got[0].Keys) != len(want.Keys) {
			missing, extra := []string{}, []string{}
			actual := map[string]bool{}
			for _, kv := range got[0].Keys {
				actual[string(kv.Key)] = true
			}
			expected := map[string]bool{}
			for _, kv := range want.Keys {
				expected[string(kv.Key)] = true
				if !actual[string(kv.Key)] {
					missing = append(missing, hex.EncodeToString(kv.Key))
				}
			}
			for _, kv := range got[0].Keys {
				if !expected[string(kv.Key)] {
					extra = append(extra, hex.EncodeToString(kv.Key))
				}
			}
			return fmt.Errorf("original native key count mismatch %s expected=%d actual=%d missing=%v extra=%v source_version=%+v target_version=%+v", want.Config.Name, len(want.Keys), len(got[0].Keys), missing, extra, want.Version, got[0].Version)
		}
		for i, kv := range want.Keys {
			if !bytes.Equal(kv.Key, got[0].Keys[i].Key) || !bytes.Equal(kv.Value, got[0].Keys[i].Value) {
				return fmt.Errorf("original native byte mismatch %s key%d", want.Config.Name, i)
			}
			keys++
		}
		a, e := reference.Snapshot(ctx, want.Config.Name)
		if e != nil {
			return e
		}
		b, e := target.Snapshot(ctx, want.Config.Name)
		if e != nil {
			return e
		}
		if e = store.EqualSnapshots(a, b); e != nil {
			return fmt.Errorf("point differential %s: %w", want.Config.Name, e)
		}
		for _, archive := range a.Archives {
			points += int64(len(archive.Points))
		}
		for _, db := range []*store.Store{reference, target} {
			_, n, e := ingestoracle.VerifyFetch(ctx, db, b, capture.Captured.Unix())
			if e != nil {
				return e
			}
			renderPoints += n
		}
	}
	if receipt.CopiedCount != len(present) {
		return errors.New("native copy bitmap mismatch")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"capture_sha256": capture.SHA256, "names": len(present), "wire_bytes": capture.Bytes, "native_seconds": seconds, "native_MBps": float64(capture.Bytes) / seconds / 1e6, "original_raw_keys_equal": keys, "logical_point_bits_equal": points, "render_point_bits_equal": renderPoints, "mismatches": 0, "native_stats": target.NativeStats(), "cold_pack": target.ColdPackStats(), "temporary_stores_removed_on_exit": true})
}
