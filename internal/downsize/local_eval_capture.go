package downsize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

// Older evaluators must reject a native pair instead of silently treating the
// survivor as absent when they ignore the new metadata field.
const evalNativePairMagic = "BKPAIR01"

// Read and verify the complete capture even when only a subset is selected.
// Consumers receive owned batches; both private survivor stores use this same
// reader so native source and target layout proofs have identical framing rules.
func importEvalCapture(ctx context.Context, directory string, capture nativeCapture, selected map[string]bool, consume func([]store.TransferMetric) error) (map[string]string, error) {
	if capture.StoreFormat < 6 || capture.StoreFormat > 12 || len(capture.SHA256) != 64 || filepath.Base(capture.WireFile) != capture.WireFile {
		return nil, errors.New("invalid native capture metadata")
	}
	file, err := os.Open(filepath.Join(directory, capture.WireFile))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	hash := sha256.New()
	reader := io.TeeReader(file, hash)
	prefixBytes := int64(0)
	if capture.TargetNative != nil {
		var prefix [len(evalNativePairMagic)]byte
		if _, err := io.ReadFull(reader, prefix[:]); err != nil || string(prefix[:]) != evalNativePairMagic {
			return nil, errors.New("native survivor capture requires paired-input framing")
		}
		prefixBytes = int64(len(prefix))
	}
	stream, err := store.NewTransferStream(reader)
	if err != nil {
		return nil, err
	}
	if stream.Count != len(capture.Names) {
		return nil, errors.New("native capture name count differs")
	}
	planned := make(map[string]bool, len(capture.Names))
	for _, name := range capture.Names {
		if planned[name] {
			return nil, errors.New("duplicate capture name")
		}
		planned[name] = true
	}
	seen := make(map[string]bool, len(capture.Names))
	layouts := make(map[string]string)
	var records []store.TransferMetric
	buffered := 0
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		err := consume(records)
		records = nil
		buffered = 0
		return err
	}
	for range stream.Count {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, err := stream.Next()
		if err != nil {
			return nil, err
		}
		name := record.Config.Name
		if !planned[name] || seen[name] {
			return nil, errors.New("native capture names differ")
		}
		seen[name] = true
		if record.Version.ID == 0 || selected != nil && !selected[name] {
			continue
		}
		layouts[name] = evalRecordLayoutDigest(record)
		for i, kv := range record.Keys {
			record.Keys[i] = store.RelayoutKV{Key: bytes.Clone(kv.Key), Value: bytes.Clone(kv.Value)}
			buffered += len(kv.Key) + len(kv.Value)
		}
		records = append(records, record)
		if len(records) >= 512 || buffered >= 4<<20 {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	_, count, err := stream.Finish()
	if err != nil {
		return nil, err
	}
	if count+prefixBytes != capture.Bytes || hex.EncodeToString(hash.Sum(nil)) != capture.SHA256 {
		return nil, errors.New("native capture integrity mismatch")
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return layouts, nil
}
