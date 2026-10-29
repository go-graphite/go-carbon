package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

const receiptName = "VERIFIED-CAPTURES.json"

type captureReceipt = chunkstore.CaptureReceipt

type captureProof struct{ db *pebble.DB }

func publishCaptureReceipt(dir string, id identity, verification string, maxMetricID uint64) error {
	data, err := json.Marshal(captureReceipt{Version: 1, Identity: id, Verification: verification, BaselineMaxMetricID: maxMetricID})
	if err != nil {
		return err
	}
	return writeCheckpointFile(dir, receiptName, data, os.O_TRUNC)
}

// An older fully verified baseline can be adopted only by explicit operational
// assertion while it is still an unserved, completed live baseline. The caller
// must retain and audit the successful converter command and verification log.
func adoptVerifiedBaseline(dir string, id identity) error {
	for _, marker := range []string{"CONVERSION-IN-PROGRESS", "MIRROR-RECONCILE-REQUIRED"} {
		if _, err := os.Stat(filepath.Join(dir, marker)); err == nil {
			return fmt.Errorf("cannot adopt baseline with %s", marker)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "NEEDS-CATCHUP")); err != nil {
		return errors.New("adoption requires an unserved live baseline with NEEDS-CATCHUP")
	}
	return publishCaptureReceipt(dir, id, "operator assertion: completed fully verified baseline; captures retained", 0)
}

func openCaptureProof(dir string, id identity) (*captureProof, error) {
	if _, err := os.Stat(filepath.Join(dir, "MIRROR-RECONCILE-REQUIRED")); !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("fast catch-up cannot adopt a previously served/mirrored store; use full reconciliation")
	}
	data, err := os.ReadFile(filepath.Join(dir, receiptName))
	if err != nil {
		return nil, fmt.Errorf("fast catch-up requires a verified capture receipt: %w", err)
	}
	var receipt captureReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		return nil, err
	}
	if receipt.Version != 1 || receipt.Identity != id || receipt.Verification == "" {
		return nil, errors.New("verified capture receipt does not match baseline identity")
	}
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()
	db, err := pebble.Open(filepath.Join(dir, ".capture-proof"), &pebble.Options{Cache: cache, MemTableSize: 4 << 20})
	if err != nil {
		return nil, err
	}
	return &captureProof{db: db}, nil
}

func proofBytes(m chunkstore.Metadata) []byte {
	result := make([]byte, 24)
	binary.BigEndian.PutUint64(result, m.ID)
	binary.BigEndian.PutUint64(result[8:], m.Generation)
	binary.BigEndian.PutUint64(result[16:], m.Revision)
	return result
}

func (p *captureProof) matches(name string, m chunkstore.Metadata) (bool, error) {
	data, closer, err := p.db.Get([]byte(name))
	if errors.Is(err, pebble.ErrNotFound) {
		return m.Generation == 1 && m.Revision == 1, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	if len(data) != 24 {
		return false, nil
	} // durable pending marker, never a skip
	return string(data) == string(proofBytes(m)), nil
}

// Sync pending BEFORE publishing either capture file. A crash can otherwise
// pair a newly captured source with an older database generation.
func (p *captureProof) pending(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.db.Set([]byte(name), []byte{0}, pebble.Sync)
}

func (p *captureProof) verified(name string, m chunkstore.Metadata) error {
	// No cross-database atomicity is assumed. If this record survives but the
	// destination commit does not, ID/generation/revision differ and force import.
	// If the record is lost, the earlier synced pending marker forces import.
	return p.db.Set([]byte(name), proofBytes(m), pebble.NoSync)
}
