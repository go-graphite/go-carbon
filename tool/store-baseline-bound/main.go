// store-baseline-bound inspects immutable converter verification receipts.
// It never opens or changes the converted metric database.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

type result struct {
	Version                      int
	Identity                     store.CaptureIdentity
	Records, BaselineMaxMetricID uint64
	OrderedReceiptsSHA256        string
	ReceiptSHA256                string
	Seconds                      float64
}

func scan(db *pebble.DB, h hash.Hash) (uint64, uint64, error) {
	it, err := db.NewIter(nil)
	if err != nil {
		return 0, 0, err
	}
	defer it.Close()
	var count, maximum uint64
	for it.First(); it.Valid(); it.Next() {
		key, v := it.Key(), it.Value()
		if len(key) == 0 || len(v) != 65 || v[0] != 1 {
			return 0, 0, errors.New("invalid baseline verification record")
		}
		id := binary.BigEndian.Uint64(v[1:9])
		if id == 0 || binary.BigEndian.Uint64(v[9:17]) == 0 || binary.BigEndian.Uint64(v[17:25]) == 0 {
			return 0, 0, errors.New("invalid baseline destination identity")
		}
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(key)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(key)
		_, _ = h.Write(v)
		count++
		maximum = max(maximum, id)
	}
	return count, maximum, it.Error()
}

func inspect(dir string) (result, error) {
	began := time.Now()
	var out result
	for _, name := range []string{"CONVERSION-IN-PROGRESS"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			return out, errors.New("baseline conversion still active or guard inaccessible")
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "VERIFIED-CAPTURES.json"))
	if err != nil {
		return out, err
	}
	var receipt store.CaptureReceipt
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return out, err
	}
	if receipt.Version != 1 || receipt.Verification == "" || !receipt.Identity.Live || receipt.Identity.SourceID == "" {
		return out, errors.New("missing verified live baseline identity")
	}
	var checkpoint struct {
		Version  int
		Identity store.CaptureIdentity
	}
	path := filepath.Join(dir, ".baseline-verification")
	b, err := os.ReadFile(filepath.Join(path, "IDENTITY.json"))
	if err != nil {
		return out, err
	}
	if err = json.Unmarshal(b, &checkpoint); err != nil {
		return out, err
	}
	if checkpoint.Version != 1 || checkpoint.Identity != receipt.Identity {
		return out, errors.New("checkpoint and baseline identities differ")
	}
	cache := pebble.NewCache(16 << 20)
	defer cache.Unref()
	db, err := pebble.Open(path, &pebble.Options{ReadOnly: true, Cache: cache, MemTableSize: 8 << 20})
	if err != nil {
		return out, err
	}
	h := sha256.New()
	count, maximum, err := scan(db, h)
	err = errors.Join(err, db.Close())
	if err != nil {
		return out, err
	}
	if count == 0 || maximum == 0 {
		return out, errors.New("empty verification checkpoint")
	}
	sum := sha256.Sum256(raw)
	out = result{Version: 1, Identity: receipt.Identity, Records: count, BaselineMaxMetricID: maximum, OrderedReceiptsSHA256: hex.EncodeToString(h.Sum(nil)), ReceiptSHA256: hex.EncodeToString(sum[:]), Seconds: time.Since(began).Seconds()}
	return out, nil
}

func main() {
	dir := flag.String("destination", "", "completed baseline destination (only .baseline-verification is opened)")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "-destination required")
		os.Exit(2)
	}
	out, err := inspect(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
