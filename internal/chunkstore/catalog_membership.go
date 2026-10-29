package chunkstore

import (
	"encoding/binary"
	"errors"
	"hash/maphash"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/phasecpu"

	"github.com/cockroachdb/pebble"
)

// catalogMembership can prove absence only after a complete catalog snapshot.
// Every catalog encoder adds its name before committing, including writes made
// while that snapshot is being scanned. Bits are never removed: failed writes
// and deletions can only add harmless false positives.
type catalogMembership struct {
	seed     maphash.Seed
	words    []atomic.Uint64
	ready    atomic.Bool
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// halt stops the scan and waits for it to end. It is safe to call more than
// once and concurrently.
func (f *catalogMembership) halt() {
	f.stopOnce.Do(func() { close(f.stop) })
	<-f.done
}

func newCatalogMembership(highWatermark uint64) *catalogMembership {
	// About 32 bits per allocated ID, rounded up, with a fixed 64 MiB ceiling.
	// Saturation only increases ordinary database lookups; it cannot hide data.
	bytes := uint64(4096)
	for bytes < 64<<20 && highWatermark > bytes/4 {
		bytes *= 2
	}
	return &catalogMembership{
		seed: maphash.MakeSeed(), words: make([]atomic.Uint64, bytes/8),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

func (f *catalogMembership) add(name string) {
	if f == nil {
		return
	}
	hash := maphash.String(f.seed, name)
	step := bits.RotateLeft64(hash, 32) | 1
	mask := uint64(len(f.words)*64 - 1)
	for range 4 {
		bit := hash & mask
		f.words[bit/64].Or(uint64(1) << (bit % 64))
		hash += step
	}
}

func (f *catalogMembership) mayContain(name string) bool {
	if f == nil || !f.ready.Load() {
		return true
	}
	hash := maphash.String(f.seed, name)
	step := bits.RotateLeft64(hash, 32) | 1
	mask := uint64(len(f.words)*64 - 1)
	for range 4 {
		bit := hash & mask
		if f.words[bit/64].Load()&(uint64(1)<<(bit%64)) == 0 {
			return false
		}
		hash += step
	}
	return true
}

func (s *Store) startCatalogMembership() {
	if s.incrementalSource == "" {
		return
	}
	view := s.db.NewSnapshot()
	var high uint64
	if value, closer, err := view.Get(sequenceKey()); err == nil {
		if len(value) == 8 {
			high = binary.BigEndian.Uint64(value)
		}
		_ = closer.Close()
	}
	f := newCatalogMembership(high)
	// Publish before Open returns, so every concurrent catalog write joins the
	// same filter even if it is newer than the initial snapshot.
	s.catalogMembership = f
	go s.loadCatalogMembership(view, f)
}

// membershipRefresh bounds how long the catalog scan reads through one
// iterator. An iterator pins the LSM version it was opened on: tables that
// compactions replace stay on disk until it closes, and the reader that
// releases the last reference evicts each of them from the table and block
// caches. A process stopped two minutes after it opened, its scan still on
// the first iterator, spent 1.54 s in Close stopping the scan, with input
// closed; releasing 1,091 replaced tables took 0.24 s on a laptop. Reopening
// on the same snapshot reads the same catalog.
var membershipRefresh = 100 * time.Millisecond

// catalogScanPause, when set by a test, runs as the scan opens each iterator,
// before it reads it.
var catalogScanPause func(stop <-chan struct{})

func (s *Store) loadCatalogMembership(view *pebble.Snapshot, f *catalogMembership) {
	phasecpu.Set(phasecpu.MetadataCache)
	defer phasecpu.Set(phasecpu.Other)
	defer close(f.done)
	defer view.Close()
	for lower := []byte("m/"); lower != nil; {
		var err error
		if lower, err = f.scan(view, lower); err != nil {
			return
		}
	}
	select {
	case <-f.stop:
		return
	default:
	}
	f.ready.Store(true)
}

var errMembershipStopped = errors.New("catalog membership scan stopped")

// scan adds catalog names from lower on through one iterator, for about
// membershipRefresh. It returns the key to continue from, or nil once the
// catalog is complete.
func (f *catalogMembership) scan(view *pebble.Snapshot, lower []byte) ([]byte, error) {
	iter, err := view.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: []byte("m0")})
	if err != nil {
		return nil, err
	}
	if catalogScanPause != nil {
		catalogScanPause(f.stop)
	}
	deadline := time.Now().Add(membershipRefresh)
	var next []byte
	n := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		select {
		case <-f.stop:
			return nil, errors.Join(errMembershipStopped, iter.Close())
		default:
		}
		key := iter.Key()
		f.add(string(key[2:]))
		if n++; n%1024 == 0 && time.Now().After(deadline) {
			// The smallest key after this one.
			next = append(append(make([]byte, 0, len(key)+1), key...), 0)
			break
		}
	}
	return next, errors.Join(iter.Error(), iter.Close())
}
