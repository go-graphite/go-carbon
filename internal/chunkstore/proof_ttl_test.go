package chunkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func ttlHistory(t *testing.T, s *Store) mutationHistory {
	t.Helper()
	m, err := s.Metadata(t.Context(), "proof")
	if err != nil {
		t.Fatal(err)
	}
	h, err := readMutationHistory(s.db, m)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestProofTTLIdleRenewalLegacyAndDisabled(t *testing.T) {
	for _, mode := range []string{"idle", "renewed", "legacy", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Unix(10000, 0)
			ttl := time.Hour
			if mode == "disabled" {
				ttl = 0
			}
			s, err := Open(t.TempDir(), Options{ExactBlocks: true, MutationProofScopeTTL: &ttl, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			seedProof(t, s)
			old, _ := windowProof(t, s)
			if mode == "legacy" {
				// The old schema had no timestamp; its normal json.Unmarshal decoder
				// also accepts our added field (unknown fields are not rejected).
				m, _ := s.Metadata(t.Context(), "proof")
				h := ttlHistory(t, s)
				for i := range h.Scopes {
					h.Scopes[i].LastRegistered = 0
				}
				b, _ := json.Marshal(h)
				if err := s.db.Set(mutationKey(m), b, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(time.Hour)
			if mode == "renewed" {
				windowProof(t, s)
			}
			now = now.Add(time.Nanosecond)
			before := s.ProofStats()
			// Direct archive writes retain historical timestamps despite advancing now.
			if err := s.UpdateManyForArchive(t.Context(), "proof", []Point{{Timestamp: 9920, Value: 2}}, 12000); err != nil {
				t.Fatal(err)
			}
			h := ttlHistory(t, s)
			if mode == "idle" {
				assertNoProofHistory(t, s, "proof")
				stats := s.ProofStats()
				if stats.TrackedWrites != before.TrackedWrites || stats.UntrackedWrites != before.UntrackedWrites+1 || stats.ExpiredScopes != 1 || stats.ExpiredHistories != 1 {
					t.Fatal(before, stats)
				}
				// An intervening late mutation cannot reuse the retired window stamp.
				if err := s.UpdateManyForArchive(t.Context(), "proof", []Point{{Timestamp: 9900, Value: 9}}, 12000); err != nil {
					t.Fatal(err)
				}
				if fresh, _ := windowProof(t, s); fresh == old {
					t.Fatal("old token survived untracked in-window write")
				}
			} else {
				if len(h.Scopes) != 1 || h.Scopes[0].LastRegistered == 0 {
					t.Fatalf("scope missing: %+v", h)
				}
				if mode == "legacy" && h.Scopes[0].LastRegistered != now.UnixNano() {
					t.Fatal("legacy scope was not stamped on first write")
				}
				if got, _ := windowProof(t, s); got != old {
					t.Fatal("live/legacy scope lost clean outside-window stamp")
				}
				if s.ProofStats().ExpiredScopes != 0 {
					t.Fatal(s.ProofStats())
				}
			}
		})
	}
}

func TestProofTTLPartialAndRestart(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(10000, 0)
	ttl := time.Hour
	options := Options{ExactBlocks: true, MutationProofScopeTTL: &ttl, Now: func() time.Time { return now }}
	s, err := Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	seedProof(t, s)
	windowProof(t, s)
	now = now.Add(30 * time.Minute)
	if _, _, err := s.MutationProof(t.Context(), "proof", 9950, 9970); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30*time.Minute + time.Nanosecond)
	s, err = Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateManyForArchive(t.Context(), "proof", []Point{{Timestamp: 9920, Value: 2}}, 12000); err != nil {
		t.Fatal(err)
	}
	h := ttlHistory(t, s)
	if len(h.Scopes) != 1 || h.Scopes[0].From != 9950 {
		t.Fatalf("partial expiry: %+v", h)
	}
	if stats := s.ProofStats(); stats.ExpiredScopes != 1 || stats.ExpiredHistories != 0 {
		t.Fatal(stats)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Minute)
	s, err = Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.UpdateManyForArchive(t.Context(), "proof", []Point{{Timestamp: 9920, Value: 3}}, 12000); err != nil {
		t.Fatal(err)
	}
	assertNoProofHistory(t, s, "proof")
}

// No point changes still have to commit the legacy stamp or expiry tombstone.
func TestProofTTLContinuationNoop(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(fmt.Sprint(grouped), func(t *testing.T) {
			now := time.Unix(10000, 0)
			ttl := time.Hour
			s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: noopTTLCodec{}, MutationProofScopeTTL: &ttl, Now: func() time.Time { return now }})
			defer closeTestStore(t, s)
			createTestMetric(t, s, "proof")
			windowProof(t, s)
			m, _ := s.Metadata(t.Context(), "proof")
			h := ttlHistory(t, s)
			h.Scopes[0].LastRegistered = 0
			data, _ := json.Marshal(h)
			if err := s.db.Set(mutationKey(m), data, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			write := func() {
				t.Helper()
				var err error
				if grouped {
					err = s.UpdateBatch(t.Context(), []MetricUpdate{{Name: "proof"}, {Name: "proof"}}, nil)
				} else {
					err = s.UpdateMany(t.Context(), "proof", nil)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			write()
			if h := ttlHistory(t, s); h.Scopes[0].LastRegistered != now.UnixNano() {
				t.Fatal("noop dropped legacy stamp", h)
			}
			now = now.Add(ttl)
			write() // exactly TTL remains live
			if h := ttlHistory(t, s); len(h.Scopes) != 1 {
				t.Fatal("expired at inclusive boundary")
			}
			now = now.Add(time.Nanosecond)
			before := s.ProofStats()
			write()
			assertNoProofHistory(t, s, "proof")
			after := s.ProofStats()
			if after.TrackedWrites != before.TrackedWrites || after.ExpiredScopes != 1 || after.ExpiredHistories != 1 {
				t.Fatal(before, after)
			}
			m2, _ := s.Metadata(t.Context(), "proof")
			if m2.Revision != m.Revision {
				t.Fatal("bookkeeping-only commit advanced revision")
			}
		})
	}
}

type noopTTLCodec struct{ batchTestCodec }

func (noopTTLCodec) Update(_ context.Context, _ ContinuationIO, _ Metadata, _ []Point, _ int, _ func() time.Time, _ *ContinuationMaintenance) error {
	return nil
}

func TestProofTTLConcurrentSameNameBatches(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Unix(10000, 0).UnixNano())
	ttl := time.Hour
	codec := &pausedUpdateCodec{name: "held", entered: make(chan struct{}), release: make(chan struct{})}
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: codec, MetadataCacheSize: 8 << 20, MutationProofScopeTTL: &ttl, Now: func() time.Time { return time.Unix(0, clock.Load()) }})
	defer closeTestStore(t, s)
	createTestMetric(t, s, "held")
	proof := func() string {
		t.Helper()
		p, _, err := s.MutationProof(t.Context(), "held", 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	old := proof()
	first := make(chan error, 1)
	go func() {
		first <- s.UpdateBatch(t.Context(), []MetricUpdate{{Name: "held", Points: []Point{{Value: 1}}}, {Name: "held", Points: []Point{{Value: 2}}}}, nil)
	}()
	<-codec.entered // staged while still live
	clock.Add(int64(ttl + time.Nanosecond))
	second := make(chan error, 1)
	go func() {
		second <- s.UpdateBatch(t.Context(), []MetricUpdate{{Name: "held", Points: []Point{{Value: 3}}}, {Name: "held", Points: []Point{{Value: 4}}}}, nil)
	}()
	select {
	case err := <-second:
		close(codec.release)
		<-first
		t.Fatalf("same-name batch overtook reservation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(codec.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	assertNoProofHistory(t, s, "held")
	if proof() == old {
		t.Fatal("staged history resurrected old token")
	}
	m, _ := s.Metadata(t.Context(), "held")
	value, err := s.newContinuationIO(t.Context(), s.db, nil, m).Get([]byte("point"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, uint64Bytes(math.Float64bits(10))) {
		t.Fatalf("ordered point bytes: %x", value)
	}
}

func TestProofTTLDecoderAndConfiguration(t *testing.T) {
	{
		s := proofTestStore(t, t.TempDir())
		if s.mutationProofScopeTTL != 72*time.Hour {
			t.Fatal(s.mutationProofScopeTTL)
		}
		s.Close()
	}
	negative := -time.Nanosecond
	if s, err := Open(t.TempDir(), Options{MutationProofScopeTTL: &negative}); err == nil {
		s.Close()
		t.Fatal("negative TTL accepted")
	}
	// Match the pre-TTL decoder, including its unknown-field behavior.
	var legacy struct {
		Scopes []struct {
			From, Until int64
			Revision    uint64
			Reason      string
		}
		Version     int
		Floor, Head uint64
		Entries     []mutationEntry
	}
	current := mutationHistory{Version: 1, Scopes: []mutationScope{{From: 1, Until: 2, LastRegistered: time.Now().UnixNano()}}}
	data, _ := json.Marshal(current)
	if err := json.Unmarshal(data, &legacy); err != nil || len(legacy.Scopes) != 1 {
		t.Fatal("old decoder rejects new history", err)
	}
}

func TestProofTTLFailedBatchDoesNotPublishExpiry(t *testing.T) {
	now := time.Unix(10000, 0)
	ttl := time.Hour
	s, _ := openTestStore(t, t.TempDir(), Options{ExactBlocks: true, Continuation: batchTestCodec{}, MutationProofScopeTTL: &ttl, Now: func() time.Time { return now }})
	defer closeTestStore(t, s)
	createTestMetric(t, s, "proof")
	old, _ := windowProof(t, s)
	now = now.Add(ttl + time.Nanosecond)
	err := s.UpdateBatch(t.Context(), []MetricUpdate{{Name: "proof", Points: []Point{{Value: 1}}}, {Name: "proof", Points: []Point{{Value: -1}}}}, nil)
	if err == nil {
		t.Fatal("injected error lost")
	}
	if h := ttlHistory(t, s); len(h.Scopes) != 1 {
		t.Fatal("failed batch published tombstone", h)
	}
	if stats := s.ProofStats(); stats.ExpiredScopes != 0 || stats.ExpiredHistories != 0 {
		t.Fatal("failed expiry counted", stats)
	}
	if err := s.UpdateBatch(t.Context(), []MetricUpdate{{Name: "proof", Points: []Point{{Value: 2}}}}, nil); err != nil {
		t.Fatal(err)
	}
	assertNoProofHistory(t, s, "proof")
	if stats := s.ProofStats(); stats.ExpiredScopes != 1 || stats.ExpiredHistories != 1 {
		t.Fatal(stats)
	}
	if fresh, _ := windowProof(t, s); fresh == old {
		t.Fatal("old token survived successful retry")
	}
}

func TestProofTTLLegacyStampPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(10000, 0)
	ttl := time.Hour
	o := Options{ExactBlocks: true, Continuation: noopTTLCodec{}, MutationProofScopeTTL: &ttl, Now: func() time.Time { return now }}
	s, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	createTestMetric(t, s, "proof")
	windowProof(t, s)
	m, _ := s.Metadata(t.Context(), "proof")
	h := ttlHistory(t, s)
	h.Scopes[0].LastRegistered = 0
	data, _ := json.Marshal(h)
	if err := s.db.Set(mutationKey(m), data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	now = now.Add(7 * 24 * time.Hour)
	if err := s.UpdateMany(t.Context(), "proof", nil); err != nil {
		t.Fatal(err)
	}
	stamped := ttlHistory(t, s).Scopes[0].LastRegistered
	if stamped != now.UnixNano() {
		t.Fatal("legacy record expired on first touch")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Minute)
	s, err = Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.UpdateMany(t.Context(), "proof", nil); err != nil {
		t.Fatal(err)
	}
	if got := ttlHistory(t, s).Scopes[0].LastRegistered; got != stamped {
		t.Fatal("restart/write renewed idle scope")
	}
	now = now.Add(30*time.Minute + time.Nanosecond)
	if err := s.UpdateMany(t.Context(), "proof", nil); err != nil {
		t.Fatal(err)
	}
	assertNoProofHistory(t, s, "proof")
}

func TestProofTTLCorruptMetadataFailsClosed(t *testing.T) {
	s := proofTestStore(t, t.TempDir())
	defer s.Close()
	seedProof(t, s)
	windowProof(t, s)
	m, _ := s.Metadata(t.Context(), "proof")
	before := proofPointBytes(t, s, "proof")
	h := ttlHistory(t, s)
	h.Scopes[0].LastRegistered = -1
	data, _ := json.Marshal(h)
	if err := s.db.Set(mutationKey(m), data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if p, _, err := s.MutationProof(t.Context(), "proof", 9890, 9910); err == nil || p != "" {
		t.Fatal("corrupt timestamp returned a selective proof", p, err)
	}
	if err := s.UpdateMany(t.Context(), "proof", []Point{{Timestamp: 9900, Value: 42}}); err == nil {
		t.Fatal("corrupt metadata was silently skipped")
	}
	if !bytes.Equal(before, proofPointBytes(t, s, "proof")) {
		t.Fatal("failed write modified points")
	}
}
