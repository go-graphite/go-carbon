package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestStopBackgroundThenCompact(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	s.StopBackground()
	createTestMetric(t, s, "owner.metric")
	if err := s.Compact(); err != nil {
		t.Fatal("explicit compaction after StopBackground", err)
	}
	if _, err := s.Metadata(context.Background(), "owner.metric"); err != nil {
		t.Fatal("compaction lost the owner's write", err)
	}
}

func TestStopBackgroundThenPurgeInvalidNames(t *testing.T) {
	t.Parallel()
	s, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem()})
	defer closeTestStore(t, s)
	s.StopBackground()
	m := createTestMetric(t, s, "owner.metric")
	m.Name = strings.Repeat("x", maxMetricNameBytes+1)
	encoded, err := s.encodeCatalog(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Set(catalogKey(m.Name), encoded, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if n, err := s.PurgeInvalidNames(context.Background()); err != nil || n != 1 {
		t.Fatalf("explicit purge after StopBackground: removed=%d err=%v", n, err)
	}
	if _, err := metadataFrom(s.db, m.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("invalid catalog name survived", err)
	}
	if n, err := s.PurgeInvalidNames(context.Background()); err != nil || n != 0 {
		t.Fatalf("repeated purge: removed=%d err=%v", n, err)
	}
}

// A 1 byte/s compaction budget holds a real Pebble Compact in flight until
// close releases pacing. Both close paths must join the caller and prevent
// another span or operation from touching the closed database.
func TestExplicitCompactCloseAfterStopBackground(t *testing.T) {
	for _, forExit := range []bool{false, true} {
		t.Run(map[bool]string{false: "Close", true: "CloseForExit"}[forExit], func(t *testing.T) {
			s, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true,
				CompactionRate: 1, L0CompactionThreshold: 10, L0StopWritesThreshold: 20})
			closed := false
			defer func() {
				if !closed {
					closeTestStore(t, s)
				}
			}()
			s.StopBackground()
			for generation := range 2 {
				if err := s.db.Set([]byte("p.block"), bytes.Repeat([]byte{byte(generation)}, 1024), pebble.NoSync); err != nil {
					t.Fatal(err)
				}
				if err := s.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			compacted := make(chan error, 1)
			go func() { compacted <- s.Compact() }()
			deadline := time.Now().Add(time.Minute)
			for {
				s.compactionPacing.mu.Lock()
				var compacting bool
				for _, f := range s.compactionPacing.files {
					compacting = compacting || f.compacting.Load()
				}
				s.compactionPacing.mu.Unlock()
				if compacting {
					break
				}
				select {
				case err := <-compacted:
					t.Fatal("explicit compaction never reached Pebble", err)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("compaction did not start")
				}
				time.Sleep(time.Millisecond)
			}
			// Close must release operational pacing and join the admitted
			// caller. The deadline detects a hang, not host-dependent speed.
			closeDone := make(chan error, 1)
			go func() {
				if forExit {
					closeDone <- s.CloseForExit()
				} else {
					closeDone <- s.Close()
				}
			}()
			select {
			case err := <-closeDone:
				closed = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Minute):
				t.Fatal("close did not release compaction pacing")
			}
			select {
			case err := <-compacted:
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, pebble.ErrClosed) && !errors.Is(err, pebble.ErrCancelledCompaction) {
					t.Fatal(err)
				}
			case <-time.After(time.Minute):
				t.Fatal("close returned before joining explicit compaction")
			}
			if err := s.Compact(); !errors.Is(err, context.Canceled) {
				t.Fatal("compaction admitted after close", err)
			}
			if err := s.DowngradeFloatCodec(context.Background()); !errors.Is(err, context.Canceled) {
				t.Fatal("float downgrade admitted after close", err)
			}
		})
	}
}

type closeColdCodec struct {
	testColdCodec
	entered, cancelled, release chan struct{}
}

func (c closeColdCodec) Repack(ctx context.Context, _ ContinuationIO, _ ColdPackRequest) (RepackStats, uint64, bool, error) {
	close(c.entered)
	<-ctx.Done()
	close(c.cancelled)
	<-c.release
	return RepackStats{}, 0, false, ctx.Err()
}

func TestCloseJoinsExplicitColdPack(t *testing.T) {
	for _, step := range []bool{false, true} {
		t.Run(map[bool]string{false: "RunColdPack", true: "ColdPackStep"}[step], func(t *testing.T) {
			codec := closeColdCodec{entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
			defer close(codec.release)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, _ := openTestStore(t, t.TempDir(), Options{fs: vfs.NewMem(), ExactBlocks: true, TimeBuckets: true,
				Continuation: codec, CacheSize: 1 << 20, MemTableSize: 1 << 20})
			s.StopBackground()
			createTestMetric(t, s, "owner.cold")
			if err := s.ConfigureColdPack(ColdPackOptions{Mode: "exact"}); err != nil {
				t.Fatal(err)
			}
			packed := make(chan error, 1)
			go func() {
				if step {
					_, err := s.ColdPackStep(ctx)
					packed <- err
				} else {
					packed <- s.RunColdPack(ctx)
				}
			}()
			select {
			case <-codec.entered:
			case <-time.After(time.Minute):
				t.Fatal("explicit cold pack did not enter codec")
			}
			// Repeated background stops leave owner-driven work alive.
			s.StopBackground()
			select {
			case <-codec.cancelled:
				t.Fatal("StopBackground cancelled explicit cold pack")
			default:
			}
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			select {
			case <-codec.cancelled:
			case <-time.After(time.Minute):
				t.Fatal("Close did not cancel explicit cold pack")
			}
			select {
			case err := <-closed:
				t.Fatal("Close did not join the codec", err)
			default:
			}
			if err := s.Compact(); !errors.Is(err, context.Canceled) {
				t.Fatal("Close admitted another compaction", err)
			}
			// Release the codec before waiting for close, while keeping failure
			// paths from leaving it blocked.
			codec.release <- struct{}{}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Minute):
				t.Fatal("Close did not join explicit cold pack promptly")
			}
			if err := <-packed; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if _, err := s.ColdPackStep(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("ColdPackStep admitted after close", err)
			}
			if err := s.RunColdPack(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("RunColdPack admitted after close", err)
			}
			if err := s.DowngradeColdPack(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("DowngradeColdPack admitted after close", err)
			}
			if err := s.DowngradeFloatCodec(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("DowngradeFloatCodec admitted after close", err)
			}
			if _, err := s.PurgeInvalidNames(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("PurgeInvalidNames admitted after close", err)
			}
		})
	}
}
