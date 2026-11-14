package chunkstore

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

// Queue real Fetch/Metadata calls behind every admission slot, then close the
// store before releasing those slots. On the unfixed store, releasing the slots
// wakes Fetch into NewReadView on a closed DB and panics with "pebble: closed".
func TestCloseCancelsQueuedReads(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		s, now := openTestStore(t, "/store", Options{fs: vfs.NewMem()})
		m := createTestMetric(t, s, "queued.read")
		admitted, release, err := s.AdmitRead(context.Background(), m.Name)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if err := s.readSlots.Acquire(context.Background(), s.Stats().ReadCapacity-1); err != nil {
			t.Fatal(err)
		}
		const readers = 128
		done := make(chan error, readers*3)
		for range readers {
			go func() { _, err := s.Fetch(context.Background(), m.Name, now-60, now); done <- err }()
			go func() { _, err := s.Metadata(context.Background(), m.Name); done <- err }()
			go func() { _, r, err := s.AdmitRead(context.Background(), m.Name); r(); done <- err }()
		}
		synctest.Wait()
		if len(done) != 0 {
			t.Fatal("read escaped a full admission queue")
		}
		err = s.Close()
		if err != nil {
			t.Fatal(err)
		}
		// A held admission is not an active view, so it must not block Close.
		// Release after Close as queued HTTP handlers did during shutdown.
		s.readSlots.Release(s.Stats().ReadCapacity - 1)
		release()
		for range readers * 3 {
			if err := <-done; err == nil {
				t.Fatal("queued read succeeded after Close")
			}
		}
		if _, err := s.Fetch(admitted, m.Name, now-60, now); err == nil {
			t.Fatal("stale admission bypassed Close")
		}
		checks := []struct {
			name string
			read func() error
		}{
			{"fetch", func() error { _, e := s.Fetch(context.Background(), m.Name, now-60, now); return e }},
			{"metadata", func() error { _, e := s.Metadata(context.Background(), m.Name); return e }},
			{"list", func() error { _, e := s.List(context.Background(), ""); return e }},
			{"catalog", func() error { _, e := s.ListCatalogPage(context.Background(), "", "", 10); return e }},
			{"snapshot", func() error { _, e := s.Snapshot(context.Background(), m.Name); return e }},
			{"watermark", func() error { _, e := s.CatalogHighWatermark(context.Background()); return e }},
		}
		for _, check := range checks {
			if err := check.read(); err == nil {
				t.Errorf("%s succeeded after Close", check.name)
			}
		}
	})
}

type closeReadCodec struct {
	bucketCodec
	entered, canceled, finish chan struct{}
}

func (c closeReadCodec) Fetch(ctx context.Context, io ContinuationIO, _ Metadata, _, _ int, _ func() time.Time) (*Series, error) {
	close(c.entered)
	<-ctx.Done()
	close(c.canceled)
	<-c.finish
	// The view is still alive until the codec and all its handles return.
	if _, err := io.(ContinuationProber).Probe([]byte("plain")); err != nil {
		return nil, err
	}
	return nil, ctx.Err()
}

func TestCloseCancelsActiveReadBeforeClosingView(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := closeReadCodec{entered: make(chan struct{}), canceled: make(chan struct{}), finish: make(chan struct{})}
		s, now := openTestStore(t, "/store", Options{fs: vfs.NewMem(), ExactBlocks: true, Continuation: codec})
		m := createTestMetric(t, s, "active.read")
		read := make(chan error, 1)
		go func() { _, err := s.Fetch(context.Background(), m.Name, now-60, now); read <- err }()
		<-codec.entered
		closed := make(chan error, 1)
		go func() { closed <- s.Close() }()
		<-codec.canceled
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close finished before read released its view: %v", err)
		default:
		}
		close(codec.finish)
		if err := <-read; err == nil {
			t.Fatal("active read did not observe cancellation")
		}
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
	})
}

// CloseForExit deliberately leaves Pebble cleanup goroutines for process exit,
// so exercise it outside a synctest bubble (which requires every goroutine to end).
func TestCloseForExitRejectsHeldReadAdmission(t *testing.T) {
	s, now := openTestStore(t, "/store", Options{fs: vfs.NewMem()})
	m := createTestMetric(t, s, "exit.read")
	ctx, release, err := s.AdmitRead(context.Background(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := s.CloseForExit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Fetch(ctx, m.Name, now-60, now); err == nil {
		t.Fatal("held admission bypassed CloseForExit")
	}
	if _, err := s.Metadata(context.Background(), m.Name); err == nil {
		t.Fatal("metadata bypassed CloseForExit")
	}
}
