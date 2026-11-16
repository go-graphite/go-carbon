package chunkstore

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type frozenWorkspaceCodec struct {
	bucketCodec
	entered, canceled, finish chan struct{}
}

type testFrozenDecoder struct {
	c  *frozenWorkspaceCodec
	io ContinuationIO
	m  Metadata
}

func (c *frozenWorkspaceCodec) NewFrozenReadDecoder(io ContinuationIO, m Metadata, _ func() time.Time) (FrozenReadDecoder, error) {
	return &testFrozenDecoder{c: c, io: io, m: m}, nil
}
func (d *testFrozenDecoder) Close() error { return nil }
func (d *testFrozenDecoder) Fetch(ctx context.Context, from, until int) (*Series, error) {
	if d.c.entered != nil {
		close(d.c.entered)
		<-ctx.Done()
		close(d.c.canceled)
		<-d.c.finish
		return nil, ctx.Err()
	}
	if _, err := d.io.Get([]byte("plain")); err != nil {
		return nil, err
	}
	return &Series{Metadata: d.m, FromTime: from, UntilTime: until, Step: 1, Values: []float64{7}}, nil
}

func openFrozenWorkspaceStore(t *testing.T, codec *frozenWorkspaceCodec) (*Store, *FrozenMetric) {
	t.Helper()
	fs := vfs.NewMem()
	opts := Options{fs: fs, ExactBlocks: true, Continuation: codec}
	s, _ := openTestStore(t, "/frozen", opts)
	m := createTestMetric(t, s, "frozen.workspace")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	opts.ReadOnly = true
	s, err := Open("/frozen", opts)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := s.ResolveFrozen(context.Background(), m.Name)
	if err != nil {
		t.Fatal(err)
	}
	return s, frozen
}

func TestFrozenWorkspaceDoesNotRetainReadLifetime(t *testing.T) {
	s, m := openFrozenWorkspaceStore(t, &frozenWorkspaceCodec{})
	var workspace FrozenReadWorkspace
	consume := func(_, _, _ int, values []float64) error {
		if len(values) != 1 || values[0] != 7 {
			t.Fatal("workspace result")
		}
		return nil
	}
	for range 3 {
		if err := s.FetchFrozenValuesWithWorkspace(context.Background(), m, 1, 2, &workspace, consume); err != nil {
			t.Fatal(err)
		}
		if s.readCount != 0 || workspace.io.ctx != nil || workspace.io.reader != nil || workspace.io.state != nil || workspace.io.iter != nil {
			t.Fatal("workspace retained an active read")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.FetchFrozenValuesWithWorkspace(ctx, m, 1, 2, &workspace, consume); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request: %v", err)
	}
	if err := s.FetchFrozenValuesWithWorkspace(context.Background(), m, 1, 2, &workspace, consume); err != nil {
		t.Fatal("cancellation poisoned next request: ", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.FetchFrozenValuesWithWorkspace(context.Background(), m, 1, 2, &workspace, consume); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed store: %v", err)
	}
	if err := workspace.Reset(); err != nil {
		t.Fatal(err)
	}
}

func TestFrozenWorkspaceCloseCancelsActiveRead(t *testing.T) {
	pebbleSynctest(t, func(t *testing.T) {
		codec := &frozenWorkspaceCodec{entered: make(chan struct{}), canceled: make(chan struct{}), finish: make(chan struct{})}
		s, m := openFrozenWorkspaceStore(t, codec)
		var workspace FrozenReadWorkspace
		consume := func(_, _, _ int, _ []float64) error { return nil }
		read := make(chan error, 1)
		go func() { read <- s.FetchFrozenValuesWithWorkspace(context.Background(), m, 1, 2, &workspace, consume) }()
		<-codec.entered
		if err := s.FetchFrozenValuesWithWorkspace(context.Background(), m, 1, 2, &workspace, consume); !errors.Is(err, ErrFrozenWorkspaceBusy) {
			t.Fatalf("concurrent workspace use: %v", err)
		}
		closed := make(chan error, 1)
		go func() { closed <- s.Close() }()
		<-codec.canceled
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close released active view: %v", err)
		default:
		}
		close(codec.finish)
		if err := <-read; !errors.Is(err, context.Canceled) {
			t.Fatalf("active read cancellation: %v", err)
		}
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if err := workspace.Reset(); err != nil {
			t.Fatal(err)
		}
	})
}
