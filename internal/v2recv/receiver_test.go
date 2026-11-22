package v2recv_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/internal/v2recv"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/klauspost/compress/zstd"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func fixtureValue(mode string, conn, ordinal int) float64 {
	if mode == "binary" || mode == "binary-zstd" {
		bits := []uint64{0x8000000000000000, 0x7ff8000000000123, 0x7ff0000000000000, 0xfff0000000000000, 1}
		if ordinal%13 < len(bits) {
			return math.Float64frombits(bits[ordinal%13])
		}
	}
	return float64(conn) + .125
}
func wire(t *testing.T, mode string, conn, rounds int) []byte {
	t.Helper()
	var plain bytes.Buffer
	if mode != "binary" && mode != "binary-zstd" {
		for i := 0; i < rounds; i++ {
			fmt.Fprintf(&plain, "metric.%d %.17g %d\n", i%7, float64(conn)+.125, 100000+conn*rounds+i)
		}
		if mode == "plaintext" {
			return plain.Bytes()
		}
		e, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		return e.EncodeAll(plain.Bytes(), nil)
	}
	flags := byte(0)
	if mode == "binary-zstd" {
		flags = codec.FlagZstd
	}
	e, err := codec.NewEncoder(codec.Options{Flags: flags, MaxRows: 31, MaxNames: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	out, _ := codec.AppendPreamble(nil, flags)
	for i := 0; i < rounds; i++ {
		if err = e.Add([]byte(fmt.Sprintf("metric.%d", i%7)), fixtureValue(mode, conn, i), int64(100000+conn*rounds+i)); err != nil {
			t.Fatal(err)
		}
		if e.Ready() {
			out, err = e.Flush(out)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	out, err = e.Flush(out)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func TestTCP32ConnectionsExactCore(t *testing.T) {
	for _, run := range []struct {
		mode   string
		buffer int
	}{{"plaintext", 0}, {"zstd-plaintext", 0}, {"binary", 0}, {"binary-zstd", 0}, {"plaintext", 300}, {"binary-zstd", 300}} {
		mode := run.mode
		t.Run(fmt.Sprintf("%s-buffer%d", mode, run.buffer), func(t *testing.T) {
			const workers, connections, rounds = 4, 32, 127
			root := t.TempDir()
			store, err := v2daemon.Open(root, v2daemon.Config{Layer: 1, Producers: workers, Shards: 2, MaxPendingRows: 257})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			r, err := store.ReceiveTCP(l, v2recv.Config{BatchSize: 19, BufferBytes: run.buffer, Logger: quiet(), SyncInterval: 20 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for c := 0; c < connections; c++ {
				payload := wire(t, mode, c, rounds)
				wg.Add(1)
				go func() {
					defer wg.Done()
					c, e := net.Dial("tcp", l.Addr().String())
					if e != nil {
						t.Error(e)
						return
					}
					defer c.Close() // fragmented writes exercise preamble and frame boundaries
					for len(payload) > 0 {
						n := min(137, len(payload))
						if _, e = c.Write(payload[:n]); e != nil {
							t.Error(e)
							return
						}
						payload = payload[n:]
					}
				}()
			}
			wg.Wait()
			deadline := time.Now().Add(10 * time.Second)
			for r.Stats().Accepted < connections*rounds && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err = r.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			if got := r.Stats(); got.Accepted != connections*rounds || got.Rejected+got.ParseErrors+got.TransportErrors+got.SinkErrors != 0 {
				t.Fatal(got)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			cats := make([]*partlog.NameCatalog, workers)
			for i := range cats {
				cats[i], err = partlog.OpenNameCatalog(filepath.Join(root, "placement-group-00000001", "journal", fmt.Sprintf("producer-%03d", i), "catalog"))
				if err != nil {
					t.Fatal(err)
				}
				defer cats[i].Close()
			}
			core, err := v2core.Open(filepath.Join(root, "placement-group-00000001", "core"), v2core.Config{JournalIdentity: "core-first-fixed-v1", Producers: workers, Shards: 2, Owners: 2, MaxRows: 2 << 20, PartitionSeconds: 86400, AppendParts: true, RawCodec: "run1"}, v2core.Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			defer core.Close()
			seen := make([]bool, connections*rounds)
			count := 0
			for _, db := range core.DBs() {
				snap := db.NewSnapshot()
				err = v2core.ScanRaw(snap, func(row v2core.Row) error {
					p := int((row.ID - 1) % workers)
					local := (row.ID-1)/workers + 1
					name, _, ok := cats[p].Lookup(local)
					ordinal := int(row.Time) - 100000
					if !ok || ordinal < 0 || ordinal >= len(seen) {
						return fmt.Errorf("invalid row %+v", row)
					}
					c, i := ordinal/rounds, ordinal%rounds
					if seen[ordinal] || name != fmt.Sprintf("metric.%d", i%7) || row.Bits != math.Float64bits(fixtureValue(mode, c, i)) {
						return fmt.Errorf("tuple changed %+v name=%s", row, name)
					}
					seen[ordinal] = true
					count++
					return nil
				})
				snap.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			if count != len(seen) {
				t.Fatalf("got %d want %d", count, len(seen))
			}
		})
	}
}

type fakeOwner struct {
	block   chan struct{}
	entered chan struct{}
	once    sync.Once
	err     error
	syncs   chan struct{}
}

func (o *fakeOwner) Append(_ []string, _ []uint64, p []points.Point, _ []uint64, _ int64) error {
	o.once.Do(func() {
		if o.entered != nil {
			close(o.entered)
		}
	})
	if o.block != nil {
		<-o.block
	}
	return o.err
}
func (o *fakeOwner) Seal() error { return nil }
func (o *fakeOwner) Drain() error {
	if o.syncs != nil {
		select {
		case o.syncs <- struct{}{}:
		default:
		}
	}
	return nil
}
func TestIdleSyncAndFailure(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			o := &fakeOwner{entered: make(chan struct{}), syncs: make(chan struct{}, 10)}
			if failure {
				o.err = errors.New("disk full")
			}
			l, _ := net.Listen("tcp", "127.0.0.1:0")
			s, e := v2recv.Start(l, []v2recv.Owner{o}, v2recv.Config{SyncInterval: 10 * time.Millisecond, Logger: quiet()})
			if e != nil {
				t.Fatal(e)
			}
			c, e := net.Dial("tcp", l.Addr().String())
			if e != nil {
				t.Fatal(e)
			}
			_, _ = io.WriteString(c, "a 1 123\n")
			select {
			case <-o.entered:
			case <-time.After(time.Second):
				t.Fatal("not flushed before idle read")
			}
			if !failure {
				select {
				case <-o.syncs:
				case <-time.After(time.Second):
					t.Fatal("idle sync missing")
				}
			}
			c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := s.Wait(ctx)
			if failure {
				if err == nil || s.Stats().Rejected != 1 || s.Stats().SinkErrors != 1 {
					t.Fatal(err, s.Stats())
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestBackpressureDoesNotDrop(t *testing.T) {
	o := &fakeOwner{block: make(chan struct{}), entered: make(chan struct{})}
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	s, e := v2recv.Start(l, []v2recv.Owner{o}, v2recv.Config{BatchSize: 1, Logger: quiet()})
	if e != nil {
		t.Fatal(e)
	}
	c, _ := net.Dial("tcp", l.Addr().String())
	_, _ = io.WriteString(c, "a 1 1\na 2 2\na 3 3\n")
	<-o.entered
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.Close()
	go func() { done <- s.Wait(ctx) }()
	select {
	case <-done:
		t.Fatal("drain bypassed blocked owner")
	case <-time.After(20 * time.Millisecond):
	}
	close(o.block)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	if got := s.Stats(); got.Accepted != 3 || got.Rejected != 0 {
		t.Fatal(got)
	}
}

func TestMalformedInputsAreCounted(t *testing.T) {
	for _, binary := range []bool{false, true} {
		t.Run(fmt.Sprint(binary), func(t *testing.T) {
			l, _ := net.Listen("tcp", "127.0.0.1:0")
			s, err := v2recv.Start(l, []v2recv.Owner{&fakeOwner{}}, v2recv.Config{Logger: quiet()})
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte("bad\n(metric 1 123\n")
			if binary {
				payload = wire(t, "binary-zstd", 0, 5)
				payload[len(payload)-1] ^= 0x80
			}
			c, _ := net.Dial("tcp", l.Addr().String())
			_, _ = c.Write(payload)
			c.Close()
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				st := s.Stats()
				if st.ParseErrors+st.TransportErrors > 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err = s.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			st := s.Stats()
			if binary {
				if st.Accepted != 0 || st.TransportErrors != 1 {
					t.Fatal(st)
				}
			} else if st.Accepted != 1 || st.Rejected != 1 || st.ParseErrors != 1 {
				t.Fatal(st)
			}
		})
	}
}
