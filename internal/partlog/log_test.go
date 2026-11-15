package partlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

func TestOrderedBitsClockAndRestart(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Streams: 2, SegmentBytes: 180, SyncInterval: time.Hour}
	l, e := Open(dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	identity := l.Identity()
	bits := []uint64{0x7ff8000000000042, 0x8000000000000000, 1, 0xfff0000000000000}
	for p := 0; p < 2; p++ {
		for i, v := range bits {
			b := binary.LittleEndian.AppendUint64(nil, v)
			c, e := l.Append(p, int64(-i), b)
			if e != nil || c != uint64(i+1) {
				t.Fatal(c, e)
			}
		}
	}
	if e = l.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = l.Close(); e != nil {
		t.Fatal(e)
	}
	l, e = Open(dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	if identity != l.Identity() {
		t.Fatal("identity changed")
	}
	for p := 0; p < 2; p++ {
		var got []uint64
		e = l.Replay(p, 1, func(r Record) error {
			if r.Clock != -int64(r.Cursor-1) {
				return fmt.Errorf("clock changed: %+v", r)
			}
			got = append(got, binary.LittleEndian.Uint64(r.Payload))
			return nil
		})
		if e != nil || !slices.Equal(got, bits[1:]) {
			t.Fatal(got, e)
		}
	}
}
func TestMinimumFrontierRetainsSlowShard(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Streams: 1, SegmentBytes: 100, SyncInterval: time.Hour}
	l, e := Open(dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 8; i++ {
		if _, e = l.Append(0, 1, bytes.Repeat([]byte{byte(i)}, 24)); e != nil {
			t.Fatal(e)
		}
	}
	count := func() int { files, _ := filepath.Glob(filepath.Join(dir, "*.plog")); return len(files) }
	initial := count()
	if e = l.Truncate([][]uint64{{8}, {2}}); e != nil {
		t.Fatal(e)
	}
	if count() != initial-1 {
		t.Fatalf("slow frontier lost history initial%d now%d", initial, count())
	}
	var cursors []uint64
	if e = l.Replay(0, 2, func(r Record) error { cursors = append(cursors, r.Cursor); return nil }); e != nil {
		t.Fatal(e)
	}
	if !slices.Equal(cursors, []uint64{3, 4, 5, 6, 7, 8}) {
		t.Fatal(cursors)
	}
	if e = l.Truncate([][]uint64{{8}, {8}}); e != nil {
		t.Fatal(e)
	}
	if count() != 2 {
		t.Fatal("trim exceeded synced cursor", count())
	}
	if e = l.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = l.Truncate([][]uint64{{8}, {8}}); e != nil {
		t.Fatal(e)
	}
	if count() != 1 {
		t.Fatal(count())
	}
	if e = l.Close(); e != nil {
		t.Fatal(e)
	}
	l, e = Open(dir, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	c, e := l.Append(0, 1, []byte{9})
	if e != nil || c != 9 {
		t.Fatal(c, e)
	}
	if e = l.Replay(0, 0, func(Record) error { return nil }); e == nil {
		t.Fatal("missing retained prefix silently accepted")
	}
}
func TestTornTailAndCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Streams: 1}
			l, e := Open(dir, opts)
			if e != nil {
				t.Fatal(e)
			}
			_, e = l.Append(0, 1, []byte("complete"))
			if e != nil {
				t.Fatal(e)
			}
			if e = l.Close(); e != nil {
				t.Fatal(e)
			}
			files, _ := filepath.Glob(filepath.Join(dir, "*.plog"))
			f, e := os.OpenFile(files[0], os.O_RDWR|os.O_APPEND, 0600)
			if e != nil {
				t.Fatal(e)
			}
			if corrupt {
				_ = f.Close()
				f, e = os.OpenFile(files[0], os.O_RDWR, 0600)
				if e != nil {
					t.Fatal(e)
				}
				_, e = f.WriteAt([]byte{'X'}, headerSize+headerSize)
			} else {
				_, e = f.Write([]byte("torn"))
			}
			if e != nil {
				t.Fatal(e)
			}
			_ = f.Close()
			l, e = Open(dir, opts)
			if corrupt {
				if e == nil {
					_ = l.Close()
					t.Fatal("checksum failure accepted")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			defer l.Close()
			var n int
			if e = l.Replay(0, 0, func(r Record) error {
				n++
				if string(r.Payload) != "complete" {
					t.Fatal(r)
				}
				return nil
			}); e != nil || n != 1 {
				t.Fatal(n, e)
			}
		})
	}
}
func TestPeriodicSyncErrorAndCallback(t *testing.T) {
	var syncs atomic.Int32
	l, e := Open(t.TempDir(), Options{Streams: 1, SyncInterval: 10 * time.Millisecond, Hook: func(_ int, phase string) {
		if phase == "after-sync" {
			syncs.Add(1)
		}
	}})
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	if _, e = l.Append(0, 1, []byte{1}); e != nil {
		t.Fatal(e)
	}
	until := time.Now().Add(time.Second)
	for syncs.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if syncs.Load() != 1 {
		t.Fatal(syncs.Load())
	}
	sentinel := errors.New("apply failed")
	if e = l.Replay(0, 0, func(Record) error { return sentinel }); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	if e = l.Truncate([][]uint64{{2}}); e == nil || !strings.Contains(e.Error(), "tail") {
		t.Fatal(e)
	}
}

func TestBatchBoundaryBitsAndPolicy(t *testing.T) {
	want := Batch{Metric: "x;a=z;b=y", PolicyEpoch: 17, Points: []points.Point{{Value: math.Float64frombits(0x7ff8000000000042), Timestamp: -17}, {Value: math.Float64frombits(0x8000000000000000), Timestamp: 1}}}
	raw, e := EncodeBatch(want)
	if e != nil {
		t.Fatal(e)
	}
	got, e := DecodeBatch(raw)
	if e != nil {
		t.Fatal(e)
	}
	if got.Metric != want.Metric || got.PolicyEpoch != want.PolicyEpoch || len(got.Points) != len(want.Points) {
		t.Fatal(got)
	}
	for i, p := range got.Points {
		if math.Float64bits(p.Value) != math.Float64bits(want.Points[i].Value) || p.Timestamp != want.Points[i].Timestamp {
			t.Fatal(i, p)
		}
	}
	if _, e = DecodeBatch(raw[:len(raw)-1]); e == nil {
		t.Fatal("truncated batch accepted")
	}
}

func TestAsyncSyncFailureIsReported(t *testing.T) {
	var log *Log
	var e error
	log, e = Open(t.TempDir(), Options{Streams: 1, SyncInterval: 5 * time.Millisecond, Hook: func(_ int, phase string) {
		if phase == "after-write" {
			_ = log.streams[0].file.Close()
		}
	}})
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	if _, e = log.Append(0, 1, []byte{1}); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-log.Errors():
		if e == nil {
			t.Fatal("missing sync failure")
		}
	case <-time.After(time.Second):
		t.Fatal("background sync failure was silent")
	}
	if _, e = log.Append(0, 2, []byte{2}); e == nil {
		t.Fatal("append succeeded after fatal sync failure")
	}
}
