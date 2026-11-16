package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

func TestNativeCommittedWindowSource(t *testing.T) {
	ctx := context.Background()
	donor, now := openTestStore(t, "/donor", Options{fs: vfs.NewMem(), ExactBlocks: true, CacheSize: 1 << 20, MemTableSize: 1 << 20})
	defer closeTestStore(t, donor)
	createTestMetric(t, donor, "hot")
	ts := int64((now - 60) / 60 * 60)
	if err := donor.UpdateMany(ctx, "hot", []Point{{Timestamp: ts, Value: math.Copysign(0, -1)}}); err != nil {
		t.Fatal(err)
	}
	donor.SetNativePendingCheck(func(string) bool { return true })
	ordinary, err := donor.PinNativeSource()
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Close()
	pin, err := donor.PinNativeWindowSource()
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Close()
	// Both a permanently pending name and later committed revisions are expected
	// during live historical repair. The pinned value must remain bit exact.
	if err := donor.UpdateMany(ctx, "hot", []Point{{Timestamp: ts, Value: 42}}); err != nil {
		t.Fatal(err)
	}
	window := &NativeWindow{From: ts, Until: ts + 1}
	for _, test := range []struct {
		source *NativeSource
		found  bool
	}{{ordinary, false}, {pin, true}} {
		var wire bytes.Buffer
		if err := test.source.WriteWindow(ctx, []string{"hot"}, &wire, window); err != nil {
			t.Fatal(err)
		}
		stream, err := NewTransferStream(&wire)
		if err != nil {
			t.Fatal(err)
		}
		record, err := stream.Next()
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := stream.Finish(); err != nil {
			t.Fatal(err)
		}
		if (record.Version.ID != 0) != test.found {
			t.Fatalf("unexpected pending fence: %+v", record)
		}
		if test.found {
			snap, err := donor.nativeRecordSnapshot(ctx, record)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, archive := range snap.Archives {
				for _, p := range archive.Points {
					if p.Timestamp == ts {
						found = true
						if math.Float64bits(p.Value) != math.Float64bits(math.Copysign(0, -1)) {
							t.Fatal("pinned bits changed", p)
						}
					}
				}
			}
			if !found {
				t.Fatal("committed point missing")
			}
		}
	}
	for _, window := range []*NativeWindow{nil, {From: time.Now().Unix(), Until: time.Now().Unix() + 60}} {
		var wire bytes.Buffer
		if err := pin.WriteWindow(ctx, []string{"hot"}, &wire, window); !errors.Is(err, ErrFormat) || wire.Len() != 0 {
			t.Fatal("window pin escaped historical scope", err, wire.Len())
		}
	}
}
