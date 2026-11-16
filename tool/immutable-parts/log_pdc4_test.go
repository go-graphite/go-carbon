package main

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-graphite/go-carbon/internal/partlog"
)

func TestNumericPDC4SeekAndReopen(t *testing.T) {
	dir := t.TempDir()
	cs, err := openReplayCatalogs(dir, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	want := []row{{ID: 0, Time: math.MinInt64, Bits: 1 << 63, Seq: math.MaxUint64}, {ID: 17, Time: -7, Bits: 0x7ff8000000000042, Seq: 0}, {ID: math.MaxUint64, Time: math.MaxInt64, Bits: 0x7ff0000000000000, Seq: 7}}
	first, err := cs[0].encode(want[:1])
	if err != nil {
		t.Fatal(err)
	}
	second, err := cs[0].encode(want)
	if err != nil {
		t.Fatal(err)
	}
	if err = closeReplayCatalogs(cs); err != nil {
		t.Fatal(err)
	}
	cs, err = openReplayCatalogs(dir, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplayCatalogs(cs)
	got, err := cs[0].decode(nil, second) // deliberately skip the first record
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("seek/reopen: %v %v", got, err)
	}
	got, err = cs[0].decode(got, first)
	if err != nil || !reflect.DeepEqual(got, want[:1]) {
		t.Fatalf("independent reset: %v %v", got, err)
	}
	second[len(second)/2] ^= 1
	if _, err = cs[0].decode(nil, second); err == nil {
		t.Fatal("accepted corruption")
	}
}

func TestNumericPDC4MixedReplay(t *testing.T) {
	root := t.TempDir()
	logDir := filepath.Join(root, "log")
	cs, err := openReplayCatalogs(logDir, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	opts := partlog.Options{Streams: 2}
	j, err := partlog.Open(logDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]row{{{ID: 1, Time: 20, Bits: math.Float64bits(5), Seq: 3}}, {{ID: 2, Time: -7, Bits: 1 << 63, Seq: 4}}}
	for p := range 2 {
		if _, err = j.Append(p, 9, encodeLogRows(want[p])); err != nil {
			t.Fatal(err)
		}
		b, e := cs[p].encode(want[p])
		if e != nil {
			t.Fatal(e)
		}
		if binary.LittleEndian.Uint32(b[:4]) == 0 {
			t.Fatal("missing codec discriminator")
		}
		if _, err = j.Append(p, 10, b); err != nil {
			t.Fatal(err)
		}
	}
	if err = j.Close(); err != nil {
		t.Fatal(err)
	}
	if err = closeReplayCatalogs(cs); err != nil {
		t.Fatal(err)
	}
	cfg := coreConfig{AppendParts: true, Shards: 2, Producers: 2, MaxRows: 100, PartitionSeconds: 3600}
	l, err := openLoggedCore(logDir, filepath.Join(root, "parts"), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.replay(); err != nil {
		t.Fatal(err)
	}
	if err = l.close(); err != nil {
		t.Fatal(err)
	}
	// Every durable cursor is already applied: seeking beyond predictor epochs
	// must remain safe and must not re-apply the retained log on restart.
	l, err = openLoggedCore(logDir, filepath.Join(root, "parts"), cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.replay(); err != nil {
		t.Fatal(err)
	}
	if err = l.close(); err != nil {
		t.Fatal(err)
	}
}
