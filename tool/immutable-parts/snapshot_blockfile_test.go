//go:build sealrollup_integration

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	pr "github.com/go-graphite/go-carbon/internal/partsread"
)

// This opt-in fixture isolates an immutable manifest's direct block locator.
// It is deliberately not a production part format or a replacement for GCP3.
// The native comparator still reopens its engine; this source has no engine
// cache. Both retain the uncontrolled OS page cache, and file handles reopen.
func fixtureBlockLocator(identity []byte, offset int64, payload []byte) []byte {
	key := append([]byte(nil), identity...)
	key = binary.BigEndian.AppendUint64(key, uint64(offset))
	key = binary.BigEndian.AppendUint32(key, uint32(len(payload)))
	return binary.BigEndian.AppendUint32(key, crc32.ChecksumIEEE(payload))
}

func fixtureBlockFileReader(file *os.File, stages *latencyStages) pr.ReadSlotBlock {
	return func(ctx context.Context, key []byte, _ []pr.SlotRange, yield func(pr.Sample) error) error {
		if len(key) < 16 {
			return fmt.Errorf("invalid fixture block locator")
		}
		loc := key[len(key)-16:]
		offset, size := int64(binary.BigEndian.Uint64(loc)), int(binary.BigEndian.Uint32(loc[8:]))
		if offset < 0 || size <= 0 || size > 512*24 || size%24 != 0 {
			return fmt.Errorf("invalid fixture block extent")
		}
		var scratch [512 * 24]byte
		payload := scratch[:size]
		var start time.Time
		if stages != nil {
			start = time.Now()
		}
		_, err := file.ReadAt(payload, offset)
		if stages != nil {
			stages.Seek += time.Since(start).Nanoseconds()
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(loc[12:]) {
			return fmt.Errorf("fixture block checksum mismatch")
		}
		for len(payload) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if stages != nil {
				start = time.Now()
			}
			p := pr.Sample{Time: int64(binary.LittleEndian.Uint64(payload)), Bits: binary.LittleEndian.Uint64(payload[8:]), Sequence: binary.LittleEndian.Uint64(payload[16:])}
			if stages != nil {
				stages.Decode += time.Since(start).Nanoseconds()
				stages.Records++
				start = time.Now()
			}
			if err := yield(p); err != nil {
				return err
			}
			if stages != nil {
				stages.Emit += time.Since(start).Nanoseconds()
			}
			payload = payload[24:]
		}
		return nil
	}
}

func TestFixtureBlockFileRoundTrip(t *testing.T) {
	want := []pr.Sample{{Time: 60, Bits: 0x8000000000000000, Sequence: 1}, {Time: 660, Bits: 0x7ff8000000000042, Sequence: 2}, {Time: 60, Bits: 0xfff0000000000000, Sequence: ^uint64(0)}}
	var payload []byte
	for _, p := range want {
		payload = binary.LittleEndian.AppendUint64(payload, uint64(p.Time))
		payload = binary.LittleEndian.AppendUint64(payload, p.Bits)
		payload = binary.LittleEndian.AppendUint64(payload, p.Sequence)
	}
	file, err := os.OpenFile(filepath.Join(t.TempDir(), "part"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err = file.Write(payload); err != nil {
		t.Fatal(err)
	}
	key := fixtureBlockLocator([]byte("pg/vshard/full-series/archive/block"), 0, payload)
	read := fixtureBlockFileReader(file, nil)
	var got []pr.Sample
	if err = read(context.Background(), key, nil, func(p pr.Sample) error { got = append(got, p); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bits changed %v != %v", got, want)
	}
	if _, err = file.WriteAt([]byte{0xff}, 8); err != nil {
		t.Fatal(err)
	}
	if err = read(context.Background(), key, nil, func(pr.Sample) error { t.Fatal("corrupt block emitted data"); return nil }); err == nil {
		t.Fatal("corruption accepted")
	}
}
