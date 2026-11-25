package tcp

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/klauspost/compress/zstd"
)

// This test is also run unchanged with the original cache sources through a Go
// overlay. The saved transcript includes exact persisted float bits and order.
func TestStreamAdmissionMalformedNameReplay(t *testing.T) {
	var transcript []byte
	for _, flags := range []byte{0, codec.FlagZstd} {
		for _, badLength := range []int{0, codec.DefaultMaxNameSize + 1} {
			c := cache.New()
			c.SetMaxSize(0)
			c.SetThrottleWithHash(func(p *points.Points, _ bool, _ uint64, state *any) bool {
				n, _ := (*state).(int)
				*state = n + 1
				return p.Metric == "throttled"
			})
			e, err := codec.NewEncoder(codec.Options{Flags: flags, MaxNames: 2, MaxRows: 17})
			if err != nil {
				t.Fatal(err)
			}
			wire, err := codec.AppendPreamble(nil, flags)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 100 {
				name := []string{"duplicate", "throttled", "invalid;tag", "other"}[i/9%4]
				if err := e.Add([]byte(name), math.Float64frombits(uint64(i)*0x7123456789abcdef), int64(i%7)-3); err != nil {
					t.Fatal(err)
				}
			}
			wire, err = e.Flush(wire)
			e.Close()
			if err != nil {
				t.Fatal(err)
			}
			// A reset makes the malformed definition's first id unambiguous.
			wire = append(wire, codec.ResetFrame)
			wire = append(wire, make([]byte, codec.HeaderSize-1)...)
			raw := binary.AppendUvarint(nil, 0)
			raw = binary.AppendUvarint(raw, 1)
			raw = binary.AppendUvarint(raw, uint64(badLength))
			raw = append(raw, strings.Repeat("x", badLength)...)
			payload := raw
			if flags != 0 {
				z, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
				if err != nil {
					t.Fatal(err)
				}
				payload = z.EncodeAll(raw, nil)
				z.Close()
			}
			frame := make([]byte, codec.HeaderSize)
			binary.BigEndian.PutUint32(frame[4:8], uint32(len(payload)))
			binary.BigEndian.PutUint32(frame[8:12], uint32(len(raw)))
			binary.BigEndian.PutUint32(frame[12:16], 1)
			binary.BigEndian.PutUint32(frame[16:20], crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
			wire = append(wire, append(frame, payload...)...)
			_, stats, logs := runStream(t, wire, func(r *TCP) {
				r.SetStreamNameResolver(c.ResolveStreamName)
				r.SetStreamPointOut(c.AddStreamPoint)
				r.SetStreamPointsOut(c.AddStreamPoints)
			})
			if stats["streamDecodeErrors"] != 1 || logs != 1 {
				t.Fatal("malformed definition was not rejected", stats, logs)
			}
			for _, name := range []string{"duplicate", "throttled", "invalid;tag", "other"} {
				p, ok := c.Pop(name)
				if name == "throttled" && ok {
					t.Fatal("throttled name accepted")
				}
				if p != nil {
					transcript = append(transcript, p.AppendBinary(nil)...)
				}
			}
		}
	}
	if len(transcript) == 0 {
		t.Fatal("empty transcript")
	}
	if path := os.Getenv("STREAM_TCP_REPLAY_OUTPUT"); path != "" {
		if err := os.WriteFile(path, transcript, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Every replay accepted valid prefix frames before rejecting the bad suffix.
	if !bytes.Contains(transcript, []byte("duplicate")) {
		t.Fatal("valid prefix lost")
	}
}
