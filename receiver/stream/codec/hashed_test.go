package codec

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestHashedEncoderParity(t *testing.T) {
	for _, flags := range []byte{0, FlagZstd} {
		for _, maxNames := range []int{1, 2, 32, 1000} {
			for _, frame := range []int{96, 256, 4096} {
				for _, collide := range []bool{false, true} {
					t.Run(fmt.Sprintf("flags=%d/names=%d/frame=%d/collide=%v", flags, maxNames, frame, collide), func(t *testing.T) {
						hash := nameHash
						if collide {
							// Caller-supplied hashes are the test hook: force every
							// distinct name into one chain, including dictionary id 0.
							hash = func([]byte) uint64 { return 0 }
						}
						checkHashedParity(t, Options{Flags: flags, MaxNames: maxNames, MaxFrameSize: frame, MaxRows: 17}, []byte{0, 3, 1, 3, 17, 31, 3, 0, 1, 31}, hash)
					})
				}
			}
		}
	}
}

func checkHashedParity(t *testing.T, opts Options, data []byte, hash func([]byte) uint64) {
	t.Helper()
	oracle := encoder(t, opts)
	hashed, err := NewHashedEncoder(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer hashed.Close()
	var want, got []byte
	flushBoth := func() {
		t.Helper()
		var e1, e2 error
		want, e1 = oracle.Flush(want)
		got, e2 = hashed.Flush(got)
		if !errors.Is(e1, e2) || !bytes.Equal(want, got) {
			t.Fatalf("flush differs: errors %v / %v, bytes %d / %d", e1, e2, len(want), len(got))
		}
	}
	for repeat := range 4 {
		for i, token := range data {
			// Recreate each byte slice so pointer identity cannot stand in for
			// name equality. Include rejected empty and over-limit definitions.
			name := fmt.Appendf(nil, "example.shared.component.metric.%d", token%32)
			if token == 254 {
				name = nil
			} else if token == 255 {
				name = bytes.Repeat([]byte{'x'}, opts.MaxFrameSize)
			}
			value := math.Float64frombits(uint64(token)<<56 | uint64(i+repeat))
			ts := int64(i) - int64(repeat)*1700000000
			d1, r1, e1 := oracle.AddObserved(name, value, ts)
			d2, r2, e2 := hashed.AddHashed(name, hash(name), value, ts)
			if !errors.Is(e1, e2) || d1 != d2 || r1 != r2 || oracle.Ready() != hashed.Ready() || oracle.NameCount() != hashed.NameCount() {
				t.Fatalf("add differs at %d: (%v,%v,%v) / (%v,%v,%v)", i, d1, r1, e1, d2, r2, e2)
			}
			// Neither the dictionary nor a collision entry may borrow name.
			clear(name)
			// Sometimes add past Ready to exercise Add's implicit frame split.
			if (oracle.Ready() && i%3 == 0) || token%7 == 0 {
				flushBoth()
			}
			if token%29 == 0 {
				if e1, e2 := oracle.Reset(), hashed.Reset(); !errors.Is(e1, e2) {
					t.Fatalf("reset differs: %v / %v", e1, e2)
				}
			}
		}
	}
	flushBoth()
}

func FuzzHashedEncoderParity(f *testing.F) {
	f.Add([]byte{0, 1, 2, 1, 2, 0, 255, 254, 3, 31})
	f.Add([]byte{7, 7, 7, 8, 8, 0, 31, 29})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 1024 {
			return
		}
		checkHashedParity(t, Options{Flags: data[0] & FlagZstd, MaxNames: 1 + int(data[0]%32), MaxRows: 1 + int(data[0]%17), MaxFrameSize: 256}, data, func(name []byte) uint64 { return nameHash(name) & 3 })
	})
}
