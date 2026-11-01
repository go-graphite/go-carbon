package codec

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestPeerDictionaryOptions(t *testing.T) {
	for _, limit := range []int{-1, (1 << 20) - 1, (1 << 30) + 1} {
		if _, err := NewEncoder(Options{PeerDictionaryBytes: limit}); !errors.Is(err, ErrLimit) {
			t.Fatalf("limit %d: %v", limit, err)
		}
	}
	for _, limit := range []int{0, 1 << 20, 1 << 30} {
		e := encoder(t, Options{PeerDictionaryBytes: limit})
		if limit == 0 {
			limit = DefaultDictionaryBytes
		}
		if e.opts.PeerDictionaryBytes != limit {
			t.Fatal("peer default or explicit value lost")
		}
	}
}

func TestPeerDictionaryResets(t *testing.T) {
	for _, flags := range []byte{0, FlagZstd} {
		for _, tc := range []struct {
			name               string
			peer, local, names int
			reason             ResetReason
		}{
			{"peer", 1 << 20, 4 << 20, DefaultMaxNames, ResetPeerBytes},
			{"local", 2 << 20, 4096, DefaultMaxNames, ResetLocalBytes},
			{"names", 2 << 20, 4 << 20, 5, ResetNames},
			{"raised", 2 << 20, 4 << 20, DefaultMaxNames, ""},
		} {
			t.Run(fmt.Sprintf("%d/%s", flags, tc.name), func(t *testing.T) {
				table, err := NewNameTable(4<<20, false)
				if err != nil {
					t.Fatal(err)
				}
				e, err := NewSharedEncoder(Options{Flags: flags, MaxNames: tc.names, DictionaryBytes: tc.local, PeerDictionaryBytes: tc.peer}, table)
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				d := decoder(t, Options{Flags: flags, DictionaryBytes: tc.peer})
				var wire []byte
				resets := 0
				const count = 3000
				for i := range count {
					name := []byte(fmt.Sprintf("example.%s.%d", strings.Repeat("component.", 30), i))
					_, reset, err := e.AddHashed(name, nameHash(name), float64(i)/8, int64(i))
					if err != nil {
						t.Fatal(err)
					}
					if reset {
						resets++
						if e.LastResetReason() != tc.reason {
							t.Fatalf("reason %q, want %q", e.LastResetReason(), tc.reason)
						}
					}
					if e.Ready() {
						wire = flush(t, e, wire)
					}
				}
				wire = flush(t, e, wire)
				var got []observation
				decodeAll(t, d, wire, observe(&got))
				if len(got) != count || (resets > 0) != (tc.reason != "") {
					t.Fatalf("rows=%d resets=%d", len(got), resets)
				}
				seen := make(map[int64]bool)
				for _, row := range got {
					wantName := fmt.Sprintf("example.%s.%d", strings.Repeat("component.", 30), row.ts)
					if row.ts < 0 || row.ts >= count || seen[row.ts] || row.name != wantName || row.bits != math.Float64bits(float64(row.ts)/8) {
						t.Fatalf("changed or duplicate point: %+v", row)
					}
					seen[row.ts] = true
				}
				if tc.name == "raised" {
					small := decoder(t, Options{Flags: flags, DictionaryBytes: 1 << 20})
					r := bytes.NewReader(wire)
					for r.Len() > 0 {
						calls := 0
						_, err := small.ReadFrame(r, func(uint32, []byte, float64, int64, *any) { calls++ })
						if errors.Is(err, ErrLimit) {
							if calls != 0 {
								t.Fatal("rejected frame emitted points")
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					t.Fatal("smaller receiver accepted oversized dictionary")
				}
			})
		}
	}
}
