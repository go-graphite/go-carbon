package codec

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestPeerFrames exchanges synthetic fixtures with an independent codec copy.
// Export and import directories are opt-in and never contain captured names.
func TestPeerFrames(t *testing.T) {
	export, peer := os.Getenv("STREAM_CODEC_EXPORT"), os.Getenv("STREAM_CODEC_IMPORT")
	if export == "" && peer == "" {
		t.Skip("set STREAM_CODEC_EXPORT or STREAM_CODEC_IMPORT")
	}
	for _, flags := range []byte{0, FlagZstd} {
		e := encoder(t, Options{Flags: flags})
		wire, _ := AppendPreamble(nil, flags)
		for i := range 10000 {
			if i > 0 && i%3000 == 0 {
				wire = flush(t, e, wire)
				if err := e.Reset(); err != nil {
					t.Fatal(err)
				}
			}
			add(t, e, fmt.Sprintf("example.peer.%d", i%128), float64(i)/8, 1700000000+int64(i))
			if e.Ready() {
				wire = flush(t, e, wire)
			}
		}
		wire = flush(t, e, wire)
		name := fmt.Sprintf("peer-%d.bin", flags)
		if export != "" {
			if err := os.WriteFile(filepath.Join(export, name), wire, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if peer != "" {
			other, err := os.ReadFile(filepath.Join(peer, name))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(wire, other) {
				t.Fatal("peer encoder differs byte for byte")
			}
			var got []observation
			decodeAll(t, decoder(t, Options{Flags: flags}), other[PreambleSize:], observe(&got))
			if len(got) != 10000 {
				t.Fatal("peer round trip lost rows")
			}
		}
	}
	if export != "" {
		if err := os.WriteFile(filepath.Join(export, "golden.bin"), golden(t), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if peer != "" {
		wire, err := os.ReadFile(filepath.Join(peer, "golden.bin"))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(wire, golden(t)) {
			t.Fatal("peer golden differs")
		}
		var got []observation
		decodeAll(t, decoder(t, Options{}), wire[PreambleSize:], observe(&got))
		if len(got) != 5 {
			t.Fatal("peer golden lost rows")
		}
	}
}

// Exercise both sides of a configurable peer boundary in the independent
// copies, including zstd and automatic resets. The ordinary golden still pins
// unchanged v1 bytes. Zstd versions can choose different compressed payloads;
// compare their validated points and frame boundaries instead.
func TestPeerLimitFrames(t *testing.T) {
	export, peer := os.Getenv("STREAM_CODEC_EXPORT"), os.Getenv("STREAM_CODEC_IMPORT")
	if export == "" && peer == "" {
		t.Skip("set STREAM_CODEC_EXPORT or STREAM_CODEC_IMPORT")
	}
	for _, flags := range []byte{0, FlagZstd} {
		for _, limit := range []int{1 << 20, 2 << 20} {
			e := encoder(t, Options{Flags: flags, DictionaryBytes: 4 << 20, PeerDictionaryBytes: limit})
			wire, _ := AppendPreamble(nil, flags)
			for i := range 6000 {
				add(t, e, fmt.Sprintf("example.%s.%d", strings.Repeat("component.", 30), i%3000), float64(i)/8, int64(i))
				if e.Ready() {
					wire = flush(t, e, wire)
				}
			}
			wire = flush(t, e, wire)
			name := fmt.Sprintf("peer-limit-%d-%d.bin", flags, limit)
			if export != "" {
				if err := os.WriteFile(filepath.Join(export, name), wire, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if peer != "" {
				other, err := os.ReadFile(filepath.Join(peer, name))
				if err != nil {
					t.Fatal(err)
				}
				if flags == 0 && !bytes.Equal(wire, other) {
					t.Fatal("peer limit frames differ byte for byte")
				}
				var got, want []observation
				frames := decodeAll(t, decoder(t, Options{Flags: flags, DictionaryBytes: limit}), other[PreambleSize:], observe(&got))
				own := decodeAll(t, decoder(t, Options{Flags: flags, DictionaryBytes: limit}), wire[PreambleSize:], observe(&want))
				for i := range frames {
					frames[i].PayloadBytes = 0
				}
				for i := range own {
					own[i].PayloadBytes = 0
				}
				if len(got) != 6000 || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(frames, own) {
					t.Fatal("peer limit exchange changed points or frame boundaries")
				}
			}
		}
	}
}
