package codec

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
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
