// Command wire writes repeatable input fixtures for the restart experiment.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-graphite/go-carbon/receiver/stream/codec"
	"github.com/klauspost/compress/zstd"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: wire DIRECTORY")
	}
	root := os.Args[1]
	base := time.Now().Unix() - 1800
	const names, batches, rows = 256, 1024, 256
	z, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	must(err)
	defer z.Close()
	for _, mode := range []string{"plaintext", "zstd", "stream"} {
		dir := filepath.Join(root, mode)
		must(os.MkdirAll(dir, 0750))
		e, err := codec.NewEncoder(codec.Options{Flags: codec.FlagZstd})
		must(err)
		for b := 0; b < batches; b++ {
			var plain []byte
			for j := 0; j < rows; j++ {
				i := b*rows + j
				name := fmt.Sprintf("example.application.component.metric_%02d", i%names)
				value := float64(i) + 0.125
				plain = fmt.Appendf(plain, "%s %.3f %d\n", name, value, base+int64(i/names))
				must(e.Add([]byte(name), value, base+int64(i/names)))
			}
			var wire []byte
			switch mode {
			case "plaintext":
				wire = plain
			case "zstd":
				wire = z.EncodeAll(plain, nil)
			case "stream":
				wire, err = e.Flush(nil)
				must(err)
			}
			must(os.WriteFile(filepath.Join(dir, fmt.Sprintf("%03d", b)), wire, 0600))
			// Each chunk starts with a reset, so a reconnect may resume at the next
			// chunk without reusing a dictionary from the dead connection. Within
			// each chunk, frames still use the connection's ordinary stream codec.
			must(e.Reset())
		}
		e.Close()
	}
	preamble, err := codec.AppendPreamble(nil, codec.FlagZstd)
	must(err)
	must(os.WriteFile(filepath.Join(root, "preamble"), preamble, 0600))
	raw, err := json.Marshal(map[string]int64{"base": base, "names": names, "batches": batches, "rows": rows})
	must(err)
	must(os.WriteFile(filepath.Join(root, "fixture.json"), raw, 0600))
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
