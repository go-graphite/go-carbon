package mixedread

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
)

// These diagnostics run separately from latency samples. VFS table reads count
// actual read requests below Pebble's block cache, not logical codec bytes.
// Linux read_bytes counts device IO; it can be zero with a warm OS page cache.
type latencyProcessIO struct {
	Requests, Bytes, DeviceBytes uint64
	probeBytes                   uint64
}

func latencyReadProcessIO(t *testing.T) latencyProcessIO {
	t.Helper()
	f, err := os.Open("/proc/self/io")
	if err != nil {
		t.Skip("Linux process IO accounting required: ", err)
	}
	defer f.Close()
	var buf [4096]byte
	n, err := f.Read(buf[:]) // One read, so the next delta can exclude this probe.
	if err != nil {
		t.Fatal(err)
	}
	var out latencyProcessIO
	out.probeBytes = uint64(n)
	for _, line := range strings.Split(string(buf[:n]), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		switch fields[0] {
		case "syscr:":
			out.Requests = value
		case "rchar:":
			out.Bytes = value
		case "read_bytes:":
			out.DeviceBytes = value
		}
	}
	return out
}

func latencyColdIO(t *testing.T, class string, sample int, native, legacy *chunkstore.Store, fetchNative, fetchMixed func() error, nativeFirst bool) {
	t.Helper()
	measure := func(arm string, store *chunkstore.Store, fetch func() error) {
		before := store.Stats().FS
		procBefore := latencyReadProcessIO(t)
		if err := fetch(); err != nil {
			t.Fatal(err)
		}
		procAfter := latencyReadProcessIO(t)
		after := store.Stats().FS
		row := struct {
			Class, Arm              string
			Sample                  int
			TableReads, TableBytes  uint64
			ReadRequests, ReadBytes uint64
			DeviceBytes             uint64
		}{class, arm, sample,
			after.Ops[chunkstore.FSRead][chunkstore.FSTable] - before.Ops[chunkstore.FSRead][chunkstore.FSTable],
			after.Bytes[chunkstore.FSRead][chunkstore.FSTable] - before.Bytes[chunkstore.FSRead][chunkstore.FSTable],
			procAfter.Requests - procBefore.Requests - 1,
			procAfter.Bytes - procBefore.Bytes - procBefore.probeBytes,
			procAfter.DeviceBytes - procBefore.DeviceBytes}
		data, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("COLD_IO %s\n", data)
	}
	if nativeFirst {
		measure("native", native, fetchNative)
		measure("mixed", legacy, fetchMixed)
	} else {
		measure("mixed", legacy, fetchMixed)
		measure("native", native, fetchNative)
	}
}
