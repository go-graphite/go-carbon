package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// Explicitly invoked offline campaign, never part of an ordinary test run.
// The role split gives independent sender and receiver CPU/I/O measurements.
func TestCopyCampaign(t *testing.T) {
	root := os.Getenv("COPY_ROOT")
	if root == "" {
		t.Skip("COPY_ROOT not set")
	}
	role := os.Getenv("COPY_ROLE")
	inventory := filepath.Join(root, "inventory.json")
	if role == "prepare" {
		dir := filepath.Join(root, "source")
		if e := os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
		parts := 1000
		if v := os.Getenv("COPY_PARTITIONS"); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil || n < 1 || n > 100000 {
				t.Fatal("invalid COPY_PARTITIONS")
			}
			parts = n
		}
		blocks := 160
		if v := os.Getenv("COPY_BLOCKS"); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil || n < 1 {
				t.Fatal("invalid COPY_BLOCKS")
			}
			blocks = n
		}
		start := time.Now()
		var donors []*pebble.DB
		for s := 0; s < 16 && os.Getenv("COPY_RETAIN_ONLY") == ""; s++ {
			donors = append(donors, copyTestDB(t, filepath.Join(root, fmt.Sprintf("donor-%02d", s))))
		}
		source := copyFixtureGrouped(t, dir, parts, blocks, 1024, donors, 4)
		exportStart := time.Now()
		var exportBytes uint64
		for i := 0; i < 10 && os.Getenv("COPY_NO_EXPORT") == ""; i++ {
			m := source[i].Manifest
			snap := donors[int(m.VShard)%len(donors)].NewSnapshot()
			p, e := copyExport(snap, dir, m, partsformat.PartID{255, byte(i)})
			snap.Close()
			if e != nil {
				t.Fatal(e)
			}
			for _, f := range p.Files {
				exportBytes += f.Part.Bytes
				if e = os.Remove(f.Path); e != nil {
					t.Fatal(e)
				}
			}
		}
		exportSeconds := time.Since(exportStart).Seconds()
		for _, db := range donors {
			if e := db.Close(); e != nil {
				t.Fatal(e)
			}
		}
		b, e := json.Marshal(source)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(inventory, b, 0600); e != nil {
			t.Fatal(e)
		}
		var size uint64
		for _, p := range source {
			size += p.Files[0].Part.Bytes
		}
		copyPrint(map[string]any{"phase": "prepare", "seconds": time.Since(start).Seconds(), "partitions": parts, "bytes": size, "points": uint64(parts * blocks * 1024), "export_bytes": exportBytes, "export_seconds": exportSeconds, "export_MBps": float64(exportBytes) / exportSeconds / 1e6})
		return
	}
	b, e := os.ReadFile(inventory)
	if e != nil && (role == "receive" || role == "hole-receive") && os.Getenv("COPY_ADDRESS") != "" {
		c, err := net.DialTimeout("tcp", os.Getenv("COPY_ADDRESS"), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		var frame [8]byte
		binary.BigEndian.PutUint32(frame[:4], ^uint32(0))
		binary.BigEndian.PutUint32(frame[4:], 2)
		_, err = c.Write(frame[:])
		if err == nil {
			_, err = io.ReadFull(c, frame[:])
		}
		if err == nil {
			n := binary.BigEndian.Uint64(frame[:])
			if n > 64<<20 {
				err = fmt.Errorf("inventory too large: %d", n)
			} else {
				b = make([]byte, int(n))
				_, err = io.ReadFull(c, b)
			}
		}
		_ = c.Close()
		if err != nil {
			t.Fatal(err)
		}
		e = os.WriteFile(inventory, b, 0600)
	}
	if e != nil {
		t.Fatal(e)
	}
	var source []copyPartition
	if e = json.Unmarshal(b, &source); e != nil {
		t.Fatal(e)
	}
	if role == "serve" {
		if os.Getenv("COPY_COLD") != "" {
			copyEnableCold(t)
		}
		bind := os.Getenv("COPY_BIND")
		if bind == "" {
			bind = "127.0.0.1:0"
		}
		l, e := net.Listen("tcp", bind)
		if e != nil {
			t.Fatal(e)
		}
		defer l.Close()
		if e = os.WriteFile(filepath.Join(root, "address"), []byte(l.Addr().String()), 0600); e != nil {
			t.Fatal(e)
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		go func() { <-ctx.Done(); _ = l.Close() }()
		for {
			c, e := l.Accept()
			if e != nil {
				if ctx.Err() != nil {
					return
				}
				t.Fatal(e)
			}
			go func() {
				if e := copyServeConn(c, source); e != nil {
					copyPrint(map[string]any{"phase": "sender_error", "error": e.Error()})
				}
			}()
		}
	}
	if role == "hole-receive" {
		copyReceiveHoles(t, root, source)
		return
	}
	if role != "receive" {
		t.Fatal("COPY_ROLE must be prepare, serve or receive")
	}
	streams, e := strconv.Atoi(os.Getenv("COPY_STREAMS"))
	if e != nil || streams < 1 {
		t.Fatal("invalid COPY_STREAMS")
	}
	damage, e := strconv.Atoi(os.Getenv("COPY_HOLES"))
	if e != nil || damage < 0 || damage > len(source) {
		t.Fatal("COPY_HOLES must not exceed inventory partitions")
	}
	dir := filepath.Join(root, "target")
	if e = os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	var dbs []*pebble.DB
	for s := 0; s < 16; s++ {
		dbs = append(dbs, copyTestDB(t, filepath.Join(dir, fmt.Sprintf("owner-%02d", s))))
	}
	defer func() {
		for _, db := range dbs {
			if e := db.Close(); e != nil {
				t.Error(e)
			}
		}
	}()
	// Healthy partitions exist as linked SSTs on disk and have their matching
	// manifest. Setup is outside the measured repair. The damaged subset is
	// physically absent. No healthy point blocks are read during comparison.
	var target []partsformat.Manifest
	for i, p := range source {
		if i >= damage {
			var paths []string
			for fi, f := range p.Files {
				path := filepath.Join(dir, fmt.Sprintf("healthy-%04d-%04d.sst", i, fi))
				if e = os.Link(f.Path, path); e != nil {
					t.Fatal(e)
				}
				paths = append(paths, path)
			}
			if e = dbs[int(p.Manifest.VShard)%len(dbs)].Ingest(paths); e != nil {
				t.Fatal(e)
			}
			target = append(target, p.Manifest)
		}
	}
	address, e := os.ReadFile(filepath.Join(root, "address"))
	if override := os.Getenv("COPY_ADDRESS"); override != "" {
		address = []byte(override)
		e = nil
	}
	if e != nil {
		t.Fatal(e)
	}
	compare := time.Now()
	plan, compareBytes, e := copyCompare(context.Background(), string(address), target)
	compareSec := time.Since(compare).Seconds()
	group := os.Getenv("COPY_ONLY_GROUP")
	if group != "" {
		plan = copySelectGroup(source, plan, partsformat.Group(group))
	}
	if e != nil {
		t.Fatal(e)
	}
	var cpuFile *os.File
	if os.Getenv("COPY_PROFILE") != "" {
		cpuFile, e = os.Create(filepath.Join(root, "receiver.cpu"))
		if e != nil {
			t.Fatal(e)
		}
		if e = pprof.StartCPUProfile(cpuFile); e != nil {
			t.Fatal(e)
		}
		defer func() { pprof.StopCPUProfile(); cpuFile.Close() }()
	}
	var before, after syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	ioBefore := copyProcessIO()
	seconds, _ := strconv.Atoi(os.Getenv("COPY_SECONDS"))
	ctx := context.Background()
	if seconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
		defer cancel()
	}
	start := time.Now()
	var stats copyStats
	for {
		one, transferErr := copyTransfer(ctx, string(address), source, plan, streams, dbs, dir)
		stats.Bytes += one.Bytes
		stats.Partitions += one.Partitions
		e = transferErr
		if e != nil || seconds <= 0 || time.Since(start) >= time.Duration(seconds)*time.Second {
			break
		}
	}
	stats.Seconds = time.Since(start).Seconds()
	if seconds > 0 && ctx.Err() != nil {
		e = nil
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	ioAfter := copyProcessIO()
	cpu := float64(after.Utime.Sec-before.Utime.Sec+after.Stime.Sec-before.Stime.Sec) + float64(after.Utime.Usec-before.Utime.Usec+after.Stime.Usec-before.Stime.Usec)/1e6
	row := map[string]any{"phase": "receive", "streams": streams, "holes": damage, "compare_seconds": compareSec, "compare_bytes": compareBytes, "stats": stats, "receiver_cpu_seconds": cpu, "receiver_cores": cpu / stats.Seconds, "MBps": float64(stats.Bytes) / stats.Seconds / 1e6}
	row["group"] = group
	row["write_bytes"] = ioAfter["write_bytes"] - ioBefore["write_bytes"]
	row["read_bytes"] = ioAfter["read_bytes"] - ioBefore["read_bytes"]
	if e != nil {
		row["error"] = e.Error()
	}
	copyPrint(row)
	if e != nil {
		t.Fatal(e)
	}
}
func copyProcessIO() map[string]uint64 {
	out := map[string]uint64{}
	b, _ := os.ReadFile("/proc/self/io")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			n, _ := strconv.ParseUint(f[1], 10, 64)
			out[strings.TrimSuffix(f[0], ":")] = n
		}
	}
	return out
}
func copyPrint(v any) { b, _ := json.Marshal(v); fmt.Println("COPY_JSON " + string(b)) }

// Partial-hole fixture construction and planning are measured separately from
// transfer. Healthy and patch inventories alternate by partition; source raw
// blocks are copied verbatim. No payload is opened by the planner itself.
func TestCopyPartialHolePrepare(t *testing.T) {
	root := os.Getenv("COPY_ROOT")
	if root == "" || os.Getenv("COPY_ROLE") != "holes" {
		t.Skip("explicit hole fixture only")
	}
	raw, e := os.ReadFile(filepath.Join(root, "inventory.json"))
	if e != nil {
		t.Fatal(e)
	}
	var full []copyPartition
	if e = json.Unmarshal(raw, &full); e != nil {
		t.Fatal(e)
	}
	for i := range full {
		full[i] = copySealedFixture(t, full[i])
	}
	for _, percent := range []int{1, 10, 50} {
		dir := filepath.Join(root, fmt.Sprintf("holes-%d", percent))
		if e = os.Mkdir(dir, 0700); e != nil {
			t.Fatal(e)
		}
		var inventory []copyPartition
		var rawBytes uint64
		var planSeconds, assemblySeconds float64
		var before, after syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
		for pi, p := range full {
			target := *p.Blocks
			target.Blocks = nil
			var healthy []int
			// Spread holes across partitions and physical block positions. 100 blocks
			// per partition makes all three fractions exact, without rounding.
			for bi, b := range p.Blocks.Blocks {
				if (bi+pi)%100 >= percent {
					healthy = append(healthy, bi)
					target.Blocks = append(target.Blocks, b)
				}
			}
			target.Digest = copyLogicalDigest(target)
			start := time.Now()
			plan, err := copyPlanBlocks(*p.Blocks, target)
			planSeconds += time.Since(start).Seconds()
			if err != nil || plan.Conflicts != 0 {
				t.Fatal(err, plan)
			}
			start = time.Now()
			base, _, err := copyAssembleBlocks(p, healthy, dir)
			if err != nil {
				t.Fatal(err)
			}
			start = time.Now()
			patch, n, err := copyAssembleBlocks(p, plan.Missing, dir)
			assemblySeconds += time.Since(start).Seconds()
			if err != nil {
				t.Fatal(err)
			}
			rawBytes += n
			base.Additive = true
			patch.Additive = true
			inventory = append(inventory, base, patch)
		}
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
		cpu := float64(after.Utime.Sec-before.Utime.Sec+after.Stime.Sec-before.Stime.Sec) + float64(after.Utime.Usec-before.Utime.Usec+after.Stime.Usec-before.Stime.Usec)/1e6
		b, err := json.Marshal(inventory)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "inventory.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
		copyPrint(map[string]any{"phase": "hole_prepare", "percent": percent, "partitions": len(full), "raw_bytes": rawBytes, "plan_seconds": planSeconds, "plan_ms_per_1000": planSeconds * 1e6 / float64(len(full)), "assembly_seconds": assemblySeconds, "assembly_MBps": float64(rawBytes) / assemblySeconds / 1e6, "cpu_seconds": cpu})
	}
}

func copyReceiveHoles(t *testing.T, root string, source []copyPartition) {
	t.Helper()
	if len(source)%2 != 0 {
		t.Fatal("unpaired fixture")
	}
	dir := filepath.Join(root, "target")
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(dir)
	var dbs []*pebble.DB
	for i := 0; i < 16; i++ {
		dbs = append(dbs, copyTestDB(t, filepath.Join(dir, fmt.Sprint(i))))
	}
	defer func() {
		for _, db := range dbs {
			if e := db.Close(); e != nil {
				t.Error(e)
			}
		}
	}()
	var base, holes []int
	for i := 0; i < len(source); i += 2 {
		base = append(base, i)
		holes = append(holes, i+1)
	}
	streams, e := strconv.Atoi(os.Getenv("COPY_STREAMS"))
	if e != nil {
		t.Fatal(e)
	}
	address := os.Getenv("COPY_ADDRESS")
	if _, e = copyTransfer(context.Background(), address, source, base, streams, dbs, dir); e != nil {
		t.Fatal(e)
	}
	var before, after syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	transferStart := time.Now()
	st, e := copyTransfer(context.Background(), address, source, holes, streams, dbs, dir)
	transferEnd := time.Now()
	if e != nil {
		t.Fatal(e)
	}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	cpu := float64(after.Utime.Sec-before.Utime.Sec+after.Stime.Sec-before.Stime.Sec) + float64(after.Utime.Usec-before.Utime.Usec+after.Stime.Usec-before.Stime.Usec)/1e6
	// Independently regenerate the deterministic opaque fixture oracle. This
	// verification is outside the transfer timing and decodes no sample format.
	var checked uint64
	for pi := 0; pi < len(source)/2; pi++ {
		p := source[pi*2]
		prefix, err := partsformat.PartitionPrefixForGroup(p.Manifest.Group, p.Manifest.VShard, p.Manifest.Partition)
		if err != nil {
			t.Fatal(err)
		}
		it, err := dbs[int(p.Manifest.VShard)%len(dbs)].NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: partsformat.PrefixEnd(prefix)})
		if err != nil {
			t.Fatal(err)
		}
		state := uint64(pi + 1)
		bi := 0
		for it.First(); it.Valid(); it.Next() {
			k, err := partsformat.Decode(it.Key())
			if err != nil {
				t.Fatal(err)
			}
			if k.Ordinal != uint32(bi) || k.BlockStart != int64(bi*1024) {
				t.Fatal("union key order/count", pi, bi, k)
			}
			expected := make([]byte, 24*1024)
			for point := 0; point < 1024; point++ {
				state ^= state << 13
				state ^= state >> 7
				state ^= state << 17
				bits := state
				switch point {
				case 0:
					bits = 0x8000000000000000
				case 1:
					bits = 0x7ff80000000000ab
				case 2:
					bits = 1
				case 3:
					bits = 0x7ff0000000000000
				}
				binary.LittleEndian.PutUint64(expected[point*24:], uint64(pi*100*1024+bi*1024+point))
				binary.LittleEndian.PutUint64(expected[point*24+8:], bits)
				binary.LittleEndian.PutUint64(expected[point*24+16:], uint64(bi*1024+point))
			}
			wantKey, err := partsformat.Encode(partsformat.Key{Group: p.Manifest.Group, VShard: p.Manifest.VShard, Partition: int64(pi), Metric: partsformat.ID(fmt.Sprintf("copy.fixture.metric.%04d", pi%64)), BlockStart: int64(bi * 1024), Part: func() partsformat.PartID {
				var id partsformat.PartID
				binary.BigEndian.PutUint64(id[:8], uint64(pi+1))
				id[15] = 1
				return id
			}(), Ordinal: uint32(bi)})
			if err != nil || !bytes.Equal(wantKey, it.Key()) || sha256.Sum256(expected) != sha256.Sum256(it.Value()) {
				t.Fatal("union differs", pi, bi, err)
			}
			bi++
			checked++
		}
		if e = it.Error(); e != nil {
			t.Fatal(e)
		}
		if e = it.Close(); e != nil {
			t.Fatal(e)
		}
		if bi != 100 {
			t.Fatal("missing/extra union blocks", pi, bi)
		}
	}
	copyPrint(map[string]any{"phase": "hole_receive", "streams": streams, "stats": st, "MBps": float64(st.Bytes) / st.Seconds / 1e6, "receiver_cores": cpu / st.Seconds, "exact": true, "blocks_checked": checked, "sample_decode": false, "transfer_start_epoch": float64(transferStart.UnixNano()) / 1e9, "transfer_end_epoch": float64(transferEnd.UnixNano()) / 1e9})
}
