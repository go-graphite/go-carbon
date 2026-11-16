package main

import (
	"context"
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
	if e != nil && role == "receive" && os.Getenv("COPY_ADDRESS") != "" {
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
