// Command churn_receiver measures a store's stream dictionaries and cache in a
// separate process. Stdin packets are big-endian uint16 connection, uint32 size,
// then complete stream writes. Connection 65535 is a minute/measurement marker.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"

	"github.com/go-graphite/go-carbon/cache"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

func cpu() int64 {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		panic(err)
	}
	return (int64(u.Utime.Sec)+int64(u.Stime.Sec))*1e9 + (int64(u.Utime.Usec)+int64(u.Stime.Usec))*1000
}
func main() {
	connections := flag.Int("connections", 40, "connections retained")
	profile := flag.String("profile", "", "heap profile path")
	shared := flag.Bool("shared", true, "bind definitions to shared series")
	flag.Parse()
	c := cache.New()
	c.SetMaxSize(0)
	c.InitCacheScanAdds()
	decoders := make([]*codec.Decoder, *connections)
	counts := make([]int, *connections)
	var buf []byte
	var rows, ns, definitions, resets uint64
	intervalStart := cpu()
	for {
		var header [6]byte
		if _, err := io.ReadFull(os.Stdin, header[:]); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		id, size := int(binary.BigEndian.Uint16(header[:2])), int(binary.BigEndian.Uint32(header[2:]))
		if size > 32<<20 {
			panic("packet limit")
		}
		if cap(buf) < size {
			buf = make([]byte, size)
		}
		buf = buf[:size]
		if _, err := io.ReadFull(os.Stdin, buf); err != nil {
			panic(err)
		}
		if id == 65535 {
			ns += uint64(cpu() - intervalStart)
			minute := binary.BigEndian.Uint32(buf)
			for _, names := range c.GetRecentNewMetrics() {
				for name := range names {
					c.Pop(name)
				}
			}
			var natural runtime.MemStats
			runtime.ReadMemStats(&natural)
			runtime.GC()
			var live runtime.MemStats
			runtime.ReadMemStats(&live)
			rss, _ := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
			names, dict := 0, 0
			for i, d := range decoders {
				names += counts[i]
				if stats, ok := any(d).(interface{ RetainedBytes() int }); ok && d != nil {
					dict += stats.RetainedBytes()
				}
			}
			out := map[string]any{"minute": minute, "connections": *connections, "rows": rows, "cpu_ns": float64(ns) / float64(max(1, rows)), "names": names, "definitions": definitions, "resets": resets, "dictionary_bytes": dict, "heap_natural": natural.HeapAlloc, "heap_live": live.HeapAlloc, "heap_inuse": live.HeapInuse, "rss_KiB": strings.TrimSpace(string(rss))}
			if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
				panic(err)
			}
			intervalStart = cpu()
			continue
		}
		if id >= *connections {
			continue
		}
		d := decoders[id]
		if d == nil {
			flags, err := codec.ParsePreamble(buf)
			if err != nil {
				panic(err)
			}
			d, err = codec.NewDecoder(codec.Options{Flags: flags})
			if err != nil {
				panic(err)
			}
			if *shared {
				if external, ok := any(d).(interface{ SetNameResolver(func([]byte) any) }); ok {
					if names, ok := any(c).(interface{ ResolveStreamName([]byte) any }); ok {
						external.SetNameResolver(names.ResolveStreamName)
					}
				}
			}
			decoders[id] = d
			continue
		}
		r := bytes.NewReader(buf)
		for r.Len() > 0 {
			info, err := d.ReadFrame(r, func(_ uint32, name []byte, value float64, ts int64, slot *any) {
				c.AddStreamPoint(name, value, ts, slot)
			})
			if err != nil {
				panic(fmt.Errorf("connection %d: %w", id, err))
			}
			rows += uint64(info.Rows)
			definitions += uint64(info.Names)
			counts[id] += int(info.Names)
			if info.Kind == codec.ResetFrame {
				counts[id] = 0
				resets++
			}
		}
	}
	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			panic(err)
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			panic(err)
		}
		_ = f.Close()
	}
	runtime.KeepAlive(decoders)
	runtime.KeepAlive(c)
	for _, d := range decoders {
		if d != nil {
			d.Close()
		}
	}
}
