package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/points"
)

var catalogFixtureOnly = flag.Bool("catalog-fixture-only", false, "read source, report first-observed name churn and save a raw capture fixture without codec comparison")
var catalogVerifyDir = flag.String("catalog-verify", "", "verify an already completed capture root")
var catalogDir = flag.String("catalog-dir", "", "catalog experiment output root (enables catalog comparison)")
var catalogFixturePoints = flag.Int64("catalog-fixture-points", 30000000, "points retained for a source fixture; short diagnostic cohorts are not production qualification")
var captureIntervals = flag.Bool("capture-intervals", false, "report observed one-second encoded-point and process-CPU windows, separate from durable full-wall throughput")
var captureProfile = flag.String("capture-cpuprofile", "", "capture-only CPU profile with stage labels; excludes fixture setup and exact verifier")
var captureSeconds = flag.Int("capture-seconds", 0, "sustained group-fsync capture seconds after matched-byte comparison")
var captureAfter = flag.Int64("capture-after-unix", 0, "do not begin capture before this Unix second")
var captureStreams = flag.Int("capture-streams", 32, "independent capture streams")
var catalogSender = flag.String("catalog-sender", "", "internal disjoint sender: address")
var catalogFixture = flag.String("catalog-fixture", "", "internal sender fixture")
var catalogSendDuration = flag.Int("catalog-send-seconds", 120, "internal sender duration")
var catalogSendStreams = flag.Int("catalog-send-streams", 32, "internal sender streams")
var catalogRecapture = flag.String("catalog-recapture", "", "load a completed capture fixture instead of rereading the input log")
var captureUnseenEvery = flag.Uint64("capture-unseen-every", 0, "one never-seen name per N points; 0 disables churn")
var captureUnseen = flag.Bool("capture-unseen", false, "give every tenth point a new never-repeated name before durable encoding")

// unseenFrames is deterministic across packet boundaries and fixture repeats.
// It preserves every tuple and order, changing only every tenth point's name.
func unseenFrames(fs []partlog.DictionaryFrame, ordinal *uint64) []partlog.DictionaryFrame {
	return churnFrames(fs, ordinal, 10)
}

func churnFrames(fs []partlog.DictionaryFrame, ordinal *uint64, every uint64) []partlog.DictionaryFrame {
	return churnFramesInto(fs, ordinal, every, nil)
}

func churnFramesInto(fs []partlog.DictionaryFrame, ordinal *uint64, every uint64, out []partlog.DictionaryFrame) []partlog.DictionaryFrame {
	if every == 0 {
		return fs
	}
	clear(out)
	out = out[:0]
	for _, f := range fs {
		for start := 0; start < len(f.Points); {
			n := int(min(uint64(len(f.Points)-start), every-1-*ordinal%every))
			g := f
			if n == 0 {
				n = 1
				g.Key = fmt.Sprintf("\x00unseen/%d/%s", *ordinal, f.Key)
			}
			g.Points = f.Points[start : start+n]
			out = append(out, g)
			start += n
			*ordinal += uint64(n)
		}
	}
	return out
}

func catalogRecaptureMain() {
	c, e := partlog.OpenNameCatalog(filepath.Join(*catalogRecapture, "template"))
	must(e)
	defer c.Close()
	f, e := os.Open(filepath.Join(*catalogRecapture, "sender.fixture"))
	must(e)
	defer f.Close()
	var fs []partlog.DictionaryFrame
	for {
		b, e := readPacket(f)
		if e == io.EOF {
			break
		}
		must(e)
		frames, _ := catalogUnwire(b, c)
		fs = append(fs, frames...)
	}
	catalogCapture(fs, *captureStreams, *captureSeconds)
}

func catalogExact(a, b []partlog.DictionaryFrame) {
	if len(a) != len(b) {
		panic("catalog frame count")
	}
	for i, f := range a {
		g := b[i]
		if f.Key != g.Key || f.Clock != g.Clock || !bytes.Equal(f.Definition, g.Definition) || len(f.Points) != len(g.Points) {
			panic("catalog identity")
		}
		for j, p := range f.Points {
			q := g.Points[j]
			if p.Timestamp != q.Timestamp || math.Float64bits(p.Value) != math.Float64bits(q.Value) {
				panic("catalog point bits")
			}
		}
	}
}
func catalogDisk(dir string) int64 {
	var n int64
	must(filepath.Walk(dir, func(_ string, i os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if !i.IsDir() {
			n += i.Size()
		}
		return nil
	}))
	return n
}
func catalogProbe(segs []segment, partition int, rate float64) {
	if *catalogFixtureOnly {
		catalogSourceFixture(segs, partition, rate)
		return
	}
	runtime.GOMAXPROCS(8)
	must(os.MkdirAll(*catalogDir, 0700))
	var accounts [4]partlog.PredictorV3Accounting
	cats := make([]*partlog.NameCatalog, 2)
	for i := range cats {
		var e error
		cats[i], e = partlog.OpenNameCatalog(filepath.Join(*catalogDir, fmt.Sprintf("catalog-%d", i)))
		must(e)
		defer cats[i].Close()
	}
	var sample []partlog.DictionaryFrame
	samplePoints := 0
	var sourceBytes, total int64
	started := time.Now()
	for _, s := range segs {
		h := make([]byte, 64)
		_, err := s.f.ReadAt(h, 0)
		must(err)
		if !bytes.Equal(h, s.header[:]) {
			panic("source recycled")
		}
		view, frontier, selected, err := selectPartition(s.f, s.Bytes, partition)
		must(err)
		emit(map[string]any{"stage": "catalog_source", "path": s.Path, "points": selected, "bytes": frontier})
		ms := make([]*measurement, 4)
		for i := range ms {
			ms[i] = &measurement{Codec: fmt.Sprintf("catalog-variant-%d", i), BlockMiB: 4}
			if i < 2 {
				ms[i].encoder = &partlog.PredictorV3Encoder{SeriesClocks: true, SparseClocks: i == 1, Accounting: &accounts[i]}
				ms[i].decoder = &partlog.PredictorV3Decoder{}
			} else {
				ms[i].encoder = &partlog.CatalogEncoder{Catalog: cats[i-2], SeriesClocks: true, SparseClocks: i == 3, Accounting: &accounts[i]}
				ms[i].decoder = &partlog.CatalogDecoder{Catalog: cats[i-2]}
			}
		}
		r, err := inputlog.NewSegmentReplay(view)
		must(err)
		var count int64
		for {
			ps, offset, n, err := r.Next(frontier)
			if err == io.EOF {
				break
			}
			must(err)
			var variants sync.WaitGroup
			for _, m := range ms {
				variants.Add(1)
				go func(m *measurement) {
					defer variants.Done()
					for _, p := range ps {
						m.add(partlog.DictionaryFrame{Key: p.Metric, Clock: p.FirstArrival, Points: p.Data})
					}
				}(m)
			}
			variants.Wait()
			for _, p := range ps {
				f := partlog.DictionaryFrame{Key: p.Metric, Clock: p.FirstArrival, Points: p.Data}
				if samplePoints < 30000000 {
					sample = append(sample, f)
					samplePoints += len(f.Points)
				}
			}
			count += int64(n)
			if delay := time.Duration(float64(sourceBytes+offset)/(rate*1e6)*float64(time.Second)) - time.Since(started); delay > 0 {
				time.Sleep(delay)
			}
		}
		r.Close()
		for _, m := range ms {
			m.flush()
			emit(map[string]any{"stage": "matched_codec", "codec": m.Codec, "points": m.Points, "bytes_including_32B_headers": m.Bytes, "bpp": float64(m.Bytes) / float64(m.Points), "exact": true, "cpu_scope": "variants concurrent; no per-variant CPU attribution"})
			m.encoder.Close()
			m.decoder.Close()
		}
		_, err = s.f.ReadAt(h, 0)
		must(err)
		if !bytes.Equal(h, s.header[:]) || count != selected {
			panic("source changed")
		}
		sourceBytes += frontier
		total += count
	}
	for i, c := range cats {
		must(c.Sync())
		walBytes := catalogDisk(filepath.Join(*catalogDir, fmt.Sprintf("catalog-%d", i)))
		must(c.Checkpoint())
		disk := catalogDisk(filepath.Join(*catalogDir, fmt.Sprintf("catalog-%d", i)))
		var names, defs, ids, framing int64
		for id := uint64(1); id <= c.Count(); id++ {
			name, def, _ := c.Lookup(id)
			names += int64(len(name) + uvarintBytes(uint64(len(name))))
			defs += int64(len(def) + uvarintBytes(uint64(len(def))))
			ids += int64(uvarintBytes(id))
			framing += 12
		}
		emit(map[string]any{"stage": "catalog_raw_fields", "variant": i + 2, "points": total, "Names": names, "Definitions": defs, "IDs": ids, "Framing": framing + 40 + 28})
		emit(map[string]any{"stage": "catalog_storage", "variant": i + 2, "entries": c.Count(), "checkpoint_and_wal_bytes": disk, "before_checkpoint_bytes": walBytes, "points": total, "catalog_bpp": float64(disk) / float64(total)})
	}
	for i, a := range accounts {
		emit(map[string]any{"stage": "catalog_fields", "variant": i, "points": a.Points, "pre_zstd": a, "compressed_bpp": float64(a.Encoded) / float64(a.Points)})
	}
	emit(map[string]any{"stage": "catalog_done", "points": total, "source_bytes": sourceBytes, "sample_points": samplePoints, "exact": true})
	if *captureSeconds > 0 {
		if delay := time.Until(time.Unix(*captureAfter, 0)); delay > 0 {
			emit(map[string]any{"stage": "capture_wait", "seconds": delay.Seconds()})
			time.Sleep(delay)
		}
		catalogCapture(sample, *captureStreams, *captureSeconds)
	}
}

// The sender transmits raw exact tuples from a retained real cohort over TCP.
// Its separate process is pinned to CPUs96-191; capture uses CPUs0-95. Repeating
// the cohort is reported explicitly, including warm predictor effects.
func catalogWire(fs []partlog.DictionaryFrame, c *partlog.NameCatalog) [][]byte {
	var out [][]byte
	var b []byte
	flush := func() {
		if len(b) > 0 {
			out = append(out, b)
			b = nil
		}
	}
	for _, f := range fs {
		id, e := c.Intern(f.Key, f.Definition)
		must(e)
		if len(b) > 1<<20 {
			flush()
		}
		b = binary.LittleEndian.AppendUint64(b, id)
		b = binary.LittleEndian.AppendUint64(b, uint64(f.Clock))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(f.Points)))
		for _, p := range f.Points {
			b = binary.LittleEndian.AppendUint64(b, uint64(p.Timestamp))
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(p.Value))
		}
	}
	flush()
	return out
}
func writePacket(w io.Writer, b []byte) {
	var h [4]byte
	binary.LittleEndian.PutUint32(h[:], uint32(len(b)))
	_, e := w.Write(h[:])
	must(e)
	_, e = w.Write(b)
	must(e)
}
func readPacket(r io.Reader) ([]byte, error) {
	return readPacketInto(r, nil)
}
func readPacketInto(r io.Reader, b []byte) ([]byte, error) {
	var h [4]byte
	if _, e := io.ReadFull(r, h[:]); e != nil {
		return nil, e
	}
	n := binary.LittleEndian.Uint32(h[:])
	if n > 64<<20 {
		return nil, fmt.Errorf("packet too big")
	}
	if cap(b) < int(n) {
		b = make([]byte, int(n))
	} else {
		b = b[:int(n)]
	}
	_, e := io.ReadFull(r, b)
	return b, e
}
func catalogUnwire(b []byte, c *partlog.NameCatalog) ([]partlog.DictionaryFrame, int64) {
	var fs []partlog.DictionaryFrame
	var n int64
	for len(b) > 0 {
		if len(b) < 20 {
			panic("wire header")
		}
		id := binary.LittleEndian.Uint64(b)
		clock := int64(binary.LittleEndian.Uint64(b[8:]))
		np := int(binary.LittleEndian.Uint32(b[16:]))
		b = b[20:]
		if np == 0 || np > len(b)/16 {
			panic("wire points")
		}
		key, def, ok := c.Lookup(id)
		if !ok {
			panic("wire catalog")
		}
		f := partlog.DictionaryFrame{Key: key, Definition: def, Clock: clock, Points: make([]points.Point, np)}
		for i := range f.Points {
			f.Points[i] = points.Point{Timestamp: int64(binary.LittleEndian.Uint64(b)), Value: math.Float64frombits(binary.LittleEndian.Uint64(b[8:]))}
			b = b[16:]
		}
		fs = append(fs, f)
		n += int64(np)
	}
	return fs, n
}

// catalogWireReader owns reusable packet/frame/point buffers. Returned frames
// are borrowed until the next read; CatalogEncoder consumes points synchronously.
// Names and definitions already belong to the catalog: no string conversion.
type catalogWireReader struct {
	packet []byte
	frames []partlog.DictionaryFrame
	points []points.Point
}

func (w *catalogWireReader) decode(b []byte, c *partlog.NameCatalog) ([]partlog.DictionaryFrame, int64) {
	clear(w.frames)
	w.frames = w.frames[:0]
	// Each point requires 16 wire bytes; reserve once before publishing slices.
	if cap(w.points) < len(b)/16 {
		w.points = make([]points.Point, 0, len(b)/16)
	}
	w.points = w.points[:0]
	for len(b) > 0 {
		if len(b) < 20 {
			panic("wire header")
		}
		id := binary.LittleEndian.Uint64(b)
		clock := int64(binary.LittleEndian.Uint64(b[8:]))
		np := int(binary.LittleEndian.Uint32(b[16:]))
		b = b[20:]
		if np == 0 || np > len(b)/16 {
			panic("wire points")
		}
		key, def, ok := c.Lookup(id)
		if !ok {
			panic("wire catalog")
		}
		start := len(w.points)
		for range np {
			w.points = append(w.points, points.Point{Timestamp: int64(binary.LittleEndian.Uint64(b)), Value: math.Float64frombits(binary.LittleEndian.Uint64(b[8:]))})
			b = b[16:]
		}
		w.frames = append(w.frames, partlog.DictionaryFrame{Key: key, Definition: def, Clock: clock, Points: w.points[start:]})
	}
	return w.frames, int64(len(w.points))
}

func catalogSenderMain() {
	runtime.GOMAXPROCS(64)
	f, e := os.Open(*catalogFixture)
	must(e)
	var blocks [][]byte
	for {
		b, e := readPacket(f)
		if e == io.EOF {
			break
		}
		must(e)
		blocks = append(blocks, b)
	}
	must(f.Close())
	var wg sync.WaitGroup
	ready := make(chan struct{})
	for range *catalogSendStreams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, e := net.Dial("tcp", *catalogSender)
			must(e)
			defer conn.Close()
			<-ready
			deadline := time.Now().Add(time.Duration(*catalogSendDuration) * time.Second)
			for time.Now().Before(deadline) {
				for _, b := range blocks {
					writePacket(conn, b)
					if time.Now().After(deadline) {
						return
					}
				}
			}
		}()
	}
	close(ready)
	wg.Wait()
}
func catalogCapture(fs []partlog.DictionaryFrame, streams, seconds int) {
	if streams < 1 || streams > 96 || seconds < 1 {
		panic("capture limits")
	}
	runtime.GOMAXPROCS(96)
	root := filepath.Join(*catalogDir, "capture")
	every := *captureUnseenEvery
	if *captureUnseen {
		every = 10
	}
	must(os.MkdirAll(root, 0700))
	c, e := partlog.OpenNameCatalog(filepath.Join(root, "template"))
	must(e)
	blocks := catalogWire(fs, c)
	must(c.Checkpoint())
	must(c.Close())
	fixture := filepath.Join(root, "sender.fixture")
	f, e := os.Create(fixture)
	must(e)
	for _, b := range blocks {
		writePacket(f, b)
	}
	must(f.Close())
	blocks = nil
	// Copy one durable seed catalog into each independent stream's directory.
	cp, e := os.ReadFile(filepath.Join(root, "template", "catalog.checkpoint"))
	must(e)
	for i := range streams {
		dir := filepath.Join(root, fmt.Sprint(i))
		must(os.MkdirAll(dir, 0700))
		seed, e := os.Create(filepath.Join(dir, "catalog.checkpoint"))
		must(e)
		_, e = seed.Write(cp)
		must(e)
		must(seed.Sync())
		must(seed.Close())
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	defer listener.Close()
	exe, e := os.Executable()
	must(e)
	cmd := exec.Command("taskset", "-c", "96-191", exe, "-catalog-sender", listener.Addr().String(), "-catalog-fixture", fixture, "-catalog-send-seconds", fmt.Sprint(seconds), "-catalog-send-streams", fmt.Sprint(streams))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	var count, encoded, unseenPoints atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	var syncNS []int64
	var catalogSyncNS []int64
	var profile *os.File
	if *captureProfile != "" {
		profile, e = os.Create(*captureProfile)
		must(e)
		must(pprof.StartCPUProfile(profile))
		must(os.WriteFile(*captureProfile+".pid", []byte(fmt.Sprint(os.Getpid())), 0600))
	}
	start := time.Now()
	startCPU := cpu()
	must(cmd.Start())
	var intervals []map[string]any
	samplingDone := make(chan struct{})
	samplingStopped := make(chan struct{})
	if *captureIntervals {
		go func() {
			defer close(samplingStopped)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			lastT, lastCPU, lastPoints := start, startCPU, int64(0)
			for {
				select {
				case <-samplingDone:
					return
				case <-ticker.C:
					now, ns, n := time.Now(), cpu(), count.Load()
					dt := now.Sub(lastT).Seconds()
					dn := n - lastPoints
					row := map[string]any{"stage": "capture_interval", "snapshot_when_cet": now.In(time.FixedZone("CET", 3600)).Format("2006-01-02 15:04:05"), "seconds_since_start": now.Sub(start).Seconds(), "seconds": dt, "points": dn, "cores": float64(ns-lastCPU) / 1e9 / dt, "m_encoded_points_per_second": float64(dn) / dt / 1e6}
					if dn > 0 {
						row["cpu_ns_per_encoded_point"] = float64(ns-lastCPU) / float64(dn)
					}
					intervals = append(intervals, row)
					lastT, lastCPU, lastPoints = now, ns, n
				}
			}
		}()
	} else {
		close(samplingStopped)
	}
	for i := range streams {
		conn, e := listener.Accept()
		must(e)
		wg.Add(1)
		go func(i int, conn net.Conn) {
			defer wg.Done()
			defer conn.Close()
			var stages map[string]context.Context
			if profile != nil {
				stages = make(map[string]context.Context)
				for _, name := range []string{"catalog_open", "tcp_read", "wire_parse", "churn", "encode", "catalog_sync", "append", "journal_sync"} {
					stages[name] = pprof.WithLabels(context.Background(), pprof.Labels("stage", name))
				}
			}
			stage := func(name string) {
				if stages != nil {
					pprof.SetGoroutineLabels(stages[name])
				}
			}
			stage("catalog_open")
			dir := filepath.Join(root, fmt.Sprint(i))
			cat, e := partlog.OpenNameCatalog(dir)
			must(e)
			defer cat.Close()
			enc := partlog.CatalogEncoder{Catalog: cat, SeriesClocks: true, SparseClocks: true}
			defer enc.Close()
			file, e := os.Create(filepath.Join(dir, "segment"))
			must(e)
			defer file.Close()
			directory, e := os.Open(dir)
			must(e)
			must(directory.Sync())
			must(directory.Close())
			var ordinal uint64
			var wire catalogWireReader
			var transformed []partlog.DictionaryFrame
			var pending [][]byte
			next := time.Now().Add(time.Second)
			syncGroup := func() {
				t := time.Now()
				stage("catalog_sync")
				must(cat.Sync())
				catalogElapsed := time.Since(t).Nanoseconds()
				stage("append")
				for _, record := range pending {
					writePacket(file, record)
				}
				clear(pending)
				pending = pending[:0]
				stage("journal_sync")
				must(file.Sync())
				mu.Lock()
				syncNS = append(syncNS, time.Since(t).Nanoseconds())
				catalogSyncNS = append(catalogSyncNS, catalogElapsed)
				mu.Unlock()
			}
			for {
				stage("tcp_read")
				b, e := readPacketInto(conn, wire.packet)
				wire.packet = b
				if e == io.EOF {
					break
				}
				must(e)
				stage("wire_parse")
				frames, n := wire.decode(b, cat)
				stage("churn")
				transformed = churnFramesInto(frames, &ordinal, every, transformed)
				frames = transformed
				stage("encode")
				record, e := enc.Encode(frames)
				must(e)
				// Keep references private until this group's catalog is durable.
				pending = append(pending, record)
				count.Add(n)
				encoded.Add(int64(len(record) + 4))
				if time.Now().After(next) {
					syncGroup()
					next = next.Add(time.Second)
				}
			}
			syncGroup()
			if every > 0 {
				unseenPoints.Add(int64(ordinal / every))
			}
		}(i, conn)
	}
	wg.Wait()
	must(cmd.Wait())
	close(samplingDone)
	<-samplingStopped
	wall := time.Since(start).Seconds()
	captureCPU := cpu() - startCPU
	cores := float64(captureCPU) / 1e9 / wall
	if profile != nil {
		pprof.StopCPUProfile()
		must(profile.Close())
	}
	sort.Slice(syncNS, func(i, j int) bool { return syncNS[i] < syncNS[j] })
	p99 := syncNS[min(len(syncNS)-1, len(syncNS)*99/100)]
	var catalogP99 int64
	if len(catalogSyncNS) > 0 {
		sort.Slice(catalogSyncNS, func(i, j int) bool { return catalogSyncNS[i] < catalogSyncNS[j] })
		catalogP99 = catalogSyncNS[len(catalogSyncNS)*99/100]
	}
	if every > 0 {
		must(os.WriteFile(filepath.Join(root, "unseen"), []byte(fmt.Sprint(every)), 0600))
	}
	var durableBytes int64
	for i := range streams {
		durableBytes += catalogDisk(filepath.Join(root, fmt.Sprint(i)))
	}
	emit(map[string]any{"stage": "capture_storage", "points": count.Load(), "unseen_points": unseenPoints.Load(), "durable_bytes": durableBytes, "bpp": float64(durableBytes) / float64(count.Load()), "catalog_fsync_p99_ms": float64(catalogP99) / 1e6, "catalog_syncs": len(catalogSyncNS), "unseen_every": every})
	emit(map[string]any{"stage": "capture", "streams": streams, "seconds_including_final_sync": wall, "points": count.Load(), "m_points_per_second": float64(count.Load()) / wall / 1e6, "cores": cores, "cpu_ns_per_point": float64(captureCPU) / float64(count.Load()), "record_bpp": float64(encoded.Load()) / float64(count.Load()), "seed_catalog_bytes_per_stream": len(cp), "fsync_group_p99_ms": float64(p99) / 1e6, "group_syncs": len(syncNS), "exact": "tuple codec differential before run; sustained replay verification follows", "sender": "separate taskset96-191 process, raw TCP, repeated complete real cohort; capture taskset0-95"})
	for _, interval := range intervals {
		emit(interval)
	}
	verified := catalogVerify(root, streams)
	if verified != count.Load() {
		panic("capture verification count")
	}
	emit(map[string]any{"stage": "capture_replay", "points": verified, "exact": true})
}

func uvarintBytes(n uint64) int { var b [10]byte; return binary.PutUvarint(b[:], n) }

func catalogVerify(root string, streams int) int64 {
	start := time.Now()
	runtime.GOMAXPROCS(96)
	fixture := filepath.Join(root, "sender.fixture")
	unseenData, unseenErr := os.ReadFile(filepath.Join(root, "unseen"))
	var every uint64
	if unseenErr == nil {
		if _, err := fmt.Sscan(string(unseenData), &every); err != nil {
			every = 10
		}
	}
	if unseenErr != nil && !os.IsNotExist(unseenErr) {
		must(unseenErr)
	}
	var wg sync.WaitGroup
	var verified atomic.Int64
	for i := range streams {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dir := filepath.Join(root, fmt.Sprint(i))
			cat, e := partlog.OpenNameCatalog(dir)
			must(e)
			d := partlog.CatalogDecoder{Catalog: cat}
			file, e := os.Open(filepath.Join(dir, "segment"))
			must(e)
			source, e := os.Open(fixture)
			must(e)
			var ordinal uint64
			for {
				b, e := readPacket(file)
				if e == io.EOF {
					break
				}
				must(e)
				want, e := readPacket(source)
				if e == io.EOF {
					_, e = source.Seek(0, 0)
					must(e)
					want, e = readPacket(source)
				}
				must(e)
				expected, n := catalogUnwire(want, cat)
				if unseenErr == nil {
					expected = churnFrames(expected, &ordinal, every)
				}
				verified.Add(n)
				got, e := d.Decode(b)
				must(e)
				catalogExact(expected, got)
			}
			must(file.Close())
			must(source.Close())
			d.Close()
			must(cat.Close())
		}(i)
	}
	wg.Wait()
	wall := time.Since(start).Seconds()
	emit(map[string]any{"stage": "catalog_verify", "points": verified.Load(), "seconds": wall, "m_points_per_second": float64(verified.Load()) / wall / 1e6, "exact": true})
	return verified.Load()
}

// First-observed names after the first complete segment are an upper bound on
// truly new daemon names: a finite log cohort cannot prove lifetime novelty.
func catalogSourceFixture(segs []segment, partition int, rate float64) {
	if *catalogFixturePoints < 1 {
		panic("fixture points must be positive")
	}
	root := filepath.Join(*catalogDir, "fixture")
	must(os.MkdirAll(root, 0700))
	c, e := partlog.OpenNameCatalog(filepath.Join(root, "template"))
	must(e)
	defer c.Close()
	f, e := os.Create(filepath.Join(root, "sender.fixture"))
	must(e)
	defer f.Close()
	var samplePoints, total, sourceBytes int64
	var sample []partlog.DictionaryFrame
	started := time.Now()
	for index, s := range segs {
		var h [64]byte
		_, e = s.f.ReadAt(h[:], 0)
		must(e)
		if h != s.header {
			panic("source recycled")
		}
		view, frontier, selected, e := selectPartition(s.f, s.Bytes, partition)
		must(e)
		r, e := inputlog.NewSegmentReplay(view)
		must(e)
		before := c.Count()
		var count, first, last int64
		for {
			ps, offset, n, e := r.Next(frontier)
			if e == io.EOF {
				break
			}
			must(e)
			for _, p := range ps {
				if first == 0 || p.FirstArrival < first {
					first = p.FirstArrival
				}
				if p.FirstArrival > last {
					last = p.FirstArrival
				}
				_, e = c.Intern(p.Metric, nil)
				must(e)
				if samplePoints < *catalogFixturePoints {
					frame := partlog.DictionaryFrame{Key: p.Metric, Clock: p.FirstArrival, Points: p.Data}
					sample = append(sample, frame)
					samplePoints += int64(len(p.Data))
				}
			}
			count += int64(n)
			if delay := time.Duration(float64(sourceBytes+offset)/(rate*1e6)*float64(time.Second)) - time.Since(started); delay > 0 {
				time.Sleep(delay)
			}
		}
		r.Close()
		_, e = s.f.ReadAt(h[:], 0)
		must(e)
		if h != s.header || count != selected {
			panic("source changed")
		}
		sourceBytes += frontier
		total += count
		emit(map[string]any{"stage": "source_name_churn", "segment": s.Path, "warmup": index == 0, "points": count, "first_observed_names": c.Count() - before, "first_arrival_ns": first, "last_arrival_ns": last, "seconds": float64(last-first) / 1e9, "bpp": float64(frontier) / float64(count), "novelty_scope": "first observed in finite cohort; later segments upper-bound lifetime novelty"})
	}
	for _, b := range catalogWire(sample, c) {
		writePacket(f, b)
	}
	must(c.Checkpoint())
	must(f.Sync())
	emit(map[string]any{"stage": "source_fixture", "points": total, "sample_points": samplePoints, "catalog_names": c.Count(), "source_bytes": sourceBytes, "fixture": root})
}
