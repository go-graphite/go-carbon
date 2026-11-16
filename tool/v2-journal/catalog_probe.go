package main

import (
	"bytes"
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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/partlog"
	"github.com/go-graphite/go-carbon/points"
)

var catalogVerifyDir = flag.String("catalog-verify", "", "verify an already completed capture root")
var catalogDir = flag.String("catalog-dir", "", "catalog experiment output root (enables catalog comparison)")
var captureSeconds = flag.Int("capture-seconds", 0, "sustained group-fsync capture seconds after matched-byte comparison")
var captureAfter = flag.Int64("capture-after-unix", 0, "do not begin capture before this Unix second")
var captureStreams = flag.Int("capture-streams", 32, "independent capture streams")
var catalogSender = flag.String("catalog-sender", "", "internal disjoint sender: address")
var catalogFixture = flag.String("catalog-fixture", "", "internal sender fixture")
var catalogSendDuration = flag.Int("catalog-send-seconds", 120, "internal sender duration")
var catalogSendStreams = flag.Int("catalog-send-streams", 32, "internal sender streams")
var catalogRecapture = flag.String("catalog-recapture", "", "load a completed capture fixture instead of rereading the input log")
var captureUnseen = flag.Bool("capture-unseen", false, "give every tenth point a new never-repeated name before durable encoding")

// unseenFrames is deterministic across packet boundaries and fixture repeats.
// It preserves every tuple and order, changing only every tenth point's name.
func unseenFrames(fs []partlog.DictionaryFrame, ordinal *uint64) []partlog.DictionaryFrame {
	var out []partlog.DictionaryFrame
	for _, f := range fs {
		for start := 0; start < len(f.Points); {
			n := min(len(f.Points)-start, int(9-*ordinal%10))
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
	var h [4]byte
	if _, e := io.ReadFull(r, h[:]); e != nil {
		return nil, e
	}
	n := binary.LittleEndian.Uint32(h[:])
	if n > 64<<20 {
		return nil, fmt.Errorf("packet too big")
	}
	b := make([]byte, int(n))
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
	var count, encoded atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	var syncNS []int64
	var catalogSyncNS []int64
	start := time.Now()
	startCPU := cpu()
	must(cmd.Start())
	for i := range streams {
		conn, e := listener.Accept()
		must(e)
		wg.Add(1)
		go func(i int, conn net.Conn) {
			defer wg.Done()
			defer conn.Close()
			dir := filepath.Join(root, fmt.Sprint(i))
			cat, e := partlog.OpenNameCatalog(dir)
			must(e)
			defer cat.Close()
			enc := partlog.CatalogEncoder{Catalog: cat, SeriesClocks: true, SparseClocks: true}
			defer enc.Close()
			file, e := os.Create(filepath.Join(dir, "segment"))
			must(e)
			defer file.Close()
			var ordinal uint64
			next := time.Now().Add(time.Second)
			syncGroup := func() {
				t := time.Now()
				must(cat.Sync())
				must(file.Sync())
				mu.Lock()
				syncNS = append(syncNS, time.Since(t).Nanoseconds())
				mu.Unlock()
			}
			for {
				b, e := readPacket(conn)
				if e == io.EOF {
					break
				}
				must(e)
				frames, n := catalogUnwire(b, cat)
				if *captureUnseen {
					frames = unseenFrames(frames, &ordinal)
				}
				record, e := enc.Encode(frames)
				must(e)
				// Catalog definitions must be durable before their journal record
				// can be appended and eventually acknowledged by group sync.
				t := time.Now()
				must(cat.Sync())
				if *captureUnseen {
					mu.Lock()
					catalogSyncNS = append(catalogSyncNS, time.Since(t).Nanoseconds())
					mu.Unlock()
				}
				writePacket(file, record)
				count.Add(n)
				encoded.Add(int64(len(record) + 4))
				if time.Now().After(next) {
					syncGroup()
					next = next.Add(time.Second)
				}
			}
			syncGroup()
		}(i, conn)
	}
	wg.Wait()
	must(cmd.Wait())
	wall := time.Since(start).Seconds()
	cores := float64(cpu()-startCPU) / 1e9 / wall
	sort.Slice(syncNS, func(i, j int) bool { return syncNS[i] < syncNS[j] })
	p99 := syncNS[min(len(syncNS)-1, len(syncNS)*99/100)]
	var catalogP99 int64
	if len(catalogSyncNS) > 0 {
		sort.Slice(catalogSyncNS, func(i, j int) bool { return catalogSyncNS[i] < catalogSyncNS[j] })
		catalogP99 = catalogSyncNS[len(catalogSyncNS)*99/100]
		must(os.WriteFile(filepath.Join(root, "unseen"), []byte("every tenth point; per-stream ordinal\n"), 0600))
	}
	var durableBytes int64
	for i := range streams {
		durableBytes += catalogDisk(filepath.Join(root, fmt.Sprint(i)))
	}
	emit(map[string]any{"stage": "capture_storage", "points": count.Load(), "durable_bytes": durableBytes, "bpp": float64(durableBytes) / float64(count.Load()), "catalog_fsync_p99_ms": float64(catalogP99) / 1e6, "catalog_syncs": len(catalogSyncNS), "unseen": *captureUnseen})
	emit(map[string]any{"stage": "capture", "streams": streams, "seconds_including_final_sync": wall, "points": count.Load(), "m_points_per_second": float64(count.Load()) / wall / 1e6, "cores": cores, "record_bpp": float64(encoded.Load()) / float64(count.Load()), "seed_catalog_bytes_per_stream": len(cp), "fsync_group_p99_ms": float64(p99) / 1e6, "group_syncs": len(syncNS), "exact": "tuple codec differential before run; sustained replay verification follows", "sender": "separate taskset96-191 process, raw TCP, repeated complete real cohort; capture taskset0-95"})
	verified := catalogVerify(root, streams)
	if verified != count.Load() {
		panic("capture verification count")
	}
	emit(map[string]any{"stage": "capture_replay", "points": verified, "exact": true})
}

func uvarintBytes(n uint64) int { var b [10]byte; return binary.PutUvarint(b[:], n) }

func catalogVerify(root string, streams int) int64 {
	runtime.GOMAXPROCS(96)
	fixture := filepath.Join(root, "sender.fixture")
	_, unseenErr := os.Stat(filepath.Join(root, "unseen"))
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
					expected = unseenFrames(expected, &ordinal)
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
	return verified.Load()
}
