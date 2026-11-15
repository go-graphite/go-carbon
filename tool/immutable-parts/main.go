// immutable-parts is an offline experiment. It is not a daemon storage format.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/persister"
	"github.com/go-graphite/go-carbon/tags"
	whisper "github.com/go-graphite/go-whisper"
)

const buckets = 192
const recordBytes = 32

// Sequence is the input order, independent of sorting or part publication.
type row struct {
	ID        uint64
	Time      int64
	Bits, Seq uint64
}
type metric struct {
	Name   string
	Steps  []int
	Method whisper.AggregationMethod
	XFF    float64
}
type catalogue struct {
	Metrics          []metric
	Points           uint64
	MinTime, MaxTime int64
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func putRow(b []byte, r row) {
	binary.LittleEndian.PutUint64(b, r.ID)
	binary.LittleEndian.PutUint64(b[8:], uint64(r.Time))
	binary.LittleEndian.PutUint64(b[16:], r.Bits)
	binary.LittleEndian.PutUint64(b[24:], r.Seq)
}
func getRow(b []byte) row {
	return row{binary.LittleEndian.Uint64(b), int64(binary.LittleEndian.Uint64(b[8:])), binary.LittleEndian.Uint64(b[16:]), binary.LittleEndian.Uint64(b[24:])}
}
func rawPath(root string, b int) string { return filepath.Join(root, fmt.Sprintf("raw-%03d.bin", b)) }
func prepare(segment, root, schemaFile, aggregationFile string, limit uint64) error {
	if err := os.Mkdir(root, 0750); err != nil {
		return err
	}
	var schemas persister.WhisperSchemas
	var err error
	if schemaFile != "" {
		schemas, err = persister.ReadWhisperSchemas(schemaFile)
		if err != nil {
			return err
		}
	}
	aggr := persister.NewWhisperAggregation()
	if aggregationFile != "" {
		aggr, err = persister.ReadWhisperAggregation(aggregationFile)
		if err != nil {
			return err
		}
	}
	f, err := os.Open(segment)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	frontier, err := sealedFrontier(f, stat.Size())
	if err != nil {
		return err
	}
	replay, err := inputlog.NewSegmentReplay(f)
	if err != nil {
		return err
	}
	defer replay.Close()
	files := make([]*os.File, buckets)
	writers := make([]*bufio.Writer, buckets)
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()
	for i := range files {
		files[i], err = os.Create(rawPath(root, i))
		if err != nil {
			return err
		}
		writers[i] = bufio.NewWriterSize(files[i], 256<<10)
	}
	cat := catalogue{MinTime: math.MaxInt64, MaxTime: math.MinInt64}
	ids := map[string]uint64{}
	start := time.Now()
	var buf [recordBytes]byte
	for cat.Points < limit {
		records, _, _, e := replay.Next(frontier)
		if errors.Is(e, io.EOF) {
			break
		}
		if e != nil {
			return e
		}
		for _, p := range records {
			name, e := tags.Normalize(p.Metric)
			if e != nil {
				return e
			}
			id, ok := ids[name]
			if !ok {
				id = uint64(len(cat.Metrics))
				ids[name] = id
				steps := []int{1, 60, 3600}
				if schemas != nil {
					s, ok := schemas.Match(name)
					if !ok {
						return fmt.Errorf("no schema for %q", name)
					}
					steps = nil
					for _, r := range s.Retentions {
						steps = append(steps, r.SecondsPerPoint())
					}
				}
				a := aggr.Match(name)
				cat.Metrics = append(cat.Metrics, metric{name, steps, a.AggregationMethod(), a.XFilesFactor()})
			}
			for _, p := range p.Data {
				if cat.Points >= limit {
					break
				}
				r := row{id, p.Timestamp, math.Float64bits(p.Value), cat.Points}
				putRow(buf[:], r)
				if _, e := writers[id%buckets].Write(buf[:]); e != nil {
					return e
				}
				cat.Points++
				cat.MinTime = min(cat.MinTime, r.Time)
				cat.MaxTime = max(cat.MaxTime, r.Time)
			}
		}
	}
	for i, w := range writers {
		if err := w.Flush(); err != nil {
			return err
		}
		if err := files[i].Sync(); err != nil {
			return err
		}
	}
	out, e := os.Create(filepath.Join(root, "catalogue.json.tmp"))
	if e != nil {
		return e
	}
	defer out.Close()
	if e = json.NewEncoder(out).Encode(cat); e != nil {
		return e
	}
	if e = out.Sync(); e != nil {
		return e
	}
	if e = out.Close(); e != nil {
		return e
	}
	if e = os.Rename(filepath.Join(root, "catalogue.json.tmp"), filepath.Join(root, "catalogue.json")); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "prepare", "points": cat.Points, "metrics": len(cat.Metrics), "seconds": time.Since(start).Seconds(), "points_per_second": float64(cat.Points) / time.Since(start).Seconds()})
}
func loadCatalogue(root string) (catalogue, error) {
	var c catalogue
	b, e := os.ReadFile(filepath.Join(root, "catalogue.json"))
	if e == nil {
		e = json.Unmarshal(b, &c)
	}
	return c, e
}

// One producer and one consumer own each queue. Ownership is transferred with
// two atomic slots; no shared application data mutex protects rows or buffers.
type packet struct {
	Rows []row
	Err  error
	End  bool
}
type queue struct {
	slots       [2]atomic.Pointer[packet]
	write, read uint64
}

func (q *queue) push(p *packet) {
	s := &q.slots[q.write%2]
	for !s.CompareAndSwap(nil, p) {
		time.Sleep(10 * time.Microsecond)
	}
	q.write++
}
func (q *queue) pop() *packet {
	s := &q.slots[q.read%2]
	for {
		p := s.Swap(nil)
		if p != nil {
			q.read++
			return p
		}
		time.Sleep(10 * time.Microsecond)
	}
}
func produce(q *queue, root string, shard, shards, passes int, cat catalogue) {
	defer q.push(&packet{End: true})
	for pass := 0; pass < passes; pass++ {
		for b := shard; b < buckets; b += shards {
			f, e := os.Open(rawPath(root, b))
			if e != nil {
				q.push(&packet{Err: e})
				return
			}
			rd := bufio.NewReaderSize(f, 1<<20)
			buf := make([]byte, 32768*recordBytes)
			for {
				n, e := io.ReadFull(rd, buf)
				if n%recordBytes != 0 {
					f.Close()
					q.push(&packet{Err: io.ErrUnexpectedEOF})
					return
				}
				if n > 0 {
					rs := make([]row, n/recordBytes)
					for i := range rs {
						r := getRow(buf[i*recordBytes:])
						if int(r.ID%uint64(shards)) != shard {
							f.Close()
							q.push(&packet{Err: fmt.Errorf("fixture routing mismatch")})
							return
						}
						r.Seq += uint64(pass) * cat.Points
						r.Time += int64(pass) * (cat.MaxTime - cat.MinTime + 1)
						rs[i] = r
					}
					q.push(&packet{Rows: rs})
				}
				if e == io.EOF || e == io.ErrUnexpectedEOF {
					break
				}
				if e != nil {
					f.Close()
					q.push(&packet{Err: e})
					return
				}
			}
			f.Close()
		}
	}
}
func less(a, b row) int {
	if a.ID < b.ID {
		return -1
	}
	if a.ID > b.ID {
		return 1
	}
	if a.Time < b.Time {
		return -1
	}
	if a.Time > b.Time {
		return 1
	}
	if a.Seq < b.Seq {
		return -1
	}
	if a.Seq > b.Seq {
		return 1
	}
	return 0
}

// canonical aligns fine slots and chooses the latest source sequence in each.
func canonical(rs []row, step int) []row {
	out := make([]row, 0, len(rs))
	for i := 0; i < len(rs); {
		t := rs[i].Time - rs[i].Time%int64(step)
		best := rs[i]
		j := i + 1
		for j < len(rs) && rs[j].Time-rs[j].Time%int64(step) == t {
			if rs[j].Seq > best.Seq {
				best = rs[j]
			}
			j++
		}
		best.Time = t
		out = append(out, best)
		i = j
	}
	return out
}
func rollup(rs []row, from, to int, m metric) []row {
	out := make([]row, 0, len(rs)*from/to+1)
	for i := 0; i < len(rs); {
		t := rs[i].Time - rs[i].Time%int64(to)
		j := i
		count := 0
		var value float64
		var seq uint64
		for j < len(rs) && rs[j].Time-rs[j].Time%int64(to) == t {
			v := math.Float64frombits(rs[j].Bits)
			if !math.IsNaN(v) {
				switch m.Method {
				case whisper.Average, whisper.Sum:
					value += v
				case whisper.Min:
					if count == 0 || v < value {
						value = v
					}
				case whisper.Max:
					if count == 0 || v > value {
						value = v
					}
				case whisper.Last:
					value = v
				case whisper.First:
					if count == 0 {
						value = v
					}
				default:
					panic("unsupported aggregation")
				}

				count++
				seq = max(seq, rs[j].Seq)
			}
			j++
		}
		if count > 0 && float32(count)/float32(to/from) >= float32(m.XFF) {
			if m.Method == whisper.Average {
				value /= float64(count)
			}
			out = append(out, row{rs[i].ID, t, math.Float64bits(value), seq})
		}
		i = j
	}
	return out
}

// seal never calls DB.Get or reads a previous part. Rollup rows are provisional:
// a later correction or a partial occupancy window must be recomputed at merge.
func seal(db *pebble.DB, dir string, shard int, part uint64, rs []row, cat catalogue) (uint64, error) {
	slices.SortFunc(rs, less)
	path := filepath.Join(dir, fmt.Sprintf("stage-%03d-%012d.sst", shard, part))
	f, e := vfs.Default.Create(path)
	if e != nil {
		return 0, e
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{Compression: sstable.SnappyCompression})
	var key [32]byte
	value := make([]byte, 0, 256*24)
	emit := func(rows []row, res int) error {
		for k := 0; k < len(rows); k += 256 {
			end := min(k+256, len(rows))
			binary.BigEndian.PutUint64(key[:8], rows[k].ID)
			binary.BigEndian.PutUint32(key[8:12], uint32(res))
			binary.BigEndian.PutUint64(key[12:20], uint64(rows[k].Time)^(1<<63))
			binary.BigEndian.PutUint64(key[20:28], part)
			binary.BigEndian.PutUint32(key[28:], uint32(k))
			value = value[:0]
			for _, r := range rows[k:end] {
				var b [24]byte
				binary.LittleEndian.PutUint64(b[:8], uint64(r.Time))
				binary.LittleEndian.PutUint64(b[8:16], r.Bits)
				binary.LittleEndian.PutUint64(b[16:24], r.Seq)
				value = append(value, b[:]...)
			}
			if e := w.Set(key[:], value); e != nil {
				return e
			}
		}
		return nil
	}
	for i := 0; i < len(rs); {
		j := i + 1
		for j < len(rs) && rs[j].ID == rs[i].ID {
			j++
		}
		m := cat.Metrics[rs[i].ID]
		if e = emit(rs[i:j], 0); e != nil {
			w.Close()
			return 0, e
		}
		fine := canonical(rs[i:j], m.Steps[0])
		for k, step := range m.Steps {
			if k == 0 {
				continue
			}
			if k > 0 {
				fine = rollup(fine, m.Steps[k-1], step, m)
			}
			if e = emit(fine, step); e != nil {
				w.Close()
				return 0, e
			}
		}
		i = j
	}
	if e = w.Close(); e != nil {
		return 0, e
	}
	st, e := os.Stat(path)
	if e != nil {
		return 0, e
	}
	if e = db.Ingest([]string{path}); e != nil {
		return 0, e
	}
	return uint64(st.Size()), nil
}

type outcome struct {
	Points, Parts, Bytes uint64
	Err                  error
}

func consume(q *queue, db *pebble.DB, dir string, s int, cat catalogue, maxRows int, pub *publication, committed *atomic.Uint64) outcome {
	var o outcome
	rs := make([]row, 0, maxRows)
	deadline := time.Now().Add(time.Second)
	flush := func() {
		if len(rs) == 0 || o.Err != nil {
			return
		}
		b, e := seal(db, dir, s, o.Parts, rs, cat)
		o.Err = e
		o.Bytes += b
		if e == nil && committed != nil {
			committed.Add(uint64(len(rs)))
		}
		if e == nil && pub != nil {
			pub.publish(db.NewSnapshot())
		}
		o.Parts++
		rs = rs[:0]
		deadline = time.Now().Add(time.Second)
	}
	for {
		p := q.pop()
		if p.Err != nil {
			o.Err = p.Err
		}
		if p.End {
			break
		}
		o.Points += uint64(len(p.Rows))
		if o.Err != nil {
			continue
		}
		for len(p.Rows) > 0 && o.Err == nil {
			n := min(maxRows-len(rs), len(p.Rows))
			rs = append(rs, p.Rows[:n]...)
			p.Rows = p.Rows[n:]
			if len(rs) == maxRows || time.Now().After(deadline) {
				flush()
			}
		}
	}
	flush()
	return o
}
func run(root, out string, shards, passes, maxRows int) error {
	return runReaders(root, out, shards, passes, maxRows, 0)
}
func runReaders(root, out string, shards, passes, maxRows, readers int) error {
	return runConfigured(root, out, shards, passes, maxRows, readers, "")
}
func runConfigured(root, out string, shards, passes, maxRows, readers int, stepsOverride string) error {
	return runLayout(root, out, shards, passes, maxRows, readers, stepsOverride, false)
}
func runLayout(root, out string, shards, passes, maxRows, readers int, stepsOverride string, dbPerOwner bool) error {
	if maxRows < 1 || passes < 1 || readers < 0 {
		return fmt.Errorf("positive max-rows/passes and nonnegative readers required")
	}
	if shards < 1 || buckets%shards != 0 {
		return fmt.Errorf("shards must divide %d", buckets)
	}
	cat, e := loadCatalogue(root)
	if e != nil {
		return e
	}
	if stepsOverride != "" {
		var steps []int
		for _, v := range strings.Split(stepsOverride, ",") {
			n, e := strconv.Atoi(v)
			if e != nil || n <= 0 {
				return fmt.Errorf("invalid steps")
			}
			if len(steps) > 0 && (n <= steps[len(steps)-1] || n%steps[len(steps)-1] != 0) {
				return fmt.Errorf("steps must increase and divide")
			}
			steps = append(steps, n)
		}
		for i := range cat.Metrics {
			cat.Metrics[i].Steps = steps
		}
	}
	if e = os.Mkdir(out, 0750); e != nil {
		return e
	}
	nDB := 1
	if dbPerOwner {
		nDB = shards
	}
	dbs := make([]*pebble.DB, nDB)
	pubs := make([]*publication, nDB)
	for i := range dbs {
		dir := filepath.Join(out, "db")
		opts := &pebble.Options{MemTableSize: 16 << 20, MaxConcurrentCompactions: func() int { return min(shards, 16) }}
		if dbPerOwner {
			dir = filepath.Join(out, fmt.Sprintf("db-%03d", i))
			opts.MaxConcurrentCompactions = func() int { return 1 }
			opts.Experimental.TableCacheShards = 1
		}
		dbs[i], e = pebble.Open(dir, opts)
		if e != nil {
			for _, d := range dbs[:i] {
				_ = d.Close()
			}
			return e
		}
		pubs[i] = new(publication)
		pubs[i].publish(dbs[i].NewSnapshot())
	}
	start := time.Now()
	baseline := procStats()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "start", "proc": baseline, "shards": shards, "readers": readers, "db_per_owner": dbPerOwner})
	ctx, cancel := context.WithCancel(context.Background())
	readDone := make(chan readStats, readers)
	for i := 0; i < readers; i++ {
		go func() { readDone <- reader(ctx, pubs, cat) }()
	}
	done := make(chan outcome, shards)
	progress := make([]atomic.Uint64, shards)
	sampleStop := make(chan struct{})
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-sampleStop:
				return
			case <-tick.C:
				var n uint64
				for i := range progress {
					n += progress[i].Load()
				}
				_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "sample", "shards": shards, "points": n, "seconds": time.Since(start).Seconds(), "proc": procStats()})
			}
		}
	}()
	for s := 0; s < shards; s++ {
		q := new(queue)
		go produce(q, root, s, shards, passes, cat)
		go func(s int) {
			i := s % nDB
			done <- consume(q, dbs[i], out, s, cat, maxRows, pubs[i], &progress[s])
		}(s)
	}
	var total outcome
	for range shards {
		o := <-done
		total.Points += o.Points
		total.Parts += o.Parts
		total.Bytes += o.Bytes
		if o.Err != nil {
			total.Err = errors.Join(total.Err, o.Err)
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "ingest_complete", "points": total.Points, "seconds": time.Since(start).Seconds(), "proc": procStats()})
	close(sampleStop)
	<-sampleDone
	cancel()
	var reads readStats
	for i := 0; i < readers; i++ {
		r := <-readDone
		reads.N += r.N
		reads.Empty += r.Empty
		reads.OneHour += r.OneHour
		reads.SevenDays += r.SevenDays
		reads.Points += r.Points
		for j, n := range r.Hist {
			reads.Hist[j] += n
		}
		total.Err = errors.Join(total.Err, r.Err)
	}
	for _, pub := range pubs {
		pub.clear()
	}
	type closeResult struct {
		Err             error
		Debt, Compacted uint64
		Metrics         string
	}
	closed := make(chan closeResult, nDB)
	for _, db := range dbs {
		go func(db *pebble.DB) {
			e := db.Flush()
			m := db.Metrics()
			r := closeResult{Err: e, Debt: m.Compact.EstimatedDebt}
			for _, level := range m.Levels {
				r.Compacted += level.BytesCompacted
			}
			if nDB == 1 {
				r.Metrics = m.String()
			}
			r.Err = errors.Join(r.Err, db.Close())
			closed <- r
		}(db)
	}
	var debt, compacted uint64
	var metrics string
	for range nDB {
		r := <-closed
		debt += r.Debt
		compacted += r.Compacted
		metrics = r.Metrics
		total.Err = errors.Join(total.Err, r.Err)
	}
	elapsed := time.Since(start).Seconds()
	if total.Err != nil {
		return total.Err
	}
	if total.Points != cat.Points*uint64(passes) {
		return fmt.Errorf("point count mismatch: %d", total.Points)
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": "parts", "db_per_owner": dbPerOwner, "pebble_estimated_debt_bytes": debt, "pebble_compacted_bytes": compacted, "baseline_proc": baseline, "empty_reads": reads.Empty, "reads_1h": reads.OneHour, "reads_7d": reads.SevenDays, "gomaxprocs": runtime.GOMAXPROCS(0), "proc": procStats(), "readers": readers, "reads": reads.N, "reads_per_second": float64(reads.N) / elapsed, "read_p50_ms": reads.quantile(.50), "read_p99_ms": reads.quantile(.99), "read_points": reads.Points, "when_cet": time.Now().In(time.FixedZone("CET", 3600)).Format(time.RFC3339), "shards": shards, "points": total.Points, "seconds": elapsed, "points_per_second": float64(total.Points) / elapsed, "parts": total.Parts, "sst_bytes": total.Bytes, "sst_MB_per_second": float64(total.Bytes) / 1e6 / elapsed, "sst_bytes_per_input_point": float64(total.Bytes) / float64(total.Points), "pebble_metrics": metrics})
}
func main() {
	mode := flag.String("mode", "run", "prepare or run")
	journal := flag.String("journal", "", "sealed input journal")
	fixture := flag.String("fixture", "", "prepared records directory")
	out := flag.String("out", "", "new output directory")
	schemas := flag.String("schemas", "", "storage-schemas.conf (default 1s/60s/3600s)")
	aggregation := flag.String("aggregation", "", "storage-aggregation.conf")
	shards := flag.Int("shards", 16, "metric owner count (divisor of192)")
	steps := flag.String("steps", "", "override retention steps in seconds for controlled trials")
	dbPerOwner := flag.Bool("db-per-owner", false, "use a separate fresh Pebble per metric owner")
	readers := flag.Int("readers", 0, "concurrent render-style readers")
	passes := flag.Int("passes", 1, "fixture passes, later passes shifted in time")
	maxRows := flag.Int("max-rows", 262144, "byte bound per owner /32")
	limit := flag.Uint64("points", 450000000, "prepare point limit")
	flag.Parse()
	if *mode == "prepare" {
		must(prepare(*journal, *fixture, *schemas, *aggregation, *limit))
	} else if *mode == "check" {
		must(checkFixture(*fixture, *out))
	} else if *mode == "run" {
		must(runLayout(*fixture, *out, *shards, *passes, *maxRows, *readers, *steps, *dbPerOwner))
	} else {
		panic("unknown mode")
	}
}

// A publisher owns one reference. Acquisition only increments a nonzero count;
// the last release closes the Pebble snapshot, after all readers are done.
type pinned struct {
	s    *pebble.Snapshot
	refs atomic.Int64
}

func (p *pinned) release() {
	if p.refs.Add(-1) == 0 {
		p.s.Close()
	}
}

type publication struct{ current atomic.Pointer[pinned] }

func (p *publication) publish(s *pebble.Snapshot) {
	n := &pinned{s: s}
	n.refs.Store(1)
	if old := p.current.Swap(n); old != nil {
		old.release()
	}
}
func (p *publication) acquire() *pinned {
	for {
		n := p.current.Load()
		if n == nil {
			return nil
		}
		for r := n.refs.Load(); r > 0; r = n.refs.Load() {
			if n.refs.CompareAndSwap(r, r+1) {
				return n
			}
		}
	}
}
func (p *publication) clear() {
	if n := p.current.Swap(nil); n != nil {
		n.release()
	}
}

type readStats struct {
	N, Points, Empty, OneHour, SevenDays uint64
	Hist                                 [32]uint64
	Err                                  error
}

func (r readStats) quantile(q float64) float64 {
	threshold := uint64(float64(r.N) * q)
	var n uint64
	for i, v := range r.Hist {
		n += v
		if n >= threshold && n > 0 {
			return float64(uint64(1)<<i) / 1000
		}
	}
	return 0
}
func reader(ctx context.Context, pubs []*publication, cat catalogue) readStats {
	var out readStats
	for ctx.Err() == nil {
		start := time.Now()
		id := uint64(rand.IntN(len(cat.Metrics)))
		hours := int64(1)
		if rand.IntN(2) == 1 {
			hours = 168
		}
		hi := cat.MaxTime
		lo := hi - hours*3600
		pin := pubs[id%uint64(len(pubs))].acquire()
		if pin == nil {
			continue
		}
		rs, e := readMetric(pin.s, id, lo, hi)
		pin.release()
		if e != nil {
			out.Err = e
			return out
		}
		slices.SortFunc(rs, less)
		fine := canonical(rs, cat.Metrics[id].Steps[0])
		if hours > 1 {
			for k := 1; k < len(cat.Metrics[id].Steps); k++ {
				fine = rollup(fine, cat.Metrics[id].Steps[k-1], cat.Metrics[id].Steps[k], cat.Metrics[id])
			}
		}
		if len(fine) == 0 {
			out.Empty++
		}
		if hours == 1 {
			out.OneHour++
		} else {
			out.SevenDays++
		}
		out.Points += uint64(len(fine))
		us := max(1, time.Since(start).Microseconds())
		bucket := 0
		for us > 1 && bucket < 31 {
			us >>= 1
			bucket++
		}
		out.Hist[bucket]++
		out.N++
	}
	return out
}
func readMetric(s *pebble.Snapshot, id uint64, lo, hi int64) ([]row, error) {
	var lower, upper [12]byte
	binary.BigEndian.PutUint64(lower[:8], id)
	binary.BigEndian.PutUint64(upper[:8], id)
	binary.BigEndian.PutUint32(upper[8:], 1)
	it, e := s.NewIter(&pebble.IterOptions{LowerBound: lower[:], UpperBound: upper[:]})
	if e != nil {
		return nil, e
	}
	defer it.Close()
	var out []row
	for it.First(); it.Valid(); it.Next() {
		v := it.Value()
		if len(v)%24 != 0 {
			return nil, fmt.Errorf("bad value %s", hex.EncodeToString(it.Key()))
		}
		for len(v) > 0 {
			t := int64(binary.LittleEndian.Uint64(v))
			if t >= lo && t <= hi {
				out = append(out, row{id, t, binary.LittleEndian.Uint64(v[8:]), binary.LittleEndian.Uint64(v[16:])})
			}
			v = v[24:]
		}
	}
	return out, it.Error()
}

// Linux /proc counters are process-wide, including Pebble background threads.
func procStats() map[string]float64 {
	out := map[string]float64{}
	b, _ := os.ReadFile("/proc/self/io")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			v, _ := strconv.ParseFloat(f[1], 64)
			out[strings.TrimSuffix(f[0], ":")] = v
		}
	}
	b, _ = os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && (f[0] == "VmHWM:" || f[0] == "VmRSS:") {
			v, _ := strconv.ParseFloat(f[1], 64)
			out[strings.TrimSuffix(f[0], ":")+"_bytes"] = v * 1024
		}
	}
	b, _ = os.ReadFile("/proc/self/stat")
	i := strings.LastIndex(string(b), ")")
	if i >= 0 {
		f := strings.Fields(string(b[i+1:]))
		if len(f) > 12 {
			u, _ := strconv.ParseFloat(f[11], 64)
			st, _ := strconv.ParseFloat(f[12], 64)
			out["cpu_seconds_CLK_TCK100"] = (u + st) / 100
		}
	}
	return out
}

func sealedFrontier(f *os.File, size int64) (int64, error) {
	var seg [64]byte
	if _, e := f.ReadAt(seg[:], 0); e != nil {
		return 0, e
	}
	salt := binary.LittleEndian.Uint64(seg[16:])
	offset := int64(64)
	table := crc32.MakeTable(crc32.Castagnoli)
	for offset+52 <= size {
		var b [52]byte
		if _, e := f.ReadAt(b[:], offset); e != nil {
			return 0, e
		}
		if binary.LittleEndian.Uint32(b[:]) != 0x4c494347 || binary.LittleEndian.Uint64(b[8:]) != salt {
			break
		}
		if crc32.Checksum(b[:48], table) != binary.LittleEndian.Uint32(b[48:]) {
			return 0, fmt.Errorf("frame header CRC at%d", offset)
		}
		n := int64(binary.LittleEndian.Uint32(b[16:]))
		if n <= 0 || offset+52+n > size {
			return 0, fmt.Errorf("torn frame at%d", offset)
		}
		offset += 52 + n
	}
	return offset, nil
}
