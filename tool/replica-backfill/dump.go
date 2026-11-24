package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"
)

// The JSONL wire format is produced by tool/store-readonly (PR #1734).
// Only points are data: last/last_update and archive counters are summaries.
type dumpPoint struct {
	Timestamp int64  `json:"t"`
	Bits      string `json:"bits"`
}
type dumpArchive struct {
	Step   int64       `json:"step"`
	Count  int64       `json:"count"`
	Points []dumpPoint `json:"points"`
}
type dumpRow struct {
	Name     string        `json:"name"`
	Error    string        `json:"error"`
	Archives []dumpArchive `json:"archives"`
}
type dumpOffset struct {
	offset int64
	length int
}
type dumpSource struct {
	file  *os.File
	index map[string]dumpOffset
	now   int64
}

const maxDumpRow = 64 << 20

// openDump validates the entire input before any writes can start. Both plain
// and gzip inputs are spooled to a private temporary file; only names/offsets
// stay resident. ReadAt supports concurrent batches and unsorted input. Disk
// use is the uncompressed input size; Close removes it, including on failure.
func openDump(ctx context.Context, path string, now int64) (_ *dumpSource, err error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	br := bufio.NewReader(in)
	var reader io.Reader = br
	magic, _ := br.Peek(2)
	if len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, e := gzip.NewReader(br)
		if e != nil {
			return nil, e
		}
		defer gz.Close()
		reader = gz
	}
	f, err := os.CreateTemp("", "replica-backfill-dump-*")
	if err != nil {
		return nil, err
	}
	d := &dumpSource{file: f, index: make(map[string]dumpOffset), now: now}
	defer func() {
		if err != nil {
			_ = d.Close()
		}
	}()
	sc := bufio.NewScanner(reader)
	sc.Buffer(make([]byte, 64<<10), maxDumpRow)
	bw := bufio.NewWriterSize(f, 256<<10)
	var offset int64
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row, e := decodeDumpRow(sc.Bytes())
		if e != nil {
			return nil, fmt.Errorf("dump row at byte %d: %w", offset, e)
		}
		if _, exists := d.index[row.Name]; exists {
			return nil, fmt.Errorf("duplicate dump name at byte %d", offset)
		}
		n := len(sc.Bytes())
		d.index[row.Name] = dumpOffset{offset, n}
		if _, err = bw.Write(sc.Bytes()); err != nil {
			return nil, err
		}
		if err = bw.WriteByte('\n'); err != nil {
			return nil, err
		}
		offset += int64(n) + 1
	}
	if err = sc.Err(); err != nil {
		return nil, err
	}
	if err = bw.Flush(); err != nil {
		return nil, err
	}
	return d, nil
}

func decodeDumpRow(b []byte) (dumpRow, error) {
	var row dumpRow
	if err := json.Unmarshal(b, &row); err != nil {
		return row, err
	}
	if !validName(row.Name) {
		return row, errors.New("invalid or missing dump name")
	}
	if row.Error != "" && row.Error != "not found" {
		return row, fmt.Errorf("reader error: %s", row.Error)
	}
	if row.Error != "" && len(row.Archives) > 0 {
		return row, errors.New("reader error with archives")
	}
	steps := make(map[int64]bool, len(row.Archives))
	for _, a := range row.Archives {
		if a.Step <= 0 || a.Count <= 0 || a.Count > math.MaxInt64/a.Step || steps[a.Step] {
			return row, errors.New("invalid or duplicate archive retention")
		}
		steps[a.Step] = true
		for _, p := range a.Points {
			if p.Timestamp <= 0 || p.Timestamp%a.Step != 0 || len(p.Bits) != 16 {
				return row, errors.New("invalid dump point")
			}
			if _, err := strconv.ParseUint(p.Bits, 16, 64); err != nil {
				return row, fmt.Errorf("invalid point bits: %w", err)
			}
		}
	}
	return row, nil
}

func (d *dumpSource) Close() error {
	return errors.Join(d.file.Close(), os.Remove(d.file.Name()))
}

func (c *client) fetchSource(ctx context.Context, names []string, from, until int64, gaps map[string]gap) (out map[string]series, err error) {
	if c.dump == nil {
		return c.fetch(ctx, names, from, until)
	}
	start := time.Now()
	c.stats.requests.Add(1)
	defer func() {
		c.stats.nanos.Add(int64(time.Since(start)))
		if err != nil {
			c.stats.failures.Add(1)
			c.stats.errors.Add(1)
		} else {
			c.stats.ok.Add(1)
		}
	}()
	out = make(map[string]series, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		loc, ok := c.dump.index[name]
		if !ok {
			continue
		}
		b := make([]byte, loc.length)
		if _, err := c.dump.file.ReadAt(b, loc.offset); err != nil {
			return nil, err
		}
		c.stats.bytesIn.Add(int64(len(b)))
		row, err := decodeDumpRow(b)
		if err != nil {
			return nil, err
		}
		selected := -1
		fallback := -1
		for i, a := range row.Archives {
			if fallback < 0 || a.Step > row.Archives[fallback].Step {
				fallback = i
			}
			if gaps[name].step > 0 {
				if a.Step == gaps[name].step {
					selected = i
					break
				}
			} else if a.Step*a.Count >= c.dump.now-from && (selected < 0 || a.Step < row.Archives[selected].Step) {
				selected = i
			}
		}
		// Outside every retention, HTTP clamps to the coarsest archive.
		// A missing matching step still goes through the engine's mismatch check.
		if selected < 0 {
			selected = fallback
		}
		if selected < 0 {
			continue
		}
		a := row.Archives[selected]
		first, last := int64(math.MaxInt64), int64(0)
		for _, p := range a.Points {
			if p.Timestamp > from && p.Timestamp <= until {
				first = min(first, p.Timestamp)
				last = max(last, p.Timestamp)
			}
		}
		if last == 0 {
			continue
		}
		slots := (last-first)/a.Step + 1
		if slots > 8<<20 {
			return nil, errors.New("dump series exceeds 8M slots; use a smaller time window")
		}
		s := series{Start: first, Step: a.Step, Values: make([]float64, int(slots))}
		for i := range s.Values {
			s.Values[i] = math.NaN()
		}
		for _, p := range a.Points {
			if p.Timestamp < first || p.Timestamp > last {
				continue
			}
			bits, _ := strconv.ParseUint(p.Bits, 16, 64)
			s.Values[(p.Timestamp-first)/a.Step] = math.Float64frombits(bits)
		}
		out[name] = s
	}
	return out, nil
}
