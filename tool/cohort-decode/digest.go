package main

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"slices"
	"strconv"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func nativeFrame(input io.Reader) ([]byte, error) {
	var n uint64
	if err := binary.Read(input, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	if n == 0 || n > 64<<20 {
		return nil, errors.New("digest frame exceeds 64MiB")
	}
	raw := make([]byte, n)
	_, err := io.ReadFull(input, raw)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return raw, err
}

// Point frames are used only after unequal digest buckets have been selected.
func pointFrames(input io.Reader, output io.Writer) error {
	encoder := json.NewEncoder(output)
	for {
		raw, err := nativeFrame(input)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		records, err := store.ReadTransfer(bytes.NewReader(raw), 64<<20)
		if err != nil {
			return err
		}
		rows := make(map[string]any, len(records))
		for _, record := range records {
			name := record.Config.Name
			if _, ok := rows[name]; ok {
				return errors.New("duplicate native metric")
			}
			if record.Version.ID == 0 {
				rows[name] = nil
				continue
			}
			snap, err := nativeSnapshot(record)
			if err != nil {
				return err
			}
			archives := make([]any, len(snap.Archives))
			for ai, a := range snap.Archives {
				points := make(map[string][]string)
				for _, p := range a.Points {
					if p.Timestamp <= 0 {
						continue
					}
					key := strconv.FormatInt(p.Timestamp, 10)
					bits := fmt.Sprintf("%016x", math.Float64bits(p.Value))
					if !slices.Contains(points[key], bits) {
						points[key] = append(points[key], bits)
					}
				}
				archives[ai] = map[string]any{"step": a.Retention.Step, "count": a.Retention.Count, "points": points}
			}
			rows[name] = map[string]any{"archives": archives}
		}
		if err = encoder.Encode(rows); err != nil {
			return err
		}
	}
}

// A frame is a uint64 big-endian byte length followed by an existing native
// export. Its checksum is verified by decode before any digest is returned.
// Framing keeps one decoder alive across batches without retaining histories.
func digestFrames(input io.Reader, output io.Writer, from, until int64) error {
	return digestFramesWindows(input, output, from, until, nil)
}

func digestFramesWindows(input io.Reader, output io.Writer, from, until int64, windows []window) error {
	for _, w := range windows {
		if w.From < 0 || w.Until <= w.From {
			return errors.New("invalid digest window")
		}
	}
	if from < 0 || until <= from {
		return errors.New("digest requires 0 <= from < until")
	}
	encoder := json.NewEncoder(output)
	for {
		raw, err := nativeFrame(input)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		rows, err := store.ReadTransfer(bytes.NewReader(raw), 64<<20)
		if err != nil {
			return err
		}
		out := make(map[string]any, len(rows))
		for _, record := range rows {
			name := record.Config.Name
			if _, ok := out[name]; ok {
				return errors.New("duplicate native metric")
			}
			if record.Version.ID == 0 {
				out[name] = nil
				continue
			}
			row, err := nativeSnapshot(record)
			if err != nil {
				return err
			}
			row = orderedFiniteSnapshot(row)
			primary := orderedSnapshotDigest(row, from, until)
			if len(windows) > 0 {
				extra := make(map[int]any)
				for i, w := range windows {
					digest := orderedSnapshotDigest(row, w.From, w.Until)
					if len(digest["buckets"].([]any)) > 0 {
						extra[i] = digest
					}
				}
				primary["windows"] = extra
				primary["windows_complete"] = true
			}
			out[name] = primary
		}
		if err := encoder.Encode(out); err != nil {
			return err
		}
	}
}

// Order and deduplicate once per native record. Window digests use binary search
// into these immutable slices, so adding restart windows does not rescan history.
func orderedFiniteSnapshot(row store.Snapshot) store.Snapshot {
	row.Archives = slices.Clone(row.Archives)
	for ai := range row.Archives {
		archive := &row.Archives[ai]
		points := make([]store.Point, 0, len(archive.Points))
		for _, p := range archive.Points {
			if p.Timestamp > 0 && !math.IsNaN(p.Value) && !math.IsInf(p.Value, 0) {
				points = append(points, p)
			}
		}
		slices.SortFunc(points, func(a, b store.Point) int {
			if order := cmp.Compare(a.Timestamp, b.Timestamp); order != 0 {
				return order
			}
			return cmp.Compare(math.Float64bits(a.Value), math.Float64bits(b.Value))
		})
		unique := points[:0]
		for _, p := range points {
			if len(unique) > 0 && unique[len(unique)-1].Timestamp == p.Timestamp && math.Float64bits(unique[len(unique)-1].Value) == math.Float64bits(p.Value) {
				continue
			}
			unique = append(unique, p)
		}
		archive.Points = unique
	}
	return row
}

func snapshotDigest(row store.Snapshot, from, until int64) map[string]any {
	return orderedSnapshotDigest(orderedFiniteSnapshot(row), from, until)
}

func orderedSnapshotDigest(row store.Snapshot, from, until int64) map[string]any {
	buckets := []any{}
	ambiguous := [][3]int64{}
	archives := [][2]int{}
	for ai, archive := range row.Archives {
		archives = append(archives, [2]int{archive.Retention.Step, archive.Retention.Count})
		begin, _ := slices.BinarySearchFunc(archive.Points, from, func(p store.Point, ts int64) int { return cmp.Compare(p.Timestamp, ts) })
		end, _ := slices.BinarySearchFunc(archive.Points, until, func(p store.Point, ts int64) int { return cmp.Compare(p.Timestamp, ts) })
		points := archive.Points[begin:end]

		for begin := 0; begin < len(points); {
			end := begin + 1
			for end < len(points) && points[end].Timestamp == points[begin].Timestamp {
				end++
			}
			if end-begin > 1 {
				ambiguous = append(ambiguous, [3]int64{int64(ai), points[begin].Timestamp, int64(end - begin)})
			}
			begin = end
		}
		for begin := 0; begin < len(points); {
			day := points[begin].Timestamp / 86400
			h := fnv.New64a()
			end := begin
			for end < len(points) && points[end].Timestamp/86400 == day {
				var point [16]byte
				binary.BigEndian.PutUint64(point[:8], uint64(points[end].Timestamp))
				binary.BigEndian.PutUint64(point[8:], math.Float64bits(points[end].Value))
				_, _ = h.Write(point[:])
				end++
			}
			buckets = append(buckets, []any{ai, day, end - begin, fmt.Sprintf("%016x", h.Sum64()), points[begin].Timestamp, points[end-1].Timestamp})
			begin = end
		}
	}
	return map[string]any{"archives": archives, "buckets": buckets, "ambiguous": ambiguous}
}
