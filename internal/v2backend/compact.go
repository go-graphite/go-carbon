package v2backend

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/go-graphite/go-carbon/internal/daemonstore"
	"github.com/go-graphite/go-carbon/internal/partsrun"
	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

// A canonical series key is the dictionary entry for a bounded archive run.
// Names, placement and physical-slot keys are never repeated in its rows.
// The outer GCP3 prefix still scopes each record to one PG/vshard/partition.
const compactArchive = math.MaxUint32 - 1
const compactRecords = 4096

func appendCompactRecords(dst []byte, records []sealrollup.Record, w *partsrun.Workspace) ([]byte, error) {
	if len(records) == 0 || len(records) > compactRecords {
		return nil, errors.New("invalid compact archive record count")
	}
	dst = append(dst, 'V', '2', 'R', 1)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(records)))
	for start := 0; start < len(records); {
		end := start + 1
		for end < len(records) && records[end].Archive == records[start].Archive {
			end++
		}
		dst = binary.BigEndian.AppendUint32(dst, uint32(records[start].Archive))
		lengthAt := len(dst)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		runAt := len(dst)
		var err error
		dst, err = partsrun.AppendWithWorkspace(dst, records[start].Sample.Time, end-start, func(i int) partsrun.Sample {
			r := records[start+i]
			return partsrun.Sample{Time: r.Sample.Time, Bits: r.Sample.Bits, Sequence: r.Order}
		}, w)
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint32(dst[lengthAt:], uint32(len(dst)-runAt))
		// Correction is diagnostic, but retaining it makes the record codec exact.
		for i := start; i < end; i += 8 {
			var flags byte
			for j := i; j < min(i+8, end); j++ {
				if records[j].Correction {
					flags |= 1 << uint(j-i)
				}
			}
			dst = append(dst, flags)
		}
		start = end
	}
	return dst, nil
}

func visitCompactRecords(b []byte, policy daemonstore.Metric, w *partsrun.Workspace, visit func(sealrollup.Record) error) error {
	if len(b) < 8 || string(b[:4]) != "V2R\x01" {
		return errors.New("invalid compact archive header")
	}
	remaining := int(binary.BigEndian.Uint32(b[4:8]))
	if remaining < 1 || remaining > compactRecords {
		return errors.New("invalid compact archive count")
	}
	b = b[8:]
	for remaining > 0 {
		if len(b) < 8 {
			return errors.New("truncated compact archive group")
		}
		archive, size := uint64(binary.BigEndian.Uint32(b)), uint64(binary.BigEndian.Uint32(b[4:]))
		b = b[8:]
		if archive >= uint64(len(policy.Archives)) || size > uint64(len(b)) {
			return errors.New("invalid compact archive group")
		}
		run := b[:int(size)]
		b = b[int(size):]
		header, err := partsrun.Inspect(run, remaining)
		if err != nil || header.Count == 0 {
			return errors.New("invalid compact archive run")
		}
		nflags := (header.Count + 7) / 8
		if len(b) < nflags || header.Count%8 != 0 && b[nflags-1]>>uint(header.Count%8) != 0 {
			return errors.New("invalid compact correction flags")
		}
		flags := b[:nflags]
		b = b[nflags:]
		a := policy.Archives[archive]
		if a.Step <= 0 || a.Points <= 0 {
			return errors.New("invalid compact archive policy")
		}
		i := 0
		err = partsrun.VisitWithWorkspace(run, remaining, func(p partsrun.Sample) error {
			if p.Time <= 0 || p.Time > math.MaxUint32 || p.Time%int64(a.Step) != 0 || p.Sequence == 0 {
				return errors.New("invalid compact archive sample")
			}
			r := sealrollup.Record{Correction: flags[i/8]&(1<<uint(i%8)) != 0}
			r.Archive, r.Slot, r.Order = int(archive), (p.Time/int64(a.Step))%int64(a.Points), p.Sequence
			r.Sample = sealrollup.Sample{Time: p.Time, Bits: p.Bits, Sequence: p.Sequence}
			i++
			return visit(r)
		}, w)
		if err != nil {
			return err
		}
		remaining -= header.Count
	}
	if len(b) != 0 {
		return errors.New("trailing compact archive bytes")
	}
	return nil
}
