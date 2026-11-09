package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/floatresearch"
)

type outputFile struct {
	*os.File
	r        *runner
	bytes    uint64
	finished bool
}

func (f *outputFile) Write(p []byte) error {
	if err := f.r.ctx.Err(); err != nil {
		return err
	}
	if uint64(len(p)) > f.r.outputLimit-f.r.outputBytes {
		return errors.New("outputs would exceed 2x sampled input")
	}
	if f.bytes>>26 != (f.bytes+uint64(len(p)))>>26 {
		var st syscall.Statfs_t
		if err := syscall.Statfs(f.r.cfg.tmp, &st); err != nil {
			return err
		}
		if st.Bavail*uint64(st.Bsize) < 25_000_000_000 {
			return errors.New("less than 25 GB free; stopping")
		}
	}
	n, err := f.File.Write(p)
	f.bytes += uint64(n)
	f.r.outputBytes += uint64(n)
	if err == nil && n != len(p) {
		return errors.New("short output write")
	}
	return err
}
func (f *outputFile) Finish() error {
	f.finished = true
	return errors.Join(f.File.Sync(), f.File.Close())
}
func (f *outputFile) Abort() {
	if !f.finished {
		_ = f.File.Close()
		f.finished = true
	}
}
func cpuNS() (int64, error) {
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		return 0, err
	}
	return (int64(u.Utime.Sec)+int64(u.Stime.Sec))*1e9 + (int64(u.Utime.Usec)+int64(u.Stime.Usec))*1000, nil
}
func measured(fn func() error) (int64, error) {
	start, err := cpuNS()
	if err != nil {
		return 0, err
	}
	err = fn()
	end, e := cpuNS()
	return end - start, errors.Join(err, e)
}
func safeCandidate(policy string, g floatresearch.Group, original []byte) (kind string, data []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("codec panic: %v", p)
		}
	}()
	if policy == "float-codec" || policy == "float-codec-min" {
		fn := chunkstore.EncodeSealedPointsFloat
		if policy == "float-codec-min" {
			fn = chunkstore.EncodeSealedPointsFloatMin
		}
		data, err = fn(g.Points, g.Step)
		if err != nil {
			return "", nil, err
		}
		if len(data) >= len(original) {
			return "sealed", original, nil
		}
		return "float-codec", data, nil
	}
	if policy == "current" {
		_, err = codec.Encode("sealed", g)
		return "sealed", original, err
	}
	// Include creation of the full current fallback in encode CPU, as a real
	// sealing writer would need it even though this offline input has a frame.
	baseline, err := codec.Encode("sealed", g)
	if err != nil {
		return "", nil, err
	}
	kind, data, err = codec.Select(policy, g, baseline)
	if err != nil {
		return "", nil, err
	}
	if kind == "sealed" {
		return "sealed", original, nil
	}
	if kind != "sealed" {
		tag := byte(0)
		switch kind {
		case "chimp128":
			tag = 1
		case "alp":
			tag = 2
		case "dictionary":
			tag = 3
		}
		// Offline-only envelope makes each selected candidate self-describing and
		// accounts for its dispatch byte cost in the actual SST measurement.
		data = append([]byte{255, tag}, data...)
		if len(data) >= len(original) {
			kind, data = "sealed", original
		}
	}
	return kind, data, nil
}
func decodeCandidate(data []byte) (points []chunkstore.Point, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("decode panic: %v", p)
		}
	}()
	if len(data) > 2 && data[0] == 255 {
		kind := ""
		switch data[1] {
		case 1:
			kind = "chimp128"
		case 2:
			kind = "alp"
		case 3:
			kind = "dictionary"
		default:
			return nil, errors.New("unknown private envelope")
		}
		return codec.Decode(kind, data[2:])
	}
	return chunkstore.DecodeSealedPoints(data)
}
func samePoints(a, b []chunkstore.Point) bool {
	if len(a) != len(b) {
		return false
	}
	for i, p := range a {
		if p.Timestamp != b[i].Timestamp || math.Float64bits(p.Value) != math.Float64bits(b[i].Value) {
			return false
		}
	}
	return true
}
func (r *runner) transcode(path string, out *fileReport, opts sstable.WriterOptions) error {
	work, err := os.MkdirTemp(r.cfg.tmp, "sealed-inventory-")
	if err != nil {
		return err
	}
	r.tempDirs = append(r.tempDirs, work)
	success := false
	defer func() {
		if !r.cfg.keep || !success {
			_ = os.RemoveAll(work)
		}
	}()
	policies := append([]string{"baseline"}, strings.Split(r.cfg.policies, ",")...)
	seen := map[string]bool{}
	for _, policy := range policies {
		if seen[policy] {
			continue
		}
		seen[policy] = true
		if err = spaceCheck(r.cfg.tmp, 0); err != nil {
			return err
		}
		destination := filepath.Join(work, out.Name+"."+policy+".sst")
		stats, e := r.rewrite(path, destination, policy, opts)
		if e != nil {
			return e
		}
		if policy == "baseline" {
			out.BaselineBytes = stats.Bytes
			out.BaselineDeltaPct = (float64(stats.Bytes)/float64(out.Bytes) - 1) * 100
			out.BaselineWithin3Pct = math.Abs(out.BaselineDeltaPct) <= 3
			fmt.Fprintf(os.Stderr, "file=%s baseline_delta_pct=%.3f within_3pct=%t\n", out.Name, out.BaselineDeltaPct, out.BaselineWithin3Pct)
		} else {

			out.Policies[policy] = stats
		}
		if !r.cfg.keep {
			if e = os.Remove(destination); e != nil {
				return e
			}
			r.outputBytes -= stats.Bytes
		}
	}
	if r.cfg.keep {
		fmt.Fprintln(os.Stderr, "kept private tables:", work)
	}
	finishPolicies(out.Policies, out.BaselineBytes, out.FloatDataBytes)
	success = true
	return nil
}
func (r *runner) rewrite(path, destination, policy string, opts sstable.WriterOptions) (*policyReport, error) {
	reader, _, err := r.open(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	f, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	sink := &outputFile{File: f, r: r}
	defer sink.Abort()
	writer := sstable.NewWriter(sink, opts)
	defer writer.Close()
	stats := &policyReport{Winners: map[string]uint64{}}
	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for k, v := iter.First(); k != nil; k, v = iter.Next() {
		if err = r.ctx.Err(); err != nil {
			return nil, err
		}
		value, _, e := v.Value(nil)
		if e != nil {
			return nil, e
		}
		original := value
		floatGroup := isFloatGroup(k.UserKey, value) && k.Kind() == sstable.InternalKeyKindSet
		if policy != "baseline" && floatGroup {
			points, e := chunkstore.DecodeSealedPoints(original)
			if e != nil {
				return nil, fmt.Errorf("invalid source group: %w", e)
			}
			step := int(binary.LittleEndian.Uint32(original[8:12]))
			group := floatresearch.Group{Points: points, Step: step}
			kind := "sealed"
			used, encErr := measured(func() error {
				var candidateErr error
				kind, value, candidateErr = safeCandidate(policy, group, original)
				return candidateErr
			})
			stats.EncodeCPU += used
			var decoded []chunkstore.Point
			used, decErr := measured(func() error {
				if encErr != nil {
					return nil
				}
				var candidateErr error
				decoded, candidateErr = decodeCandidate(value)
				return candidateErr
			})
			stats.DecodeCPU += used
			stats.Points += uint64(len(points))
			if encErr != nil || decErr != nil || !samePoints(points, decoded) {
				stats.OracleFailures++
				kind, value = "sealed", original
			}
			if len(value) < len(original) {
				stats.ChangedGroups++
			}
			stats.Winners[kind]++
		}
		if floatGroup {
			stats.FloatGroupBytes += uint64(len(value))
		}
		if err = writer.Add(*k, value); err != nil {
			return nil, err
		}
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	dels, err := reader.NewRawRangeDelIter()
	if err != nil {
		return nil, err
	}
	if dels != nil {
		defer dels.Close()
		for span := dels.First(); span != nil; span = dels.Next() {
			for _, key := range span.Keys {
				if err = writer.Add(sstable.InternalKey{UserKey: span.Start, Trailer: key.Trailer}, span.End); err != nil {
					return nil, err
				}
			}
		}
		if err = dels.Error(); err != nil {
			return nil, err
		}
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	st, err := os.Stat(destination)
	if err != nil {
		return nil, err
	}
	stats.Bytes = uint64(st.Size())
	return stats, nil
}

func finishPolicies(policies map[string]*policyReport, baseline uint64, floats float64) {
	for _, p := range policies {
		p.SavedBytes = int64(baseline) - int64(p.Bytes)
		if baseline > 0 {
			p.WholeFileSavingPct = 100 * float64(p.SavedBytes) / float64(baseline)
		}
		if floats > 0 {
			p.FloatBytesSavingPct = 100 * float64(p.SavedBytes) / floats
		}
		if p.Points > 0 {
			p.EncodeNSPerPoint = float64(p.EncodeCPU) / float64(p.Points)
			p.DecodeNSPerPoint = float64(p.DecodeCPU) / float64(p.Points)
		}
	}
	if current := policies["current"]; current != nil {
		for _, p := range policies {
			p.EncodeDeltaNSPerPoint = p.EncodeNSPerPoint - current.EncodeNSPerPoint
			p.DecodeDeltaNSPerPoint = p.DecodeNSPerPoint - current.DecodeNSPerPoint
		}
	}
}
