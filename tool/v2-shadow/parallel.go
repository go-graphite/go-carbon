package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2daemon"
	"github.com/go-graphite/go-carbon/tags"
)

// frameView presents only the current complete frame at a decoder-local offset.
// SegmentReplay retains the unchanged per-partition dictionaries and verifies
// payloads itself. A partition is always assigned to the same serial worker.
type frameView struct {
	header [64]byte
	frame  []byte
	offset int64
}

func (v *frameView) ReadAt(p []byte, off int64) (int, error) {
	var b []byte
	if off < 64 && off >= 0 {
		b = v.header[off:]
	} else if off >= v.offset && off-v.offset < int64(len(v.frame)) {
		b = v.frame[off-v.offset:]
	}
	n := copy(p, b)
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

type frameWork struct {
	header [64]byte
	frame  []byte
	warm   bool
	fence  chan error
}
type partitionWorker struct {
	input             chan frameWork
	decoded, admitted atomic.Uint64
}

func (w *partitionWorker) run(p *v2daemon.Producer, policies map[string][]byte, layer int, failed chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	var view frameView
	var replay *inputlog.SegmentReplay
	defer func() {
		if replay != nil {
			replay.Close()
		}
	}()
	var seq, order uint64
	names := map[string]string{}
	cohortCounts := map[string]uint64{}
	var taggedGroups uint64
	defer func() {
		emit(map[string]any{"stage": "worker_census", "context": contextLoad(), "unique_source_names": len(names), "tagged_groups": taggedGroups, "cohort_points": cohortCounts})
	}()
	sendError := func(e error) {
		select {
		case failed <- e:
		default:
		}
	}
	for work := range w.input {
		if work.fence != nil {
			e := p.Seal()
			if e == nil {
				e = p.Drain()
			}
			work.fence <- e
			continue
		}
		if replay == nil || work.header != view.header {
			if replay != nil {
				replay.Close()
			}
			view = frameView{header: work.header, offset: 64}
			var e error
			replay, e = inputlog.NewSegmentReplay(&view)
			if e != nil {
				sendError(e)
				return
			}
		}
		view.frame = work.frame
		groups, end, _, e := replay.Next(view.offset + int64(len(work.frame)))
		if e != nil {
			sendError(e)
			return
		}
		view.offset = end
		if work.warm {
			continue
		}
		apps := make([]v2daemon.Application, 0, len(groups))
		n := 0
		flush := func() error {
			if len(apps) == 0 {
				return nil
			}
			e := p.AppendApplications(apps)
			apps = apps[:0]
			n = 0
			return e
		}
		for _, g := range groups {
			w.decoded.Add(uint64(len(g.Data)))
			if strings.Contains(g.Metric, ";") {
				taggedGroups++
				if taggedGroups <= 16 {
					emit(map[string]any{"stage": "tagged_discovery", "name": g.Metric})
				}
			}
			name, ok := names[g.Metric]
			if !ok {
				name, e = tags.Normalize(g.Metric)
				if e != nil {
					sendError(e)
					return
				}
				names[g.Metric] = name
			}
			policy := policies[name]
			if policy != nil {
				cohortCounts[name] += uint64(len(g.Data))
			}
			if layer == 3 && policy == nil {
				continue
			}
			for at := 0; at < len(g.Data); {
				count := min(32768-n, len(g.Data)-at)
				seqs := make([]uint64, count)
				for i := range seqs {
					seq++
					seqs[i] = seq
				}
				order++
				apps = append(apps, v2daemon.Application{Name: name, Policy: policy, Order: order, Clock: time.Now().Unix(), Points: g.Data[at : at+count], Sequence: seqs})
				n += count
				at += count
				if n == 32768 {
					if e = flush(); e != nil {
						sendError(e)
						return
					}
				}
			}
		}
		if e = flush(); e != nil {
			sendError(e)
			return
		}
		w.admitted.Store(seq)
	}
	if e := p.Seal(); e != nil {
		sendError(e)
		return
	}
	if e := p.Drain(); e != nil {
		sendError(e)
	}
}

// runParallel bounds queued frames, keeps partition order and never mutates the
// source. The synthetic frame view changes only file offsets, never frame bytes.
func runParallel(source, out, base, policyFile string, duration, interval, lag time.Duration, workers, shards, sample, layer int, capacity bool) (retErr error) {
	if workers < 1 || workers > 16 || shards < 1 || duration <= 0 || interval <= 0 || sample < 1 || lag < 0 || (!capacity && (layer != 3 || policyFile == "")) || (layer != 2 && layer != 3) {
		return errors.New("invalid parallel geometry or missing fixed L3 cohort")
	}
	src, e := filepath.EvalSymlinks(source)
	if e != nil {
		return e
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(out))
	if e != nil {
		return e
	}
	dest := filepath.Join(parent, filepath.Base(out))
	if rel, e := filepath.Rel(src, dest); e != nil || rel == "." || (!bytes.HasPrefix([]byte(rel), []byte("../"))) {
		return errors.New("source/output overlap")
	}
	if rel, e := filepath.Rel(dest, src); e != nil || rel == "." || (!bytes.HasPrefix([]byte(rel), []byte("../"))) {
		return errors.New("source/output overlap")
	}
	policies := map[string][]byte{}
	var candidates []candidate
	if policyFile != "" {
		b, e := os.ReadFile(policyFile)
		if e != nil {
			return e
		}
		var raw map[string]v2core.RollupPolicy
		if e = json.Unmarshal(b, &raw); e != nil {
			return e
		}
		for name, p := range raw {
			if string(p.Name) != name {
				return errors.New("policy identity mismatch")
			}
			b, e := v2core.EncodeRollupPolicy(p)
			if e != nil {
				return e
			}
			policies[name] = b
			candidates = append(candidates, candidate{1, name})
		}
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].Name < candidates[j].Name })
	}
	if e = os.Mkdir(out, 0700); e != nil {
		return e
	}
	store, e := v2daemon.Open(out, v2daemon.Config{Layer: layer, Producers: workers, Shards: shards, MaxPendingRows: 2 << 20, GroupInterval: time.Second})
	if e != nil {
		return e
	}
	failed := make(chan error, workers)
	var wg sync.WaitGroup
	ws := make([]*partitionWorker, workers)
	for i := range ws {
		ws[i] = &partitionWorker{input: make(chan frameWork, 2)}
		wg.Add(1)
		go ws[i].run(store.Producers[i], policies, layer, failed, &wg)
	}
	defer func() {
		for _, w := range ws {
			close(w.input)
		}
		wg.Wait()
		select {
		case e := <-failed:
			if retErr == nil {
				retErr = e
			}
		default:
		}
		if e := store.Close(); retErr == nil {
			retErr = e
		}
		emit(map[string]any{"stage": "closed_parallel", "context": contextLoad(), "error": fmt.Sprint(retErr)})
	}()
	paths, e := filepath.Glob(filepath.Join(source, "*.log"))
	if e != nil || len(paths) == 0 {
		return errors.New("no source journals")
	}
	path := paths[len(paths)-1]
	var f *os.File
	var header [64]byte
	var offset int64
	open := func() error {
		if f != nil {
			_ = f.Close()
		}
		// A published successor path may still contain its recycled predecessor
		// header. Reuse the existing filename/sequence/header readiness gate.
		t, err := readyShadowTail(path)
		if err != nil {
			return err
		}
		t.replay.Close()
		f, header, offset = t.file, t.header, 64
		return nil
	}
	if e = open(); e != nil {
		return e
	}
	defer func() { _ = f.Close() }()
	table := crc32.MakeTable(crc32.Castagnoli)
	fence := func() error {
		for _, w := range ws {
			ack := make(chan error, 1)
			select {
			case w.input <- frameWork{fence: ack}:
			case e := <-failed:
				return e
			}
			select {
			case e := <-ack:
				if e != nil {
					return e
				}
			case e := <-failed:
				return e
			}
		}
		return nil
	}
	var start, deadline, lastEOF time.Time
	warm := true
	warmStart := time.Now()
	nextSample := time.Now()
	nextSeal := time.Now()
	var offered, sourceBytes uint64
	rotation := 0
	totals := &totals{}
	jobs := make(chan compareJob, 1)
	var compareWG sync.WaitGroup
	compareWG.Add(1)
	go func() {
		defer compareWG.Done()
		for j := range jobs {
			compare(j, base, totals)
		}
	}()
	defer func() {
		close(jobs)
		compareWG.Wait()
		emit(map[string]any{"stage": "comparison_totals", "context": contextLoad(), "totals": totals})
		if !capacity && (totals.Points == 0 || totals.Mismatches > 0 || totals.Failures > 0) && retErr == nil {
			retErr = errors.New("parity incomplete or differing")
		}
	}()
	for warm || time.Now().Before(deadline) {
		select {
		case e := <-failed:
			return e
		default:
		}
		now := time.Now()
		if warm && now.Sub(warmStart) > time.Minute {
			return errors.New("parallel warmup exceeded one minute")
		}
		if !warm && !now.Before(nextSeal) {
			if e = fence(); e != nil {
				return e
			}
			nextSeal = now.Add(time.Second)
		}
		if !warm && !now.Before(nextSample) {
			var decoded, admitted uint64
			for _, w := range ws {
				decoded += w.decoded.Load()
				admitted += w.admitted.Load()
			}
			emit(map[string]any{"stage": "parallel_sample", "context": contextLoad(), "seconds": now.Sub(start).Seconds(), "offered_points": offered, "decoded_points": decoded, "admitted_points": admitted, "admission_pending_points": offered - admitted, "source_path": path, "source_offset": offset, "source_bytes": sourceBytes, "source_lag_seconds": now.Sub(lastEOF).Seconds(), "source_backlog": sourceBacklog(source, path, offset)})
			until := lastEOF.Add(-lag).Unix()
			if !capacity && until > start.Unix()+60 && len(jobs) == 0 {
				if e = fence(); e != nil {
					return e
				}
				for i, w := range ws {
					if n := w.admitted.Load(); n > 0 {
						if e = store.Core.Append(i, nil, n, true); e != nil {
							return e
						}
					}
				}
				j := compareJob{Layer: 3, From: start.Unix() + 60, Until: until, FirstSnapshot: now, SourcePath: path, SourceOffset: offset, SourceLagSeconds: now.Sub(lastEOF).Seconds()}
				for i := 0; i < sample && i < len(candidates); i++ {
					j.Names = append(j.Names, candidates[rotation%len(candidates)])
					rotation++
				}
				for _, db := range store.Core.DBs() {
					j.Snapshots = append(j.Snapshots, db.NewSnapshot())
				}
				jobs <- j
			}
			nextSample = now.Add(interval)
		}
		var current [64]byte
		if _, e = f.ReadAt(current[:], 0); e != nil {
			return e
		}
		if current != header {
			return errors.New("source recycled")
		}
		if string(header[:8]) != "GCINLOG1" || crc32.Checksum(header[:32], table) != binary.LittleEndian.Uint32(header[32:]) {
			return errors.New("invalid segment header")
		}
		var h [52]byte
		_, e = f.ReadAt(h[:], offset)
		eof := errors.Is(e, io.EOF) || e == nil && (binary.LittleEndian.Uint32(h[:]) != 0x4c494347 || !bytes.Equal(h[8:16], header[16:24]))
		if eof {
			seq := binary.LittleEndian.Uint64(header[8:])
			next := filepath.Join(source, fmt.Sprintf("%016x.log", seq+1))
			if _, e = os.Stat(next); e == nil {
				path = next
				if e = open(); e != nil {
					return e
				}
				continue
			} else if !errors.Is(e, os.ErrNotExist) {
				return e
			}
			all, _ := filepath.Glob(filepath.Join(source, "*.log"))
			for _, p := range all {
				if p > next {
					return errors.New("source gap")
				}
			}
			lastEOF = time.Now()
			if warm {
				warm = false
				start = lastEOF
				deadline = start.Add(duration)
				nextSample = start.Add(interval)
				nextSeal = start.Add(time.Second)
				emit(map[string]any{"stage": "start_parallel", "context": contextLoad(), "source_path": path, "source_offset": offset, "workers": workers, "layer": layer, "cohort_names": len(candidates), "duration_seconds": duration.Seconds(), "warmup_seconds": time.Since(warmStart).Seconds()})
			}
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if e != nil {
			return e
		}
		if crc32.Checksum(h[:48], table) != binary.LittleEndian.Uint32(h[48:]) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		n := binary.LittleEndian.Uint32(h[16:])
		partition := int(binary.LittleEndian.Uint16(h[6:]))
		if n == 0 || n > 64<<20 || partition >= 16 {
			return errors.New("invalid frame geometry")
		}
		frame := make([]byte, 52+int(n))
		copy(frame, h[:])
		if _, e = f.ReadAt(frame[52:], offset+52); e != nil || crc32.Checksum(frame[52:], table) != binary.LittleEndian.Uint32(h[44:]) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		select {
		case ws[partition%workers].input <- frameWork{header: header, frame: frame, warm: warm}:
		case e := <-failed:
			return e
		}
		offset += int64(len(frame))
		if !warm {
			offered += uint64(binary.LittleEndian.Uint32(h[24:]))
			sourceBytes += uint64(len(frame))
		}
	}
	if e = fence(); e != nil {
		return e
	}
	var admitted uint64
	for _, w := range ws {
		admitted += w.admitted.Load()
	}
	emit(map[string]any{"stage": "intake_complete", "context": contextLoad(), "points": admitted, "offered_points": offered, "seconds": time.Since(start).Seconds(), "source_path": path, "source_offset": offset})
	return nil
}

func reobserveClassic(path, base string) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	var cases []struct {
		difference
		Step int64 `json:"step"`
	}
	if e = json.Unmarshal(b, &cases); e != nil {
		return e
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, d := range cases {
		step := d.Step
		if step == 0 {
			step = 60
		}
		encoded, e := fetchPolicy(client, base, d.Name)
		if e != nil {
			return e
		}
		var policy v2core.RollupPolicy
		if e = json.Unmarshal(encoded, &policy); e != nil {
			return e
		}
		archive := -1
		for i, a := range policy.Config.Archives {
			if a.Step == step {
				archive = i
				break
			}
		}
		if archive < 0 {
			return fmt.Errorf("archive step %d not present", step)
		}
		from := d.Time - 1
		if archive > 0 {
			prev := policy.Config.Archives[archive-1]
			from = time.Now().Unix() - prev.Step*prev.Count - 2*step
		}
		r, e := fetch(client, base, d.Name, from, d.Time+step-1)
		if e != nil {
			return e
		}
		if int64(r.StepTime) != step || len(r.IsAbsent) != len(r.Values) {
			return fmt.Errorf("invalid receipt response geometry: desired step%d returned%d", step, r.StepTime)
		}
		i := (d.Time - int64(r.StartTime)) / step
		if i < 0 || i >= int64(len(r.Values)) || int64(r.StartTime)+i*step != d.Time {
			return errors.New("receipt outside response")
		}
		emit(map[string]any{"stage": "classic_receipt", "context": contextLoad(), "initial": d.difference, "step": step, "classic_present": !r.IsAbsent[i], "classic_bits": fmt.Sprintf("%016x", math.Float64bits(r.Values[i]))})
	}
	return nil
}
