package ringplan

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type summary struct {
	Metrics        uint64      `json:"metrics"`
	MovedMetrics   uint64      `json:"moved_metrics"`
	Copies         uint64      `json:"copies"`
	EstimatedBytes uint64      `json:"estimated_bytes"`
	Unowned        uint64      `json:"unowned"`
	MovedFraction  float64     `json:"moved_fraction"`
	Pairs          []pairCount `json:"pairs,omitempty"`
}

type pairCount struct {
	Source         string `json:"source"`
	Destination    string `json:"destination"`
	Metrics        uint64 `json:"metrics"`
	EstimatedBytes uint64 `json:"estimated_bytes"`
}

type stream struct {
	Path           string `json:"path"`
	Source         string `json:"source"`
	Destination    string `json:"destination"`
	Metrics        uint64 `json:"metrics"`
	EstimatedBytes uint64 `json:"estimated_bytes"`
	SHA256         string `json:"sha256"`
}

type inputState struct {
	PlanID   string   `json:"plan_id"`
	Offset   int64    `json:"offset"`
	Previous string   `json:"previous"`
	Segment  int      `json:"segment"`
	Complete bool     `json:"complete"`
	Size     int64    `json:"size"`
	MTime    int64    `json:"mtime"`
	Summary  summary  `json:"summary"`
	Streams  []stream `json:"streams"`
}

type manifest struct {
	Whole   []WholeCopy   `json:"whole,omitempty"`
	Format  string        `json:"format"`
	Version int           `json:"version"`
	PlanID  string        `json:"plan_id"`
	Spec    specification `json:"spec"`
	Summary summary       `json:"summary"`
	Streams []stream      `json:"streams"`
}

type options struct {
	allowEmpty       bool
	transform        func(*manifest) error
	Out              string
	Apply            bool
	Workers          int
	PerSourceWorkers int
	SegmentMetrics   int
	HTTPTimeout      time.Duration
	Rate             float64
}

type streamWriter struct {
	file   *os.File
	writer *bufio.Writer
	hash   hash.Hash
	stream stream
	line   []byte
}

func newStreamWriter(out, id string, inputIndex, segment int, source, destination string, destinationIndex int) (*streamWriter, error) {
	path := fmt.Sprintf("streams/%04d-%06d-%04d.jsonl", inputIndex, segment, destinationIndex)
	f, err := os.OpenFile(filepath.Join(out, path)+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	w := &streamWriter{file: f, hash: h, stream: stream{Path: path, Source: source, Destination: destination}}
	w.writer = bufio.NewWriterSize(io.MultiWriter(f, h), 256<<10)
	header := struct {
		Type        string `json:"type"`
		Format      string `json:"format"`
		Version     int    `json:"version"`
		PlanID      string `json:"plan_id"`
		Source      string `json:"source"`
		Destination string `json:"destination"`
	}{"header", formatName, version, id, source, destination}
	if err := json.NewEncoder(w.writer).Encode(header); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

func (w *streamWriter) add(name string, size int64) error {
	// validMetric excludes Go-only control escapes; AppendQuote is JSON here.
	w.line = append(w.line[:0], `{"type":"move","metric":`...)
	w.line = strconv.AppendQuote(w.line, name)
	w.line = append(w.line, `,"estimated_bytes":`...)
	w.line = strconv.AppendInt(w.line, size, 10)
	w.line = append(w.line, '}', '\n')
	if _, err := w.writer.Write(w.line); err != nil {
		return err
	}
	w.stream.Metrics++
	w.stream.EstimatedBytes += uint64(size)
	return nil
}

func (w *streamWriter) finish(out string) (stream, error) {
	end := struct {
		Type           string `json:"type"`
		Metrics        uint64 `json:"metrics"`
		EstimatedBytes uint64 `json:"estimated_bytes"`
	}{"end", w.stream.Metrics, w.stream.EstimatedBytes}
	err := json.NewEncoder(w.writer).Encode(end)
	if err == nil {
		err = w.writer.Flush()
	}
	if err == nil {
		err = w.file.Sync()
	}
	closeErr := w.file.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(filepath.Join(out, w.stream.Path)+".tmp", filepath.Join(out, w.stream.Path))
	}
	w.stream.SHA256 = hex.EncodeToString(w.hash.Sum(nil))
	return w.stream, err
}

func atomicJSON(path string, value any) error {
	f, err := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	err = json.NewEncoder(f).Encode(value)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(path+".tmp", path)
	}
	if err == nil {
		err = syncDir(filepath.Dir(path))
	}
	return err
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func checkStreams(out string, streams []stream) error {
	for _, s := range streams {
		if filepath.IsAbs(s.Path) || filepath.Clean(s.Path) != s.Path || filepath.Dir(s.Path) != "streams" {
			return errors.New("invalid stream path in checkpoint")
		}
		f, err := os.Open(filepath.Join(out, s.Path))
		if err != nil {
			return err
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return err
		}
		if hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
			return fmt.Errorf("checkpoint stream checksum mismatch: %s", s.Path)
		}
	}
	return nil
}

func plan(ctx context.Context, s specification, old, next *ring, id string, opt options) (manifest, error) {
	m := manifest{Format: formatName, Version: version, PlanID: id, Spec: s, Streams: make([]stream, 0)}
	if opt.Workers < 1 || opt.Workers > 64 || opt.PerSourceWorkers < 1 || opt.PerSourceWorkers > 16 || opt.SegmentMetrics < 1 || opt.Rate < 0 || math.IsNaN(opt.Rate) || math.IsInf(opt.Rate, 0) {
		return m, errors.New("workers=1..64, per-source-workers=1..16, segment-metrics>0 and finite rate>=0 required")
	}
	if len(s.Inputs) == 0 && !opt.allowEmpty {
		return m, errors.New("planning requires at least one inventory")
	}
	if !s.PartialInventory {
		covered := make(map[string]bool)
		for _, in := range s.Inputs {
			covered[in.Source] = true
		}
		for _, n := range s.Old.Nodes {
			if !covered[n.ID] {
				return m, fmt.Errorf("missing inventory for %s (partial_inventory must explicitly permit partial plans)", n.ID)
			}
		}
	}
	var lock *os.File
	if opt.Apply {
		if opt.Out == "" {
			return m, errors.New("--out is required with --apply")
		}
		if err := os.MkdirAll(opt.Out, 0700); err != nil {
			return m, err
		}
		var err error
		lock, err = os.OpenFile(filepath.Join(opt.Out, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return m, err
		}
		defer lock.Close()
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return m, errors.New("plan directory is in use")
		}
		defer unix.Flock(int(lock.Fd()), unix.LOCK_UN) //nolint:errcheck
		if err := os.MkdirAll(filepath.Join(opt.Out, "streams"), 0700); err != nil {
			return m, err
		}
		marker := filepath.Join(opt.Out, "state.json")
		if b, err := os.ReadFile(marker); err == nil {
			var prev struct {
				PlanID string `json:"plan_id"`
			}
			if err := json.Unmarshal(b, &prev); err != nil {
				return m, err
			}
			if prev.PlanID != id {
				return m, errors.New("output directory belongs to a different specification")
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return m, err
		} else {
			if err := atomicJSON(marker, struct {
				PlanID string `json:"plan_id"`
			}{id}); err != nil {
				return m, err
			}
		}
		// Never leave a previous commit marker visible while revalidating input.
		if err := os.Remove(filepath.Join(opt.Out, "manifest.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return m, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	states := make([]inputState, len(s.Inputs))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	sourceSlots := make(map[string]chan struct{})
	for _, in := range s.Inputs {
		if sourceSlots[in.Source] == nil {
			sourceSlots[in.Source] = make(chan struct{}, opt.PerSourceWorkers)
		}
	}
	for i := 0; i < opt.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				slots := sourceSlots[s.Inputs[index].Source]
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return
				}
				state, err := planInput(ctx, s, old, next, id, index, opt)
				<-slots
				if err != nil {
					errOnce.Do(func() { firstErr = fmt.Errorf("inventory %d (%s): %w", index, s.Inputs[index].Source, err); cancel() })
					continue
				}
				states[index] = state
			}
		}()
	}
	for i := range s.Inputs {
		select {
		case jobs <- i:
		case <-ctx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return m, firstErr
	}
	if err := ctx.Err(); err != nil {
		return m, err
	}
	for _, state := range states {
		addSummary(&m.Summary, state.Summary)
		m.Streams = append(m.Streams, state.Streams...)
	}
	if m.Summary.Metrics != 0 {
		m.Summary.MovedFraction = float64(m.Summary.MovedMetrics) / float64(m.Summary.Metrics)
	}
	if opt.transform != nil {
		if err := opt.transform(&m); err != nil {
			return m, err
		}
	}
	if opt.Apply {
		if err := atomicJSON(filepath.Join(opt.Out, "manifest.json"), m); err != nil {
			return m, err
		}
	}
	return m, nil
}

func addSummary(dst *summary, src summary) {
	dst.Metrics += src.Metrics
	dst.MovedMetrics += src.MovedMetrics
	dst.Copies += src.Copies
	dst.EstimatedBytes += src.EstimatedBytes
	dst.Unowned += src.Unowned
	for _, p := range src.Pairs {
		addPair(dst, p)
	}
}

func addPair(s *summary, p pairCount) {
	for i := range s.Pairs {
		if s.Pairs[i].Source == p.Source && s.Pairs[i].Destination == p.Destination {
			s.Pairs[i].Metrics += p.Metrics
			s.Pairs[i].EstimatedBytes += p.EstimatedBytes
			return
		}
	}
	s.Pairs = append(s.Pairs, p)
	sort.Slice(s.Pairs, func(i, j int) bool {
		if s.Pairs[i].Source == s.Pairs[j].Source {
			return s.Pairs[i].Destination < s.Pairs[j].Destination
		}
		return s.Pairs[i].Source < s.Pairs[j].Source
	})
}

func planInput(ctx context.Context, s specification, old, next *ring, id string, index int, opt options) (inputState, error) {
	in := s.Inputs[index]
	state := inputState{PlanID: id}
	statePath := filepath.Join(opt.Out, fmt.Sprintf("input-%04d.state.json", index))
	if opt.Apply {
		if b, err := os.ReadFile(statePath); err == nil {
			if err := json.Unmarshal(b, &state); err != nil {
				return state, err
			}
			if state.PlanID != id {
				return state, errors.New("checkpoint plan mismatch")
			}
			if err := checkStreams(opt.Out, state.Streams); err != nil {
				return state, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return state, err
		}
	}
	if state.Complete && (in.Format == "buckyd" || in.Format == "transfer") {
		return state, nil
	}
	inv, err := openInventory(ctx, in, state.Offset, state.Previous, opt.HTTPTimeout)
	if err != nil {
		return state, err
	}
	defer inv.close()
	if inv.file != nil {
		if state.Segment > 0 || state.Size != 0 || state.MTime != 0 {
			if state.Size != inv.info.Size() || state.MTime != inv.info.ModTime().UnixNano() {
				return state, errors.New("inventory fingerprint changed since checkpoint")
			}
		}
		state.Size, state.MTime = inv.info.Size(), inv.info.ModTime().UnixNano()
	}
	if state.Complete {
		return state, nil
	}
	oldOwners := make([]int, 0, s.Old.Replicas)
	newOwners := make([]int, 0, s.New.Replicas)
	writers := make([]*streamWriter, len(next.members))
	defer func() {
		for _, w := range writers {
			if w != nil {
				w.file.Close()
			}
		}
	}()
	counts := make([]pairCount, len(next.members))
	for i, m := range next.members {
		counts[i] = pairCount{Source: in.Source, Destination: m.ID}
	}
	segmentCount := 0
	startCount := state.Summary.Metrics
	started := time.Now()
	commit := func(done bool) error {
		if err := inv.unchanged(); err != nil {
			return err
		}
		for i, w := range writers {
			if w == nil {
				continue
			}
			stream, err := w.finish(opt.Out)
			writers[i] = nil
			if err != nil {
				return err
			}
			state.Streams = append(state.Streams, stream)
		}
		for i, p := range counts {
			if p.Metrics > 0 {
				addPair(&state.Summary, p)
				counts[i].Metrics = 0
				counts[i].EstimatedBytes = 0
			}
		}
		state.Offset, state.Previous = inv.offset, inv.previous
		state.Segment++
		segmentCount = 0
		state.Complete = done
		if opt.Apply {
			if err := syncDir(filepath.Join(opt.Out, "streams")); err != nil {
				return err
			}
			if inv.file != nil || done {
				return atomicJSON(statePath, state)
			}
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return state, err
		}
		name, size, err := inv.next()
		if errors.Is(err, io.EOF) {
			err = commit(true)
			return state, err
		}
		if err != nil {
			return state, err
		}
		key := old.compiled.KeyHash(name)
		oldOwners = old.compiled.LookupHash(key, oldOwners[:0])
		if s.Old.Hash != s.New.Hash {
			key = next.compiled.KeyHash(name)
		}
		newOwners = next.compiled.LookupHash(key, newOwners[:0])
		owned := false
		for _, owner := range oldOwners {
			if old.members[owner].ID == in.Source {
				owned = true
				break
			}
		}
		if !owned {
			state.Summary.Unowned++
			if !s.AllowUnowned {
				return state, fmt.Errorf("metric %q is not owned by source %s under old ring", name, in.Source)
			}
		}
		moved := false
		for _, dst := range newOwners {
			dstID := next.members[dst].ID
			if dstID == in.Source {
				continue
			}
			present := false
			if owned {
				for _, owner := range oldOwners {
					if old.members[owner].ID == dstID {
						present = true
						break
					}
				}
			}
			if present {
				continue
			}
			moved = true
			if math.MaxUint64-state.Summary.EstimatedBytes < uint64(size) {
				return state, errors.New("byte estimate overflow")
			}
			state.Summary.Copies++
			state.Summary.EstimatedBytes += uint64(size)
			counts[dst].Metrics++
			counts[dst].EstimatedBytes += uint64(size)
			if opt.Apply {
				w := writers[dst]
				if w == nil {
					w, err = newStreamWriter(opt.Out, id, index, state.Segment, in.Source, dstID, dst)
					if err != nil {
						return state, err
					}
					writers[dst] = w
				}
				if err := w.add(name, size); err != nil {
					return state, err
				}
			}
		}
		state.Summary.Metrics++
		if moved {
			state.Summary.MovedMetrics++
		}
		segmentCount++
		if segmentCount >= opt.SegmentMetrics {
			if err := commit(false); err != nil {
				return state, err
			}
		}
		if opt.Rate > 0 && state.Summary.Metrics%1024 == 0 {
			wait := time.Duration(float64(state.Summary.Metrics-startCount)/(opt.Rate/float64(opt.PerSourceWorkers))*float64(time.Second)) - time.Since(started)
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return state, ctx.Err()
				}
			}
		}
	}
}
