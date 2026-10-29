package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/cache"
)

type ingressPoint struct {
	at, timestamp int64
	metric        int
	bits          uint64
}

// Keep original timestamps, values and arrival intervals. Only select the
// predeclared corpus: a metric with no accepted ingress remains in the plan.
// Stable arrival ordering preserves line order for identical packet times.
func selectedIngress(path string, plan replayPlan) ([]ingressPoint, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var header [36]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	names, count := binary.LittleEndian.Uint32(header[8:12]), binary.LittleEndian.Uint64(header[12:20])
	first, last := int64(binary.LittleEndian.Uint64(header[20:28])), int64(binary.LittleEndian.Uint64(header[28:36]))
	if string(header[:8]) != "GTREPL01" || names == 0 || names > 20000000 || count == 0 || count > 100000000 || first < plan.StartNS || last < first || last > plan.EndNS || last-plan.StartNS > int64(10*time.Minute) || plan.EndNS-last > int64(time.Minute) {
		return nil, errors.New("ingress header is outside the bounded replay plan")
	}
	indices := make(map[string]int, len(plan.Rows))
	for index, row := range plan.Rows {
		indices[row.Metric] = index
	}
	remap := make([]int32, names)
	var name [16384]byte
	for index := range names {
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		if length == 0 || length > uint32(len(name)) {
			return nil, errors.New("invalid ingress metric length")
		}
		if _, err := io.ReadFull(r, name[:length]); err != nil {
			return nil, err
		}
		remap[index] = -1
		if selected, ok := indices[string(name[:length])]; ok {
			remap[index] = int32(selected)
		}
	}
	var points []ingressPoint
	var raw [28]byte
	for range count {
		if _, err := io.ReadFull(r, raw[:]); err != nil {
			return nil, err
		}
		at := int64(binary.LittleEndian.Uint64(raw[:8]))
		if at < first {
			if at < 0 || at > last-first {
				return nil, errors.New("invalid relative ingress arrival")
			}
			at += first
		}
		index := binary.LittleEndian.Uint32(raw[8:12])
		if index >= names || at < first || at > last {
			return nil, errors.New("invalid ingress record")
		}
		if remap[index] < 0 {
			continue
		}
		timestamp := int64(binary.LittleEndian.Uint64(raw[12:20]))
		if timestamp <= 0 || timestamp > math.MaxUint32 || len(points) >= 5000000 {
			return nil, errors.New("invalid timestamp or selected ingress exceeds bound")
		}
		points = append(points, ingressPoint{at: at, metric: int(remap[index]), timestamp: timestamp, bits: binary.LittleEndian.Uint64(raw[20:28])})
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing ingress data")
	}
	if len(points) == 0 {
		return nil, errors.New("no accepted ingress for the declared corpus")
	}
	sort.SliceStable(points, func(i, j int) bool { return points[i].at < points[j].at })
	return points, nil
}

// This creates a labelled workload, not a recording of production flushes.
// The real cache decides batches under the production noop / 8-point / 2-second
// policy. Its consumer only records and confirms batches; storage costs do not
// influence that preparation. Both engines subsequently receive identical work.
func prepareIngress(planPath, ingressPath, operationsPath string) (err error) {
	plan, err := loadPlan(planPath)
	if err != nil {
		return err
	}
	input, err := selectedIngress(ingressPath, plan)
	if err != nil {
		return err
	}
	planHash, err := fileSHA(planPath)
	if err != nil {
		return err
	}
	traceHash, err := fileSHA(ingressPath)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(operationsPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	c := cache.New()
	c.SetMaxSize(2000000)
	if err := c.SetWriteStrategy("noop"); err != nil {
		return err
	}
	if err := c.SetWriteoutBatching(8, 2*time.Second); err != nil {
		return err
	}
	indices := make(map[string]int, len(plan.Rows))
	for i, row := range plan.Rows {
		indices[row.Metric] = i
	}
	writer := bufio.NewWriterSize(f, 1<<20)
	encoder := json.NewEncoder(writer)
	abort, done := make(chan bool), make(chan error, 1)
	stop := sync.OnceFunc(func() { close(abort) })
	started := time.Now()
	var batches, points, reads uint64
	go func() {
		var nextRead uint64
		for {
			name := c.WriteoutQueue().Get(abort)
			if name == "" {
				done <- nil
				return
			}
			p, ok := c.PopNotConfirmed(name)
			if !ok {
				continue
			}
			op := replayOperation{AtNS: plan.StartNS + int64(time.Since(started)), Kind: "write", Metric: indices[name], Points: make([]replayPoint, len(p.Data))}
			if op.AtNS > plan.EndNS {
				done <- errors.New("prepared writeout exceeded the planned interval")
				return
			}
			for i, point := range p.Data {
				op.Points[i] = replayPoint{Timestamp: point.Timestamp, Bits: math.Float64bits(point.Value)}
			}
			if err := encoder.Encode(op); err != nil {
				done <- err
				return
			}
			batches++
			points += uint64(len(p.Data))
			// One controlled disk read per 10,000 ingress points, near the
			// observed production disk-read/input ratio; no HTTP provenance.
			if points >= nextRead {
				widths := [...]int{120, 3600, 172800}
				until := int(op.AtNS/int64(time.Second)) - 1
				if err := encoder.Encode(replayOperation{AtNS: op.AtNS, Kind: "read", Metric: op.Metric, From: until - widths[reads%3], Until: until}); err != nil {
					done <- err
					return
				}
				reads++
				nextRead = points + 10000
			}
			c.Confirm(p)
		}
	}()
	var workerErr error
	workerFinished := false
	defer func() {
		stop()
		if !workerFinished {
			workerErr = <-done
		}
		err = errors.Join(err, workerErr)
	}()
	for _, point := range input {
		if delay := time.Duration(point.at-plan.StartNS) - time.Since(started); delay > 0 {
			time.Sleep(delay)
		}
		select {
		case workerErr = <-done:
			workerFinished = true
			return errors.Join(errors.New("writeout recorder stopped"), workerErr)
		default:
		}
		c.AddPoint([]byte(plan.Rows[point.metric].Metric), math.Float64frombits(point.bits), point.timestamp)
	}
	for c.Size() != 0 {
		if plan.StartNS+int64(time.Since(started)) > plan.EndNS {
			return errors.New("writeout recorder did not drain")
		}
		select {
		case workerErr = <-done:
			workerFinished = true
			return errors.Join(errors.New("writeout recorder stopped"), workerErr)
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Get may be waiting for the next cache rebuild. Join it before reading
	// counters or flushing the sole writer's buffered output.
	stop()
	workerErr = <-done
	workerFinished = true
	if workerErr != nil {
		return workerErr
	}
	if points != uint64(len(input)) || reads == 0 || batches == 0 {
		return fmt.Errorf("writeout accounting: input=%d output=%d batches=%d reads=%d", len(input), points, batches, reads)
	}
	if err := errors.Join(writer.Flush(), f.Sync()); err != nil {
		return err
	}
	operationsHash, err := fileSHA(operationsPath)
	if err != nil {
		return err
	}
	return writeJSON(operationsPath+".preparation.json", map[string]any{"plan_sha256": planHash, "ingress_sha256": traceHash, "operations_sha256": operationsHash,
		"ingress_points": len(input), "write_batches": batches, "reads": reads, "writeout_min_points": 8, "writeout_max_delay_seconds": 2, "write_strategy": "noop", "workers": 1,
		"scope": "Original production timestamps/float bits/arrival intervals; batches prepared by the real cache with a recording-only consumer. Controlled 120s/1h/2d reads, approximately one per 10000 input points. These are not captured production persister batches or HTTP requests; no production latency or capacity qualification."})
}
