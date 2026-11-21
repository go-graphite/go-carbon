package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2core"
	"github.com/go-graphite/go-carbon/internal/v2part"
	pb3 "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"google.golang.org/protobuf/encoding/protojson"
)

// The journal has no frame append clock. An independent read-only observer
// bounds completion between its last unsuccessful read and first CRC-valid read.
// These are conservative age bounds, never event timestamps or EOF freshness.
type appendStamp struct{ Lower, Upper time.Time }
type appendKey struct {
	Sequence uint64
	Offset   int64
}
type appendClock struct {
	mu      sync.Mutex
	stamps  map[appendKey]appendStamp
	err     error
	stop    chan struct{}
	done    chan struct{}
	frames  uint64
	points  uint64
	lastEOF time.Time
}

func startAppendClock(source string) (*appendClock, error) {
	paths, e := filepath.Glob(filepath.Join(source, "*.log"))
	if e != nil || len(paths) == 0 {
		return nil, errors.New("append observer has no source")
	}
	c := &appendClock{stamps: map[appendKey]appendStamp{}, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(c.done)
		if err := c.run(source, paths[len(paths)-1]); err != nil {
			c.mu.Lock()
			c.err = err
			c.mu.Unlock()
		}
	}()
	deadline := time.Now().Add(30 * time.Second)
	for {
		c.mu.Lock()
		ready := !c.lastEOF.IsZero()
		err := c.err
		c.mu.Unlock()
		if err != nil {
			c.close()
			return nil, err
		}
		if ready {
			return c, nil
		}
		if time.Now().After(deadline) {
			c.close()
			return nil, errors.New("append observer startup exceeded 30s")
		}
		time.Sleep(time.Millisecond)
	}
}
func (c *appendClock) close() { close(c.stop); <-c.done }
func (c *appendClock) report() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]any{"frames": c.frames, "observed_points": c.points, "pending_stamps": len(c.stamps), "error": fmt.Sprint(c.err), "last_eof_unix_nano": c.lastEOF.UnixNano()}
}
func (c *appendClock) take(seq uint64, off int64) (appendStamp, error) {
	end := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		s, ok := c.stamps[appendKey{seq, off}]
		if ok {
			delete(c.stamps, appendKey{seq, off})
		}
		err := c.err
		c.mu.Unlock()
		if err != nil {
			return s, err
		}
		if ok {
			if s.Lower.IsZero() {
				return s, errors.New("frame predates append observation")
			}
			return s, nil
		}
		if time.Now().After(end) {
			return s, errors.New("append observer failed to reach frame")
		}
		time.Sleep(time.Millisecond)
	}
}
func (c *appendClock) run(source, path string) error {
	var lower time.Time
	table := crc32.MakeTable(crc32.Castagnoli)
	for {
		t, e := readyShadowTail(path)
		if e != nil {
			return e
		}
		t.replay.Close()
		seq := binary.LittleEndian.Uint64(t.header[8:])
		offset := int64(64)
		next := false
		for !next {
			select {
			case <-c.stop:
				t.file.Close()
				return nil
			default:
			}
			at := time.Now()
			var h [52]byte
			_, e = t.file.ReadAt(h[:], offset)
			valid := e == nil && binary.LittleEndian.Uint32(h[:]) == 0x4c494347 && bytes.Equal(h[8:16], t.header[16:24]) && crc32.Checksum(h[:48], table) == binary.LittleEndian.Uint32(h[48:])
			if valid {
				n := binary.LittleEndian.Uint32(h[16:])
				if n == 0 || n > 64<<20 {
					t.file.Close()
					return errors.New("append observer frame geometry")
				}
				b := make([]byte, n)
				_, e = t.file.ReadAt(b, offset+52)
				valid = e == nil && crc32.Checksum(b, table) == binary.LittleEndian.Uint32(h[44:])
				if valid {
					var current [64]byte
					if _, e = t.file.ReadAt(current[:], 0); e != nil || current != t.header {
						t.file.Close()
						return errors.New("append observer source recycled")
					}
					c.mu.Lock()
					if !lower.IsZero() {
						c.stamps[appendKey{seq, offset}] = appendStamp{lower, time.Now()}
					}
					c.frames++
					c.points += uint64(binary.LittleEndian.Uint32(h[24:]))
					tooMany := len(c.stamps) > 100000
					c.mu.Unlock()
					if tooMany {
						t.file.Close()
						return errors.New("append observer stamp bound exceeded")
					}
					offset += 52 + int64(n)
					continue
				}
			}
			if e != nil && !errors.Is(e, io.EOF) {
				t.file.Close()
				return e
			}
			// No valid complete frame existed at the beginning of this read attempt.
			successor := filepath.Join(source, fmt.Sprintf("%016x.log", seq+1))
			if _, e = os.Stat(successor); e == nil {
				path = successor
				next = true
				continue
			} else if !errors.Is(e, os.ErrNotExist) {
				t.file.Close()
				return e
			}
			lower = at
			c.mu.Lock()
			c.lastEOF = at
			c.mu.Unlock()
			select {
			case <-c.stop:
				t.file.Close()
				return nil
			case <-time.After(10 * time.Millisecond):
			}
		}
		t.file.Close()
	}
}

type visibilityFrame struct {
	Stamp  appendStamp
	Points uint64
}
type visibilityHistogram struct {
	mu                sync.Mutex
	Lower, Upper      [12002]uint64
	Points, Unbounded uint64
}

func (h *visibilityHistogram) record(frames []visibilityFrame, visible time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range frames {
		h.Points += f.Points
		if f.Stamp.Lower.IsZero() {
			h.Unbounded += f.Points
			continue
		}
		lo := max(0, visible.Sub(f.Stamp.Upper).Milliseconds()/10)
		hi := max(0, int64((visible.Sub(f.Stamp.Lower)+10*time.Millisecond-1)/(10*time.Millisecond)))
		h.Lower[min(lo, 12001)] += f.Points
		h.Upper[min(hi, 12001)] += f.Points
	}
}
func (h *visibilityHistogram) report() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	quant := func(a [12002]uint64, p uint64) float64 {
		target := ((h.Points-h.Unbounded)*p + 99) / 100
		var n uint64
		for i, v := range a {
			n += v
			if n >= max(1, target) {
				return float64(i) / 100
			}
		}
		return -1
	}
	return map[string]any{"points": h.Points, "unbounded_points": h.Unbounded, "p50_lower_seconds": quant(h.Lower, 50), "p50_upper_seconds": quant(h.Upper, 50), "p99_lower_seconds": quant(h.Lower, 99), "p99_upper_seconds": quant(h.Upper, 99), "overflow_above_seconds": 120, "definition": "point-weighted append-completion to synchronous core publication fence; conservative 10ms buckets"}
}

// Exact per-name stored policies. No prefix inference or fallback policy is used.
func fetchPolicies(client *http.Client, base string, names []string) (map[string][]byte, error) {
	result := map[string][]byte{}
	for at := 0; at < len(names); at += 512 {
		end := min(at+512, len(names))
		q := url.Values{"target": names[at:end], "format": {"json"}}
		r, e := client.Post(base+"/info/", "application/x-www-form-urlencoded", strings.NewReader(q.Encode()))
		if e != nil {
			return nil, e
		}
		b, e := io.ReadAll(io.LimitReader(r.Body, 16<<20))
		r.Body.Close()
		if e != nil {
			return nil, e
		}
		if r.StatusCode != 200 {
			if r.StatusCode == http.StatusNotFound {
				for _, name := range names[at:end] {
					one, err := fetchPolicy(client, base, name)
					if err != nil {
						return nil, fmt.Errorf("full L3 exact stored policy unavailable for %q: %w", name, err)
					}
					result[name] = one
				}
				continue
			}
			return nil, fmt.Errorf("full L3 exact policy batch HTTP %d: %.200s", r.StatusCode, b)
		}
		var response pb3.MultiMetricsInfoResponse
		if e = protojson.Unmarshal(b, &response); e != nil {
			return nil, e
		}
		for _, m := range response.Metrics {
			methods := map[string]sealrollup.Method{"average": sealrollup.Average, "sum": sealrollup.Sum, "last": sealrollup.Last, "max": sealrollup.Max, "min": sealrollup.Min, "first": sealrollup.First}
			method, ok := methods[strings.ToLower(m.ConsolidationFunc)]
			if !ok {
				return nil, errors.New("unknown stored aggregation")
			}
			p := v2core.RollupPolicy{Name: v2part.ID(m.Name), Group: "shadow", Config: sealrollup.Config{Method: method, XFilesFactor: m.XFilesFactor}}
			for _, a := range m.Retentions {
				p.Config.Archives = append(p.Config.Archives, sealrollup.Archive{Step: a.SecondsPerPoint, Count: a.NumberOfPoints})
			}
			encoded, e := v2core.EncodeRollupPolicy(p)
			if e != nil {
				return nil, e
			}
			result[m.Name] = encoded
		}
	}
	for _, name := range names {
		if result[name] == nil {
			return nil, fmt.Errorf("missing stored policy for %s", name)
		}
	}
	return result, nil
}
