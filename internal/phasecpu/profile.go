package phasecpu

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"math/rand/v2"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/encoding/protowire"
)

const Window = time.Second
const MinInterval = time.Minute

var owner sync.Mutex

type Profiler struct {
	buffer             bytes.Buffer
	stop, done         chan struct{}
	once               sync.Once
	cpu, sampled       [numPhases]atomic.Uint64
	windows, failures  atomic.Uint64
	covered, accounted atomic.Uint64
}

// Start owns the process CPU profiler for one second per interval. Other CPU
// profilers may win a window; that window is skipped, never stopped or estimated.
func Start(interval time.Duration) (*Profiler, error) { return StartWithDuration(interval, Window) }

// StartWithDuration limits sampling to at most 2% even at the shortest jittered
// period. The default is one second per minute; parsing runs after sampling ends.
func StartWithDuration(interval, window time.Duration) (*Profiler, error) {
	if err := Validate(interval, window); err != nil {
		return nil, err
	}
	if !owner.TryLock() {
		return nil, errors.New("phase CPU profiler already running")
	}
	p := &Profiler{stop: make(chan struct{}), done: make(chan struct{})}
	go p.run(interval, window)
	return p, nil
}

func (p *Profiler) Stop() {
	p.once.Do(func() { close(p.stop); <-p.done })
}

func (p *Profiler) run(interval, window time.Duration) {
	defer close(p.done)
	defer owner.Unlock()
	Set(ProfileWork)
	period := interval
	timer := time.NewTimer(period - window)
	defer timer.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-timer.C:
		}
		started := time.Now()
		p.capture(period, window)
		// Avoid repeatedly missing work synchronized to the nominal interval.
		period = time.Duration(float64(interval) * (.95 + .1*rand.Float64()))
		timer.Reset(max(0, period-time.Since(started)))
	}
}

func (p *Profiler) capture(interval, window time.Duration) {
	p.buffer.Reset()
	if err := pprof.StartCPUProfile(&p.buffer); err != nil {
		p.failures.Add(1)
		return
	}
	started := time.Now()
	timer := time.NewTimer(window)
	select {
	case <-p.stop:
	case <-timer.C:
	}
	timer.Stop()
	elapsed := time.Since(started)
	pprof.StopCPUProfile()
	totals, err := decode(p.buffer.Bytes())
	if err != nil {
		p.failures.Add(1)
		return
	}
	// A stop may interrupt a window. Keep measured samples, but do not inflate
	// that partial window to the full interval.
	scale := float64(interval) / float64(elapsed)
	select {
	case <-p.stop:
		scale = 1
	default:
	}
	for phase, nanos := range totals {
		p.sampled[phase].Add(nanos)
		p.cpu[phase].Add(uint64(float64(nanos) * scale))
	}
	p.covered.Add(uint64(elapsed))
	if scale == 1 {
		p.accounted.Add(uint64(elapsed))
	} else {
		p.accounted.Add(uint64(interval))
	}
	p.windows.Add(1)
}

var (
	cpuDesc       = prometheus.NewDesc("go_carbon_phase_cpu_seconds_total", "Estimated CPU seconds by phase; sampled CPU extrapolated over each successful profiling interval.", []string{"phase"}, nil)
	sampledDesc   = prometheus.NewDesc("go_carbon_phase_sampled_cpu_seconds_total", "CPU seconds actually observed in profiling windows, without extrapolation.", []string{"phase"}, nil)
	windowsDesc   = prometheus.NewDesc("go_carbon_phase_cpu_profile_windows_total", "Successful CPU profiling windows.", nil, nil)
	failuresDesc  = prometheus.NewDesc("go_carbon_phase_cpu_profile_failures_total", "Skipped or unreadable CPU profiling windows, including contention with another profiler.", nil, nil)
	accountedDesc = prometheus.NewDesc("go_carbon_phase_cpu_accounted_seconds_total", "Wall seconds represented by successful CPU estimates; excludes skipped profiling intervals.", nil, nil)
	coveredDesc   = prometheus.NewDesc("go_carbon_phase_cpu_profile_seconds_total", "Wall seconds actually covered by CPU profiling.", nil, nil)
)

func (*Profiler) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{cpuDesc, sampledDesc, windowsDesc, failuresDesc, coveredDesc, accountedDesc} {
		ch <- d
	}
}
func (p *Profiler) Collect(ch chan<- prometheus.Metric) {
	for phase := range numPhases {
		name := names[phase]
		ch <- prometheus.MustNewConstMetric(cpuDesc, prometheus.CounterValue, float64(p.cpu[phase].Load())/1e9, name)
		ch <- prometheus.MustNewConstMetric(sampledDesc, prometheus.CounterValue, float64(p.sampled[phase].Load())/1e9, name)
	}
	ch <- prometheus.MustNewConstMetric(windowsDesc, prometheus.CounterValue, float64(p.windows.Load()))
	ch <- prometheus.MustNewConstMetric(failuresDesc, prometheus.CounterValue, float64(p.failures.Load()))
	ch <- prometheus.MustNewConstMetric(coveredDesc, prometheus.CounterValue, float64(p.covered.Load())/1e9)
	ch <- prometheus.MustNewConstMetric(accountedDesc, prometheus.CounterValue, float64(p.accounted.Load())/1e9)
}

// fields visits a protobuf message without allocating a general profile object.
// The profile schema is runtime/pprof/proto.go.
func fields(data []byte, visit func(protowire.Number, protowire.Type, []byte, uint64)) error {
	for len(data) > 0 {
		n, t, k := protowire.ConsumeTag(data)
		if k < 0 {
			return protowire.ParseError(k)
		}
		data = data[k:]
		var b []byte
		var v uint64
		switch t {
		case protowire.BytesType:
			b, k = protowire.ConsumeBytes(data)
		case protowire.VarintType:
			v, k = protowire.ConsumeVarint(data)
		default:
			k = protowire.ConsumeFieldValue(n, t, data)
		}
		if k < 0 {
			return protowire.ParseError(k)
		}
		visit(n, t, b, v)
		data = data[k:]
	}
	return nil
}

func decode(data []byte) (out [numPhases]uint64, err error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return out, err
	}
	defer zr.Close()
	const maxProfile = 32 << 20
	raw, err := io.ReadAll(io.LimitReader(zr, maxProfile+1))
	if err != nil {
		return out, err
	}
	if len(raw) > maxProfile {
		return out, errors.New("phase CPU profile exceeds 32MiB")
	}
	var table []string
	var samples, types, locations, functions [][]byte
	err = fields(raw, func(n protowire.Number, t protowire.Type, b []byte, v uint64) {
		switch n {
		case 1:
			types = append(types, b)
		case 2:
			samples = append(samples, b)
		case 4:
			locations = append(locations, b)
		case 5:
			functions = append(functions, b)
		case 6:
			table = append(table, string(b))
		}
	})
	if err != nil {
		return out, err
	}
	str := func(i uint64) string {
		if i < uint64(len(table)) {
			return table[i]
		}
		return ""
	}
	cpu := -1
	for i, b := range types {
		var name, unit uint64
		if err = fields(b, func(n protowire.Number, t protowire.Type, b []byte, v uint64) {
			if n == 1 {
				name = v
			}
			if n == 2 {
				unit = v
			}
		}); err != nil {
			return out, err
		}
		if str(name) == "cpu" && str(unit) == "nanoseconds" {
			cpu = i
		}
	}
	if cpu < 0 {
		return out, errors.New("phase CPU profile has no cpu/nanoseconds sample type")
	}
	// Cache operations run in several caller subsystems. Use their existing
	// stack frames to isolate cache CPU without changing labels on every hit.
	functionPhases := make(map[uint64]Phase)
	for _, b := range functions {
		var id, name uint64
		if err = fields(b, func(n protowire.Number, t protowire.Type, b []byte, v uint64) {
			if n == 1 {
				id = v
			}
			if n == 2 {
				name = v
			}
		}); err != nil {
			return out, err
		}
		if phase := framePhase(str(name)); phase != Other {
			functionPhases[id] = phase
		}
	}
	locationPhases := make(map[uint64]Phase)
	for _, b := range locations {
		var id uint64
		phase := Other
		var nested error
		if err = fields(b, func(n protowire.Number, t protowire.Type, b []byte, v uint64) {
			if n == 1 {
				id = v
			}
			if n == 4 {
				nested = fields(b, func(n protowire.Number, t protowire.Type, b []byte, v uint64) {
					if n == 1 && phase == Other {
						phase = functionPhases[v]
					}
				})
			}
		}); err != nil {
			return out, err
		}
		if nested != nil {
			return out, nested
		}
		if phase != Other {
			locationPhases[id] = phase
		}
	}
	for _, sample := range samples {
		phase := Other
		pebble := Other
		stage := false
		stackPhase := Other
		var sampleCPU uint64
		valueCount := 0
		takeValue := func(v uint64) {
			if valueCount == cpu {
				sampleCPU = v
			}
			valueCount++
		}
		var nested error
		err = fields(sample, func(n protowire.Number, t protowire.Type, b []byte, v uint64) {
			if nested != nil {
				return
			}
			if n == 1 {
				if t == protowire.VarintType {
					if stackPhase == Other {
						stackPhase = locationPhases[v]
					}
				}
				if t == protowire.BytesType {
					for len(b) > 0 {
						value, k := protowire.ConsumeVarint(b)
						if k < 0 {
							nested = protowire.ParseError(k)
							return
						}
						if stackPhase == Other {
							stackPhase = locationPhases[value]
						}
						b = b[k:]
					}
				}
			}
			if n == 2 {
				if t == protowire.VarintType {
					takeValue(v)
				} else if t == protowire.BytesType {
					for len(b) > 0 {
						value, k := protowire.ConsumeVarint(b)
						if k < 0 {
							nested = protowire.ParseError(k)
							return
						}
						takeValue(value)
						b = b[k:]
					}
				}
			}
			if n == 3 {
				var key, value uint64
				nested = fields(b, func(n protowire.Number, t protowire.Type, b []byte, v uint64) {
					if n == 1 {
						key = v
					}
					if n == 2 {
						value = v
					}
				})
				switch str(key) {
				case "go_carbon_phase":
					for p, name := range names {
						if name == str(value) {
							phase = Phase(p)
							break
						}
					}
				case "persister_stage":
					stage = true
				case "pebble":
					switch str(value) {
					case "flush":
						pebble = PebbleFlush
					case "compact":
						pebble = PebbleCompaction
					case "wal-sync":
						pebble = PebbleWAL
					case "table-cache":
						pebble = PebbleTableCache
					case "gc":
						pebble = PebbleGC
					}
				}
			}
		})
		if err != nil {
			return out, err
		}
		if nested != nil {
			return out, nested
		}
		if cpu >= valueCount || int64(sampleCPU) < 0 {
			return out, errors.New("invalid phase CPU sample values")
		}
		switch {
		case pebble != Other:
			phase = pebble
		case stackPhase != Other:
			phase = stackPhase
		case stage:
			phase = Persister
		}
		out[phase] += sampleCPU
	}
	return out, nil
}

func metadataCacheFrame(name string) bool {
	const prefix = "github.com/go-graphite/go-carbon/internal/chunkstore."
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	name = strings.TrimPrefix(name, prefix)
	return strings.HasPrefix(name, "(*stateCache).") || name == "decodeStateRecord" || name == "encodeStateKeys"
}

// Validate enforces a conservative sampling duty bound including period jitter.
func Validate(interval, window time.Duration) error {
	if window <= 0 || interval <= 0 || window > interval/60 {
		return errors.New("phase CPU profile duration must be positive and at most 1/60 of its interval (duty below 2% including jitter)")
	}
	return nil
}

func framePhase(name string) Phase {
	if metadataCacheFrame(name) {
		return MetadataCache
	}
	const prefix = "github.com/go-graphite/go-carbon/cache."
	if !strings.HasPrefix(name, prefix) {
		return Other
	}
	switch strings.TrimPrefix(name, prefix) {
	case "(*Cache).Add", "(*Cache).AddPoint", "(*Cache).add":
		return CacheInsert
	case "(*WriteoutQueue).Get", "(*WriteoutQueue).get", "(*WriteoutQueue).update", "(*Cache).makeQueue", "(*Cache).makeQueueAt":
		return WriteoutQueue
	}
	return Other
}
