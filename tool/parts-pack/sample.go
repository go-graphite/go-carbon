package main

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partspack"
	pb "github.com/go-graphite/protocol/carbonapi_v3_pb"
	"golang.org/x/sys/unix"
)

type sampleBlock struct {
	points  []chunkstore.Point
	step    uint32
	encoded []byte
}
type sampleReport struct {
	Scope                                                                                    string
	Metrics, Blocks, Points, Finite, ColdPayloadBytes, BeforePayloadBytes, AfterPayloadBytes uint64
	ColdSealCPUPerPointNS, PackSealCPUPerPointNS                                             float64
	ColdDecodeP99NS, PackDecodeP99NS                                                         int64
	BitExact                                                                                 bool
}

func processSample(dir string) error {
	paths, err := filepath.Glob(filepath.Join(dir, "sample-*.pb"))
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return errors.New("no protobuf samples")
	}
	r := sampleReport{Scope: "API-render payload only; API does not expose physical occupancy or complete stored archive policy"}
	var blocks []sampleBlock
	for _, path := range paths {
		st, err := os.Stat(path)
		if err != nil {
			return err
		}
		if st.Size() > 16<<20 {
			return errors.New("sample file bound")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var response pb.MultiFetchResponse
		if err := response.UnmarshalVT(data); err != nil {
			return err
		}
		for _, m := range response.Metrics {
			if m.StepTime < 1 || m.StepTime > math.MaxUint32 {
				return errors.New("sample step out of range")
			}
			if len(m.Values) > 8<<20 || r.Points+uint64(len(m.Values)) > 8<<20 {
				return errors.New("sample exceeds 128MiB point bound")
			}
			if len(m.Values) > 0 && (int64(len(m.Values)-1) > math.MaxInt64/m.StepTime || m.StartTime > math.MaxInt64-int64(len(m.Values)-1)*m.StepTime) {
				return errors.New("sample timestamp overflow")
			}
			r.Metrics++
			for start := 0; start < len(m.Values); start += 4096 {
				end := min(start+4096, len(m.Values))
				ps := make([]chunkstore.Point, end-start)
				for i := range ps {
					v := m.Values[start+i]
					ps[i] = chunkstore.Point{Timestamp: m.StartTime + int64(start+i)*m.StepTime, Value: v}
					if !math.IsNaN(v) && !math.IsInf(v, 0) {
						r.Finite++
					}
				}
				encoded, err := partspack.EncodeBlock(ps, uint32(m.StepTime))
				if err != nil {
					return err
				}
				cold, err := chunkstore.EncodeSealedPoints(ps, int(m.StepTime))
				if err != nil {
					return err
				}
				if string(encoded) != string(cold) {
					return errors.New("sample cold-pack bytes differ")
				}
				decoded, err := partspack.DecodeBlock(encoded)
				if err != nil {
					return err
				}
				for i := range ps {
					if ps[i].Timestamp != decoded[i].Timestamp || math.Float64bits(ps[i].Value) != math.Float64bits(decoded[i].Value) {
						return errors.New("sample bits differ")
					}
				}
				r.ColdPayloadBytes += uint64(len(cold))
				r.BeforePayloadBytes += uint64(36 + len(cold))
				r.AfterPayloadBytes += uint64(len(encoded))
				r.Points += uint64(len(ps))
				r.Blocks++
				blocks = append(blocks, sampleBlock{ps, uint32(m.StepTime), encoded})
			}
		}
	}
	if r.Points == 0 {
		return errors.New("sample has no rendered slots")
	}
	// Alternating whole-corpus CPU arms include the same exact codec and allocations.
	var cpu [2]float64
	const repeats = 4
	for repeat := 0; repeat < repeats; repeat++ {
		for arm := 0; arm < 2; arm++ {
			which := (arm + repeat) % 2
			start := processCPU()
			for _, block := range blocks {
				var err error
				if which == 0 {
					_, err = chunkstore.EncodeSealedPoints(block.points, int(block.step))
				} else {
					_, err = partspack.EncodeBlock(block.points, block.step)
				}
				if err != nil {
					return err
				}
			}
			cpu[which] += processCPU() - start
		}
	}
	r.ColdSealCPUPerPointNS = cpu[0] * 1e9 / float64(r.Points*repeats)
	r.PackSealCPUPerPointNS = cpu[1] * 1e9 / float64(r.Points*repeats)
	var durations [2][]int64
	for repeat := 0; repeat < 4; repeat++ {
		for i, block := range blocks {
			for arm := 0; arm < 2; arm++ {
				which := (i + repeat + arm) % 2
				start := time.Now()
				var err error
				if which == 0 {
					_, err = chunkstore.DecodeSealedPoints(block.encoded)
				} else {
					_, err = partspack.DecodeBlock(block.encoded)
				}
				durations[which] = append(durations[which], time.Since(start).Nanoseconds())
				if err != nil {
					return err
				}
			}
		}
	}
	for i := range durations {
		slices.Sort(durations[i])
	}
	r.ColdDecodeP99NS = durations[0][(len(durations[0])-1)*99/100]
	r.PackDecodeP99NS = durations[1][(len(durations[1])-1)*99/100]
	r.BitExact = true
	return json.NewEncoder(os.Stdout).Encode(r)
}
func processCPU() float64 {
	var r unix.Rusage
	if unix.Getrusage(unix.RUSAGE_SELF, &r) != nil {
		return math.NaN()
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
