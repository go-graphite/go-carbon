package main

// The replay adapter deliberately owns no storage engine implementation.
import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/v2core"
)

type row = v2core.Row
type coreConfig = v2core.Config
type coreBatch = v2core.Batch
type coreHooks = v2core.Hooks
type coreSealInput = v2core.SealInput
type coreSealPlan = v2core.SealPlan

const recordBytes = 32

type coreEngine struct {
	engine *v2core.Engine
	cfg    coreConfig
}

func openCore(root string, cfg coreConfig) (*coreEngine, error) {
	return openCoreWithHooks(root, cfg, coreHooks{})
}
func openCoreWithHooks(root string, cfg coreConfig, h coreHooks) (*coreEngine, error) {
	engine, err := v2core.Open(root, cfg, h)
	return &coreEngine{engine: engine, cfg: cfg}, err
}
func (e *coreEngine) submit(p int, rs []row, c uint64, s bool) error {
	return e.engine.Append(p, rs, c, s)
}
func (e *coreEngine) submitBatches(p int, bs []coreBatch, c uint64, s bool) error {
	return e.engine.AppendBatches(p, bs, c, s)
}
func (e *coreEngine) durableFrontiers() [][]uint64 { return e.engine.DurableFrontiers() }
func (e *coreEngine) close() error                 { return e.engine.Close() }

func putRow(b []byte, r row) {
	binary.LittleEndian.PutUint64(b, r.ID)
	binary.LittleEndian.PutUint64(b[8:], uint64(r.Time))
	binary.LittleEndian.PutUint64(b[16:], r.Bits)
	binary.LittleEndian.PutUint64(b[24:], r.Seq)
}
func getRow(b []byte) row {
	return row{ID: binary.LittleEndian.Uint64(b), Time: int64(binary.LittleEndian.Uint64(b[8:])), Bits: binary.LittleEndian.Uint64(b[16:]), Seq: binary.LittleEndian.Uint64(b[24:])}
}
func coreShard(id uint64, n int) int                        { return v2core.Shard(id, n) }
func coreRead(s *pebble.Snapshot, id uint64) ([]row, error) { return v2core.Read(s, id) }
func procStats() map[string]float64 {
	out := map[string]float64{}
	b, _ := os.ReadFile("/proc/self/io")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			v, _ := strconv.ParseFloat(f[1], 64)
			out[strings.TrimSuffix(f[0], ":")] = v
		}
	}
	b, _ = os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && (f[0] == "VmHWM:" || f[0] == "VmRSS:") {
			v, _ := strconv.ParseFloat(f[1], 64)
			out[strings.TrimSuffix(f[0], ":")+"_bytes"] = v * 1024
		}
	}
	b, _ = os.ReadFile("/proc/self/stat")
	i := strings.LastIndex(string(b), ")")
	if i >= 0 {
		f := strings.Fields(string(b[i+1:]))
		if len(f) > 12 {
			u, _ := strconv.ParseFloat(f[11], 64)
			st, _ := strconv.ParseFloat(f[12], 64)
			out["cpu_seconds_CLK_TCK100"] = (u + st) / 100
		}
	}
	return out
}

func coreSample(e *coreEngine, stage string, start time.Time, offered uint64) map[string]any {
	return v2core.Sample(e.engine, stage, start, offered)
}

// Replay writes include the concurrently admitted live journal, generated SSTs
// (data and frontier), and Pebble compaction output. These are separate logical
// file-byte counters, not a decomposition of Linux's block-rounded write_bytes.
func replayWriteStages(root string, sample map[string]any) error {
	var journal uint64
	err := filepath.WalkDir(filepath.Join(root, "log"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err == nil {
			journal += uint64(info.Size())
		}
		return err
	})
	if err != nil {
		return err
	}
	sample["journal_file_bytes"] = journal
	sample["raw_and_frontier_sst_file_bytes"] = sample["sink_bytes"]
	sample["compaction_output_file_bytes"] = sample["compacted_bytes"]
	return nil
}
