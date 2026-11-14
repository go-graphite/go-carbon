// cohort-decode converts a bounded native read snapshot to window-scoped float bits.
// It never opens a store database or writes a metric. Codec reads use the existing
// continuation snapshot oracle over the authenticated, checksummed native wire.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/whisperio"
	"github.com/go-graphite/go-carbon/receiver/stream/codec"
)

type window struct {
	From  int64 `json:"from"`
	Until int64 `json:"until"`
}
type archive struct {
	Step            int               `json:"step"`
	Count           int               `json:"count"`
	Points          map[string]string `json:"points"`
	DuplicatePoints int               `json:"duplicate_points,omitempty"`
}
type policy struct {
	Method    int      `json:"method"`
	Retention int      `json:"retention"`
	XFF       string   `json:"xff"`
	Archives  [][2]int `json:"archives"`
}
type snapshot struct {
	Policy            policy                 `json:"policy"`
	Archives          []archive              `json:"archives"`
	RejectedNodeBytes int                    `json:"rejected_node_bytes,omitempty"`
	Version           *store.TransferVersion `json:"version,omitempty"`
	NativeConfig      *store.MetricConfig    `json:"native_config,omitempty"`
}
type recordIO []store.RelayoutKV

func (r recordIO) Get(key []byte) ([]byte, error) {
	i, ok := slices.BinarySearchFunc(r, key, func(kv store.RelayoutKV, k []byte) int { return bytes.Compare(kv.Key, k) })
	if !ok {
		return nil, store.ErrNotFound
	}
	return bytes.Clone(r[i].Value), nil
}
func (r recordIO) Visit(prefix []byte, visit func([]byte, []byte) error) error {
	i, _ := slices.BinarySearchFunc(r, prefix, func(kv store.RelayoutKV, k []byte) int { return bytes.Compare(kv.Key, k) })
	for _, kv := range r[i:] {
		if !bytes.HasPrefix(kv.Key, prefix) {
			break
		}
		if err := visit(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}
func (recordIO) Set([]byte, []byte) error  { return errors.New("read-only cohort snapshot") }
func (recordIO) Delete([]byte) error       { return errors.New("read-only cohort snapshot") }
func (recordIO) DeletePrefix([]byte) error { return errors.New("read-only cohort snapshot") }

func nativeSnapshot(r store.TransferMetric) (store.Snapshot, error) {
	m := store.Metadata{MetricConfig: r.Config, ID: r.Version.ID, Generation: r.Version.Generation, Revision: r.Version.Revision, Continuation: r.Continuation}
	var s store.Snapshot
	var err error
	if r.Continuation {
		s, err = (&whisperio.ContinuationCodec{}).Snapshot(context.Background(), recordIO(r.Keys), m, time.Now)
		if err != nil {
			return store.Snapshot{}, err
		}
	} else {
		s = store.Snapshot{Metadata: m, Archives: make([]store.Archive, len(r.Config.Retentions))}
		for i, ret := range r.Config.Retentions {
			s.Archives[i].Retention = ret
		}
		for _, kv := range r.Keys {
			if len(kv.Key) != 6 {
				return store.Snapshot{}, store.ErrFormat
			}
			ai := int(binary.BigEndian.Uint16(kv.Key[:2]))
			if ai >= len(s.Archives) {
				return store.Snapshot{}, store.ErrFormat
			}
			var block store.PointBlock
			if err = store.DecodePointBlock(kv.Value, &block); err != nil {
				return store.Snapshot{}, err
			}
			for i, p := range block.Points {
				if block.Present[i/64]&(1<<uint(i%64)) != 0 {
					s.Archives[ai].Points = append(s.Archives[ai].Points, p)
				}
			}
		}
	}
	return s, nil
}

func decode(input io.Reader, windows []window) (map[string]*snapshot, error) {
	return decodeIdentity(input, windows, false)
}

func decodeIdentity(input io.Reader, windows []window, identity bool) (map[string]*snapshot, error) {
	records, err := store.ReadTransfer(input, 64<<20)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*snapshot, len(records))
	for _, r := range records {
		if _, ok := result[r.Config.Name]; ok {
			return nil, errors.New("duplicate native metric")
		}
		if r.Version.ID == 0 {
			result[r.Config.Name] = nil
			continue
		}
		s, err := nativeSnapshot(r)
		if err != nil {
			return nil, err
		}
		out := &snapshot{Policy: policy{Method: int(r.Config.AggregationMethod), XFF: fmt.Sprintf("%08x", math.Float32bits(r.Config.XFilesFactor))}, Archives: make([]archive, len(s.Archives))}
		if identity {
			version, config := r.Version, r.Config
			out.Version, out.NativeConfig = &version, &config
		}
		path, _, _ := strings.Cut(r.Config.Name, ";")
		for _, node := range strings.Split(path, ".") {
			if errors.Is(store.ValidateMetricName(node), store.ErrNameTooLong) {
				out.RejectedNodeBytes = max(out.RejectedNodeBytes, len(node))
			}
		}
		for i, a := range s.Archives {
			out.Policy.Archives = append(out.Policy.Archives, [2]int{a.Retention.Step, a.Retention.Count})
			out.Policy.Retention = max(out.Policy.Retention, a.Retention.Step*a.Retention.Count)
			out.Archives[i] = archive{Step: a.Retention.Step, Count: a.Retention.Count, Points: make(map[string]string)}
			for _, p := range a.Points {
				if p.Timestamp <= 0 {
					continue
				}
				keep := len(windows) == 0
				for _, w := range windows {
					keep = keep || w.From <= p.Timestamp && p.Timestamp < w.Until
				}
				if keep {
					key := strconv.FormatInt(p.Timestamp, 10)
					bits := fmt.Sprintf("%016x", math.Float64bits(p.Value))
					if previous, ok := out.Archives[i].Points[key]; ok {
						if previous != bits {
							return nil, fmt.Errorf("conflicting physical timestamp %s archive %d time %s: %s != %s", r.Config.Name, i, key, previous, bits)
						}
						out.Archives[i].DuplicatePoints++
						continue
					}
					out.Archives[i].Points[key] = bits
				}
			}
		}
		result[r.Config.Name] = out
	}
	return result, nil
}

type inputPoint struct {
	Name      string `json:"name"`
	Timestamp int64  `json:"timestamp"`
	Bits      string `json:"bits"`
}

func stream(input io.Reader) ([]byte, error) {
	var points []inputPoint
	if err := json.NewDecoder(io.LimitReader(input, 1<<20)).Decode(&points); err != nil {
		return nil, err
	}
	if len(points) > 256 {
		return nil, errors.New("stream batch exceeds 256 points")
	}
	e, err := codec.NewEncoder(codec.Options{})
	if err != nil {
		return nil, err
	}
	defer e.Close()
	for _, p := range points {
		bits, err := strconv.ParseUint(p.Bits, 16, 64)
		if err != nil || len(p.Bits) != 16 || p.Timestamp <= 0 || p.Name == "" {
			return nil, errors.New("invalid exact point")
		}
		if err = e.Add([]byte(p.Name), math.Float64frombits(bits), p.Timestamp); err != nil {
			return nil, err
		}
	}
	out, err := codec.AppendPreamble(nil, 0)
	if err != nil {
		return nil, err
	}
	return e.Flush(out)
}
func main() {
	raw := flag.String("windows", "[]", "fixed half-open windows as JSON")
	mode := flag.String("mode", "decode", "decode native snapshots or stream exact input points")
	offlineDirectory := flag.String("offline-directory", "", "exclusively owned stopped store for read-only native pipe mode")
	digestFrom := flag.Int64("from", 0, "inclusive digest timestamp")
	digestUntil := flag.Int64("until", 0, "exclusive digest timestamp")
	identity := flag.Bool("identity", false, "include native version and complete metric configuration")
	joinSpec := flag.String("join-spec", "", "owned JSON digest input paths and SHA256 identities")
	joinSummary := flag.String("join-summary", "", "write counts only after complete digest stream validation")
	flag.Parse()
	if *mode == "offline-native" {
		if err := offlineNative(*offlineDirectory, os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *mode == "digest-join" || *mode == "windows-join" {
		raw, err := os.ReadFile(*joinSpec)
		var counts any
		if err == nil {
			if *mode == "digest-join" {
				var spec digestJoinSpec
				err = json.Unmarshal(raw, &spec)
				if err == nil {
					counts, err = joinDigests(spec, os.Stdout)
				}
			} else {
				var spec windowJoinSpec
				err = json.Unmarshal(raw, &spec)
				if err == nil {
					counts, err = joinWindowDigests(spec, os.Stdout)
				}
			}
		}
		if err == nil {
			var body []byte
			body, err = json.Marshal(counts)
			if err == nil {
				err = os.WriteFile(*joinSummary, append(body, '\n'), 0600)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *mode == "points-framed" {
		if err := pointFrames(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *mode == "digest" {
		var windows []window
		if err := json.Unmarshal([]byte(*raw), &windows); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := digestFramesWindows(os.Stdin, os.Stdout, *digestFrom, *digestUntil, windows); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *mode == "stream" {
		out, err := stream(os.Stdin)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if _, err = os.Stdout.Write(out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	var w []window
	if err := json.Unmarshal([]byte(*raw), &w); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result, err := decodeIdentity(os.Stdin, w, *identity)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
