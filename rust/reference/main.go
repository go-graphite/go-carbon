// Command reference exposes the pinned Go Whisper implementation to Rust's
// cross-language compatibility tests. It is not part of the Rust runtime.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/go-graphite/go-carbon/carbonserver"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver/parse"
	whisper "github.com/go-graphite/go-whisper"
)

type retention struct {
	SecondsPerPoint int `json:"seconds_per_point"`
	Points          int `json:"points"`
}

type point struct {
	Timestamp int     `json:"timestamp"`
	Value     float64 `json:"value"`
}

type request struct {
	Op          string                  `json:"op"`
	Path        string                  `json:"path"`
	Now         int64                   `json:"now"`
	Retentions  []retention             `json:"retentions"`
	Aggregation string                  `json:"aggregation"`
	XFF         float32                 `json:"xff"`
	Compressed  bool                    `json:"compressed"`
	OutOfOrder  bool                    `json:"out_of_order"`
	Sparse      bool                    `json:"sparse"`
	Flock       bool                    `json:"flock"`
	Points      []point                 `json:"points"`
	From        int                     `json:"from"`
	Until       int                     `json:"until"`
	Line        string                  `json:"line"`
	Metric      string                  `json:"metric"`
	Version     int                     `json:"version"`
	Entries     []carbonserver.FLCEntry `json:"entries"`
}

func execute(r request) (result any, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("reference panic: %v", value)
		}
	}()
	whisper.Now = func() time.Time { return time.Unix(r.Now, 0) }
	if r.Op == "dump_write" {
		file, err := os.Create(r.Path)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		p := &points.Points{Metric: r.Metric}
		for _, value := range r.Points {
			p.Data = append(p.Data, points.Point{Timestamp: int64(value.Timestamp), Value: value.Value})
		}
		_, err = p.WriteBinaryTo(file)
		return nil, err
	}
	if r.Op == "dump_read" {
		file, err := os.Open(r.Path)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		var result []map[string]any
		err = points.ReadBinary(file, func(p *points.Points) {
			values := []point{}
			for _, value := range p.Data {
				values = append(values, point{Timestamp: int(value.Timestamp), Value: value.Value})
			}
			result = append(result, map[string]any{"metric": p.Metric, "points": values})
		})
		return result, err
	}
	if r.Op == "flc_write" || r.Op == "flc_read" {
		mode := byte('r')
		if r.Op == "flc_write" {
			mode = 'w'
		}
		flc, err := carbonserver.NewFileListCache(r.Path, carbonserver.FLCVersion(r.Version), mode)
		if err != nil {
			return nil, err
		}
		if mode == 'w' {
			for i := range r.Entries {
				if err = flc.Write(&r.Entries[i]); err != nil {
					flc.Close()
					return nil, err
				}
			}
			return nil, flc.Close()
		}
		defer flc.Close()
		entries := []carbonserver.FLCEntry{}
		for {
			entry, err := flc.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			entries = append(entries, *entry)
		}
		return entries, nil
	}
	if r.Op == "parse" {
		name, value, timestamp, err := parse.PlainLine([]byte(r.Line))
		if err != nil {
			return nil, err
		}
		return map[string]any{"metric": string(name), "timestamp": timestamp, "value": jsonFloat(value)}, nil
	}
	opts := &whisper.Options{Compressed: r.Compressed, OutOfOrder: r.OutOfOrder, Sparse: r.Sparse, FLock: r.Flock}
	var w *whisper.Whisper
	if r.Op == "create" {
		rets := make(whisper.Retentions, len(r.Retentions))
		for i, ret := range r.Retentions {
			v := whisper.NewRetention(ret.SecondsPerPoint, ret.Points)
			rets[i] = &v
		}
		w, err = whisper.CreateWithOptions(r.Path, rets, whisper.ParseAggregationMethod(r.Aggregation), r.XFF, opts)
	} else {
		w, err = whisper.OpenWithOptions(r.Path, opts)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
	}()
	switch r.Op {
	case "create", "info":
		rets := make([]retention, 0, len(w.Retentions()))
		for _, ret := range w.Retentions() {
			rets = append(rets, retention{ret.SecondsPerPoint(), ret.NumberOfPoints()})
		}
		return map[string]any{"aggregation": w.AggregationMethod().String(), "xff": w.XFilesFactor(), "retentions": rets, "compressed": w.IsCompressed()}, nil
	case "update":
		ps := make([]*whisper.TimeSeriesPoint, len(r.Points))
		for i, p := range r.Points {
			ps[i] = &whisper.TimeSeriesPoint{Time: p.Timestamp, Value: p.Value}
		}
		if err := w.UpdateMany(ps); err != nil {
			return nil, err
		}
		return map[string]any{"updated": len(ps), "diverted": w.OutOfOrderPoints}, nil
	case "fetch":
		series, err := w.Fetch(r.From, r.Until)
		if err != nil || series == nil {
			return nil, err
		}
		values := make([]any, len(series.Values()))
		for i, v := range series.Values() {
			values[i] = jsonFloat(v)
		}
		return map[string]any{"from": series.FromTime(), "until": series.UntilTime(), "step": series.Step(), "values": values}, nil
	case "compact":
		return nil, w.MergeOutOfOrder()
	case "integrity":
		return nil, w.CheckIntegrity()
	default:
		return nil, fmt.Errorf("unknown operation %q", r.Op)
	}
}

func jsonFloat(value float64) any {
	switch {
	case math.IsNaN(value):
		return nil
	case math.IsInf(value, 1):
		return "+Inf"
	case math.IsInf(value, -1):
		return "-Inf"
	default:
		return value
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var r request
		var result any
		err := json.Unmarshal(scanner.Bytes(), &r)
		if err == nil {
			result, err = execute(r)
		}
		out := map[string]any{"result": result}
		if err != nil {
			out["error"] = err.Error()
		}
		if err := encoder.Encode(out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
