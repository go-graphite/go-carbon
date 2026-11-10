// inputlog-inventory reads an owner-copied journal prefix. Its output identifies
// recovery candidates, never authorizes a store write or substitutes journal
// values for the source store's committed archive values.
package main

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-graphite/go-carbon/internal/inputlog"
	"github.com/go-graphite/go-carbon/points"
)

type manifest struct {
	Version            int
	Snapshot           string
	SHA256             string
	Bytes              int64
	Windows            []window
	MaxNames           int
	MaxDictionaryBytes int64
}

type window struct {
	Name        string
	From, Until int64
}

type candidate struct {
	Name        string
	Windows     map[string]int64
	First, Last int64
}

func inventory(ctx context.Context, m manifest, output io.Writer) error {
	if m.MaxNames == 0 {
		m.MaxNames = 2_000_000
	}
	if m.MaxDictionaryBytes == 0 {
		m.MaxDictionaryBytes = 256 << 20
	}
	if m.Version != 1 || !strings.HasPrefix(m.Snapshot, "/") || m.Bytes < 64 || m.Bytes > 4<<30 || len(m.SHA256) != 64 || len(m.Windows) == 0 || len(m.Windows) > 32 {
		return errors.New("require version1, absolute copied snapshot, SHA256, <=4GiB exact length and1..32 windows")
	}
	if m.MaxNames < 1 || m.MaxNames > 16_000_000 || m.MaxDictionaryBytes < 1 || m.MaxDictionaryBytes > 2<<30 {
		return errors.New("inspection requires bounded1..16M names and1..2GiB dictionary bytes")
	}
	if _, err := hex.DecodeString(m.SHA256); err != nil {
		return err
	}
	windows := map[string]bool{}
	for _, w := range m.Windows {
		if w.Name == "" || windows[w.Name] || w.From <= 0 || w.From >= w.Until {
			return errors.New("unique named positive timestamp windows required")
		}
		windows[w.Name] = true
	}
	file, err := os.Open(m.Snapshot)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != m.Bytes {
		return errors.New("copied snapshot size differs; active preallocated logs are not accepted")
	}
	digest := sha256.New()
	byName := map[string]*candidate{}
	var selected int64
	stats, err := inputlog.Inspect(ctx, io.TeeReader(file, digest), inputlog.InspectLimits{
		Bytes: m.Bytes, Points: 2_000_000_000, Frames: 2_000_000, Names: m.MaxNames, DictionaryBytes: m.MaxDictionaryBytes,
	}, func(p *points.Points) error {
		for _, point := range p.Data {
			if math.IsNaN(point.Value) {
				continue
			}
			for _, w := range m.Windows {
				if point.Timestamp < w.From || point.Timestamp >= w.Until {
					continue
				}
				c := byName[p.Metric]
				if c == nil {
					if len(byName) >= m.MaxNames {
						return errors.New("candidate name limit exceeded")
					}
					c = &candidate{Name: p.Metric, Windows: map[string]int64{}, First: point.Timestamp, Last: point.Timestamp}
					byName[p.Metric] = c
				}
				c.First, c.Last = min(c.First, point.Timestamp), max(c.Last, point.Timestamp)
				c.Windows[w.Name]++
				selected++
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("unverified inspection stopped after %+v: %w", stats, err)
	}
	if stats.Bytes != m.Bytes || hex.EncodeToString(digest.Sum(nil)) != m.SHA256 {
		return errors.New("copied snapshot SHA256 or exact frame length differs")
	}
	// Nothing is emitted before complete frame, dictionary and SHA verification.
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	encoder := json.NewEncoder(output)
	if err := encoder.Encode(map[string]any{"mode": "journal-recovery-candidates", "version": 1, "manifest": m, "stats": stats, "names": len(names), "selected_records": selected, "write_authority": false, "coverage": "raw metric timestamps only; excludes events outside requested windows and records outside this copied prefix"}); err != nil {
		return err
	}
	for _, name := range names {
		if err := encoder.Encode(byName[name]); err != nil {
			return err
		}
	}
	return nil
}

func run() error {
	manifestPath := flag.String("manifest", "", "owner-copied snapshot and raw metric timestamp windows")
	outputPath := flag.String("output", "", "new gzip JSONL candidate file; removed on any failure")
	flag.Parse()
	if *manifestPath == "" || *outputPath == "" || flag.NArg() != 0 {
		return errors.New("-manifest and -output are required")
	}
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		return err
	}
	if len(raw) > 64<<10 {
		return errors.New("manifest exceeds64KiB")
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	output, err := os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		output.Close()
		if !success {
			os.Remove(*outputPath)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	compressed := gzip.NewWriter(output)
	if err := inventory(ctx, m, compressed); err != nil {
		return err
	}
	if err := compressed.Close(); err != nil {
		return err
	}
	if err := output.Sync(); err != nil {
		return err
	}
	success = true
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
