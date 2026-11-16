//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
)

// storedComparisonInput reads actual stored slots, including stale occupants.
// It never writes the conversion target or changes its publication authority.
// Repeated part images must agree bit for bit before their slots are deduplicated.
func storedComparisonInput(ctx context.Context, target string) (map[partsconvert.Unit][]partsconvert.StoredMetric, error) {
	db, err := pebble.Open(filepath.Join(target, "parts"), &pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	it, err := db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	type identity struct{ group, name string }
	metrics := make(map[identity]*partsconvert.StoredMetric)
	var inputBytes, decodedBytes uint64
	for ok := it.First(); ok; ok = it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		inputBytes += uint64(len(it.Key()) + len(it.Value()))
		if inputBytes > 256<<20 {
			return nil, errors.New("same-slot comparison exceeds 256 MiB input bound")
		}
		key, err := partsformat.Decode(it.Key())
		if err != nil {
			return nil, err
		}
		m, ai, points, err := partsconvert.DecodeArchive(it.Value())
		if err != nil {
			return nil, err
		}
		if ai != key.Archive {
			return nil, errors.New("comparison archive key mismatch")
		}
		decodedBytes += uint64(len(points)) * 16
		if decodedBytes > 512<<20 {
			return nil, errors.New("same-slot comparison exceeds 512 MiB decoded-slot bound")
		}
		m.Name, m.Group = key.Metric, key.Group
		id := identity{string(key.Group), string(key.Metric)}
		previous := metrics[id]
		if previous == nil {
			previous = &m
			metrics[id] = previous
		} else {
			if previous.Method != m.Method || previous.XFilesFactorBits != m.XFilesFactorBits || len(previous.Archives) != len(m.Archives) {
				return nil, errors.New("comparison policy differs between parts")
			}
			for i, a := range m.Archives {
				if previous.Archives[i].Step != a.Step || previous.Archives[i].Slots != a.Slots {
					return nil, errors.New("comparison retention differs between parts")
				}
			}
		}
		previous.Archives[ai].Points = append(previous.Archives[ai].Points, points...)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	out := make(map[partsconvert.Unit][]partsconvert.StoredMetric)
	for _, m := range metrics {
		for ai := range m.Archives {
			points := m.Archives[ai].Points
			slices.SortFunc(points, func(a, b partsconvert.StoredPoint) int {
				if a.Timestamp < b.Timestamp {
					return -1
				}
				if a.Timestamp > b.Timestamp {
					return 1
				}
				return 0
			})
			n := 0
			for _, p := range points {
				if n > 0 && points[n-1].Timestamp == p.Timestamp {
					if points[n-1].ValueBits != p.ValueBits {
						return nil, fmt.Errorf("conflicting comparison slot for %s", m.Name)
					}
					continue
				}
				points[n] = p
				n++
			}
			m.Archives[ai].Points = points[:n]
		}
		u := partsconvert.Unit{Group: string(m.Group), VShard: partsformat.Route(m.Name)}
		out[u] = append(out[u], *m)
	}
	for u := range out {
		slices.SortFunc(out[u], func(a, b partsconvert.StoredMetric) int { return bytes.Compare(a.Name, b.Name) })
	}
	if len(out) == 0 {
		return nil, errors.New("no real converted slots to compare")
	}
	return out, nil
}
