package main

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

type windowEdge struct {
	Block             string
	Target, Reference int
	Windows           []int
}
type windowJoinSpec struct {
	Inputs []digestInput
	Edges  []windowEdge
}

func windowRow(row *digestRow, id int) (*digestRow, error) {
	if row == nil {
		return nil, nil
	}
	if !row.WindowsComplete {
		return nil, errors.New("complete window census required")
	}
	if scoped := row.Windows[strconv.Itoa(id)]; scoped != nil {
		return scoped, nil
	}
	return &digestRow{Archives: row.Archives}, nil
}

// All survivors are streamed once. The window edges choose each block's replica;
// history buckets are skipped by the JSON decoder in this mode.
func joinWindowDigests(spec windowJoinSpec, output io.Writer) (map[string]digestJoinCounts, error) {
	counts := make(map[string]digestJoinCounts)
	if len(spec.Inputs) < 2 || len(spec.Edges) == 0 {
		return counts, errors.New("window join needs explicit inputs and edges")
	}
	for _, edge := range spec.Edges {
		if edge.Block == "" || edge.Target < 0 || edge.Reference < 0 || edge.Target >= len(spec.Inputs) || edge.Reference >= len(spec.Inputs) || edge.Target == edge.Reference || len(edge.Windows) == 0 {
			return counts, errors.New("invalid replica window edge")
		}
		for _, id := range edge.Windows {
			a, b := spec.Inputs[edge.Target].Windows, spec.Inputs[edge.Reference].Windows
			if id < 0 || id >= len(a) || id >= len(b) {
				return counts, errors.New("window was not scanned by both inputs")
			}
			if a[id] != b[id] || a[id].From < 0 || a[id].Until <= a[id].From {
				return counts, errors.New("replica window definitions differ or are invalid")
			}
		}
	}
	cursors := []*digestCursor{}
	defer func() {
		for _, c := range cursors {
			c.file.Close()
		}
	}()
	for _, input := range spec.Inputs {
		c, err := openDigest(input)
		if err != nil {
			return counts, err
		}
		c.windowsOnly = true
		cursors = append(cursors, c)
		if err = c.next(); err != nil {
			return counts, err
		}
	}
	enc := json.NewEncoder(output)
	for {
		name := ""
		for _, c := range cursors {
			if !c.eof && (name == "" || c.name < name) {
				name = c.name
			}
		}
		if name == "" {
			break
		}
		rows := make([]*digestRow, len(cursors))
		for i, c := range cursors {
			if !c.eof && c.name == name {
				rows[i] = c.row
			}
		}
		for _, edge := range spec.Edges {
			// A reference-only name is still checked, and therefore detects a missing
			// survivor name with finite replica points in a restart window.
			if rows[edge.Reference] == nil {
				continue
			}
			count := counts[edge.Block]
			count.DonorNames++
			if rows[edge.Target] == nil {
				count.UnavailableSurvivor++
			}
			if count.ByWindow == nil {
				count.ByWindow = make(map[int]*digestJoinCounts)
			}
			for _, id := range edge.Windows {
				source, err := windowRow(rows[edge.Reference], id)
				if err != nil {
					return counts, err
				}
				target, err := windowRow(rows[edge.Target], id)
				if err != nil {
					return counts, err
				}
				windowCount := count.ByWindow[id]
				if windowCount == nil {
					windowCount = &digestJoinCounts{}
					count.ByWindow[id] = windowCount
				}
				windowCount.DonorNames++
				if target == nil {
					windowCount.UnavailableSurvivor++
				}
				before := *windowCount
				wanted, err := unequalBuckets(source, target, windowCount)
				if err != nil {
					return counts, err
				}
				count.Equal += windowCount.Equal - before.Equal
				count.Drilled += windowCount.Drilled - before.Drilled
				count.PolicyMismatch += windowCount.PolicyMismatch - before.PolicyMismatch
				if len(wanted) > 0 {
					if err = enc.Encode(map[string]any{"block": edge.Block, "window": id, "name": name, "donor": spec.Inputs[edge.Reference].Host, "survivor": spec.Inputs[edge.Target].Host, "buckets": wanted, "survivor_native_present": target != nil}); err != nil {
						return counts, err
					}
				}
			}
			counts[edge.Block] = count
		}
		for _, c := range cursors {
			if !c.eof && c.name == name {
				if err := c.next(); err != nil {
					return counts, err
				}
			}
		}
	}
	return counts, nil
}
