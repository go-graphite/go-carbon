package ringplan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"
)

type comparison struct {
	OldHash        string   `json:"old_hash"`
	NewHash        string   `json:"new_hash"`
	Baseline       string   `json:"baseline"`
	Keep           []string `json:"keep"`
	Metrics        uint64   `json:"metrics"`
	Moved          uint64   `json:"moved"`
	EstimatedBytes uint64   `json:"estimated_bytes"`
	Fraction       float64  `json:"fraction"`
	before         *ring
	after          *ring
}

func compare(ctx context.Context, s specification, old *ring, target, samples, maxCandidates int, timeout time.Duration) ([]comparison, error) {
	if target < 1 || target >= len(s.Old.Nodes) || samples < 1 || maxCandidates < 1 || maxCandidates > 10000 {
		return nil, errors.New("compare requires 1<=target-nodes<N, samples>0 and max-candidates=1..10000")
	}
	if s.Old.Replicas != 1 {
		return nil, errors.New("strategy comparison currently requires one physical replica per site")
	}
	// Enumerate subsets before allocating rings; bound the combinatorial search.
	var subsets [][]member
	var visit func(int, []member) error
	visit = func(start int, picked []member) error {
		if len(picked) == target {
			if len(subsets) >= maxCandidates/3 {
				return fmt.Errorf("more than %d candidate scenarios; raise --max-candidates or narrow membership", maxCandidates)
			}
			subsets = append(subsets, append([]member(nil), picked...))
			return nil
		}
		for i := start; i <= len(s.Old.Nodes)-(target-len(picked)); i++ {
			if err := visit(i+1, append(picked, s.Old.Nodes[i])); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(0, nil); err != nil {
		return nil, err
	}
	hyp := s.Old
	hyp.Hash = "carbon"
	carbonOld, err := newRing(&hyp)
	if err != nil {
		return nil, err
	}
	results := make([]comparison, 0, len(subsets)*3)
	for _, nodes := range subsets {
		keep := make([]string, len(nodes))
		for i, m := range nodes {
			keep[i] = m.ID
		}
		for _, scenario := range []struct {
			before                     *ring
			oldHash, newHash, baseline string
		}{
			{old, s.Old.Hash, s.Old.Hash, "current"},
			{old, s.Old.Hash, "carbon", "current"},
			{carbonOld, "carbon", "carbon", "hypothetical: already carbon"},
		} {
			t := topology{Hash: scenario.newHash, Replicas: 1, VirtualNodes: s.Old.VirtualNodes, Nodes: nodes}
			after, err := newRing(&t)
			if err != nil {
				return nil, err
			}
			results = append(results, comparison{OldHash: scenario.oldHash, NewHash: scenario.newHash, Baseline: scenario.baseline, Keep: keep, before: scenario.before, after: after})
		}
	}
	var scratch [1]int
	count := func(name string, size int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		oldHash := old.compiled.KeyHash(name)
		carbonHash := oldHash
		if s.Old.Hash != "carbon" {
			carbonHash = carbonOld.compiled.KeyHash(name)
		}
		actualOwner := old.compiled.LookupHash(oldHash, scratch[:0])[0]
		hypOwner := carbonOld.compiled.LookupHash(carbonHash, scratch[:0])[0]
		jumpTarget := -1
		if s.Old.Hash == "jump_fnv1a" {
			jumpTarget = results[0].after.compiled.LookupHash(oldHash, scratch[:0])[0]
		}
		for i := range results {
			r := &results[i]
			a := actualOwner
			if r.Baseline != "current" {
				a = hypOwner
			}
			key := oldHash
			if r.NewHash == "carbon" {
				key = carbonHash
			}
			b := jumpTarget
			if r.NewHash != "jump_fnv1a" {
				b = r.after.compiled.LookupHash(key, scratch[:0])[0]
			}
			r.Metrics++
			if r.before.members[a].ID != r.after.members[b].ID {
				r.Moved++
				r.EstimatedBytes += uint64(size)
			}
		}
		return nil
	}
	if len(s.Inputs) == 0 {
		for i := 0; i < samples; i++ {
			name := "aggregations.minutely.by_service.service_" + strconv.Itoa(i%997) + ".by_host.host_" + strconv.Itoa(i%1777) + ".metric_" + strconv.Itoa(i)
			if err := count(name, 600); err != nil {
				return nil, err
			}
		}
	} else {
		for _, in := range s.Inputs {
			inv, err := openInventory(ctx, in, 0, "", timeout)
			if err != nil {
				return nil, err
			}
			for {
				name, size, err := inv.next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					inv.close()
					return nil, err
				}
				owner := old.compiled.Lookup(name, scratch[:0])[0]
				if old.members[owner].ID != in.Source && !s.AllowUnowned {
					inv.close()
					return nil, fmt.Errorf("source %s does not own %q", in.Source, name)
				}
				if err := count(name, size); err != nil {
					inv.close()
					return nil, err
				}
			}
			err = inv.unchanged()
			inv.close()
			if err != nil {
				return nil, err
			}
		}
	}
	for i := range results {
		if results[i].Metrics != 0 {
			results[i].Fraction = float64(results[i].Moved) / float64(results[i].Metrics)
		}
	}
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Baseline != results[j].Baseline {
			return results[i].Baseline < results[j].Baseline
		}
		return results[i].EstimatedBytes < results[j].EstimatedBytes
	})
	return results, nil
}
