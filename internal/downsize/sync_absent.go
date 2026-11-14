package downsize

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
)

// resolveAbsent checks every configured store, including peers whose catalog
// omitted the name. A digest covers only its serving archive: absence requires
// either a native not-found response or a zero count in every native archive.
func (s *syncer) resolveAbsent(ctx context.Context, row *syncRow, names []string, info map[string][]syncArchive) error {
	now := s.o.now().Unix()
	end := now - int64(s.o.settle.Seconds())
	probe := syncRange{From: max(int64(1), end-86400), Until: end}
	target, targetErr := s.digest(ctx, s.o.to, names, probe)
	if targetErr != nil && !errors.Is(targetErr, errSyncNoDigest) {
		return targetErr
	}
	var candidates []string
	for _, n := range names {
		positive := false
		for _, a := range target[n] {
			positive = positive || a.Count > 0
		}
		if _, indexed := info[n]; !indexed || !positive {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	stores := append([]string{s.o.to}, s.o.from...)
	type result struct {
		schemas             map[string][]syncArchive
		positive, uncertain map[string]bool
		err                 error
	}
	results := make([]result, len(stores))
	var wg sync.WaitGroup
	for i, store := range stores {
		wg.Add(1)
		go func(i int, store string) {
			defer wg.Done()
			r := &results[i]
			r.positive, r.uncertain = map[string]bool{}, map[string]bool{}
			got, err := target, targetErr
			if i > 0 {
				got, err = s.digest(ctx, store, candidates, probe)
			}
			if err != nil && !errors.Is(err, errSyncNoDigest) {
				r.err = err
				return
			}
			native := err == nil
			var found []string
			for _, n := range candidates {
				if _, ok := got[n]; ok || !native {
					found = append(found, n)
				}
				for _, a := range got[n] {
					r.positive[n] = r.positive[n] || a.Count > 0
				}
			}
			if len(found) == 0 {
				return
			}
			r.schemas, err = s.b.Info(ctx, store, found)
			if err != nil {
				r.err = err
				return
			}
			groups := map[syncArchive][]string{}
			for _, n := range found {
				if len(r.schemas[n]) == 0 {
					if native {
						r.uncertain[n] = true
					} else {
						groups[syncArchive{}] = append(groups[syncArchive{}], n)
					}
				}
				for _, a := range r.schemas[n] {
					groups[a] = append(groups[a], n)
				}
			}
			for a, ns := range groups {
				from, until := probe.From, probe.Until
				if a.Step > 0 {
					from, until = syncWindow(now, a, s.o.settle)
				}
				if from >= until {
					continue
				}
				if native {
					digests, e := s.digest(ctx, store, ns, syncRange{From: from, Until: until})
					if e != nil {
						r.err = e
						return
					}
					for _, n := range ns {
						matched := false
						for _, d := range digests[n] {
							if d.Step == a.Step {
								matched = true
								r.positive[n] = r.positive[n] || d.Count > 0
							}
						}
						r.uncertain[n] = r.uncertain[n] || !matched
					}
				} else {
					points, e := s.fetch(ctx, store, ns, from, until)
					if e != nil {
						r.err = e
						return
					}
					for n, series := range points {
						if a.Step == 0 || series.Step != a.Step {
							r.uncertain[n] = true
						}
						for _, v := range series.Values {
							if !math.IsNaN(v) && !math.IsInf(v, 0) {
								r.positive[n] = true
								break
							}
						}
					}
				}
			}
		}(i, store)
	}
	wg.Wait()
	for i, r := range results {
		if r.err != nil {
			return fmt.Errorf("absence proof %s: %w", stores[i], r.err)
		}
	}
	for _, n := range candidates {
		positive, uncertain := false, false
		for i, r := range results {
			uncertain = uncertain || r.uncertain[n]
			if !r.positive[n] {
				continue
			}
			positive = true
			if len(info[n]) == 0 {
				info[n] = r.schemas[n]
			}
			if i > 0 {
				mask, _ := s.masks.Load(n)
				var bits uint64
				if mask != nil {
					bits = mask.(uint64)
				}
				s.masks.Store(n, bits|1<<uint(i-1))
			}
		}
		if !positive && uncertain {
			row.Skipped = append(row.Skipped, syncSkip{n, "schema_metadata_unavailable"})
		}
		if !positive && !uncertain {
			row.Resolved = append(row.Resolved, syncSkip{n, "absent_all_sources"})
		}
	}
	return nil
}
