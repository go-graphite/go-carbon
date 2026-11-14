package downsize

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
)

// Frozen window verifier before target detail reads were restricted to the
// union of mismatching names. Compare the complete ordered verdict against it.
func (s *syncer) checkWindowReference(ctx context.Context, row *syncRow, names []string, schemas map[string][]syncArchive, w syncRange) error {
	if s.isDown(s.o.to) {
		return fmt.Errorf("target: %w", errSyncStoreDown)
	}
	type sourceCheck struct {
		source   string
		subset   []string
		digest   map[string][]syncDigest
		err      error
		detail   []string
		points   map[string]syncSeries
		fetchErr error
		coarse   []syncHole
	}
	var checks []*sourceCheck
	for src, source := range s.o.from {
		subset := s.holders(src, names)
		if len(subset) == 0 {
			continue
		}
		if s.isDown(source) {
			for _, n := range subset {
				row.Skipped = append(row.Skipped, syncSkip{n, "source down: " + source})
			}
			continue
		}
		checks = append(checks, &sourceCheck{source: source, subset: subset})
	}
	if len(checks) == 0 {
		return nil
	}
	targetDigest, targetErr := map[string][]syncDigest(nil), errSyncNoDigest
	if !s.noDigest(s.o.to) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			targetDigest, targetErr = s.digest(ctx, s.o.to, names, w)
		}()
		for _, c := range checks {
			if s.noDigest(c.source) {
				c.err = errSyncNoDigest
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.digest, c.err = s.digest(ctx, c.source, c.subset, w)
			}()
		}
		wg.Wait()
	}
	if targetErr != nil && !errors.Is(targetErr, errSyncNoDigest) {
		return fmt.Errorf("target digest: %w", targetErr)
	}
	byDigest := targetErr == nil // otherwise every source is read point by point
	for _, c := range checks {
		source, subset := c.source, c.subset
		detail := subset
		if byDigest {
			switch err := c.err; {
			case err == nil:
				detail = detail[:0:0]
				for _, n := range subset {
					if mismatch, coarse := syncDigestMismatch(c.digest[n], targetDigest[n]); mismatch {
						detail = append(detail, n)
						for _, h := range coarse {
							h.Name, h.Source, h.Window = n, source, w.Name
							h.First, h.Last = w.From, w.Until-1
							c.coarse = append(c.coarse, h)
						}
					}
				}
				s.prog.digests.Add(int64(len(subset)))
			case errors.Is(err, errSyncNoDigest):
			case errors.Is(err, errSyncStoreDown):
				for _, n := range subset {
					row.Skipped = append(row.Skipped, syncSkip{n, "source down: " + source})
				}
				continue
			default:
				return fmt.Errorf("source %s digest: %w", source, err)
			}
		}
		c.detail = detail
	}
	needPoints := false
	for _, c := range checks {
		needPoints = needPoints || len(c.detail) > 0
	}
	if !needPoints {
		return nil
	}
	var targetPoints map[string]syncSeries
	var targetFetchErr error
	var reads sync.WaitGroup
	reads.Add(1)
	go func() {
		defer reads.Done()
		targetPoints, targetFetchErr = s.fetch(ctx, s.o.to, names, w.From, w.Until)
	}()
	for _, c := range checks {
		if len(c.detail) == 0 {
			continue
		}
		s.prog.reads.Add(int64(len(c.detail)))
		reads.Add(1)
		go func() {
			defer reads.Done()
			c.points, c.fetchErr = s.fetch(ctx, c.source, c.detail, w.From, w.Until)
		}()
	}
	reads.Wait()
	if targetFetchErr != nil {
		return fmt.Errorf("target fetch: %w", targetFetchErr)
	}
	for _, c := range checks {
		source, detail, got, err := c.source, c.detail, c.points, c.fetchErr
		row.Holes = append(row.Holes, c.coarse...)
		if errors.Is(err, errSyncStoreDown) {
			for _, n := range detail {
				row.Skipped = append(row.Skipped, syncSkip{n, "source down: " + source})
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("source %s fetch: %w", source, err)
		}
		for _, n := range detail {
			sv, ok := got[n]
			tv := targetPoints[n]
			if !ok || sv.Step <= 0 || (tv.Step != 0 && tv.Step != sv.Step) {
				continue // retention differs: not comparable at one step
			}
			from := (w.From + sv.Step - 1) / sv.Step * sv.Step
			if h, ok := syncCompare(n, source, sv, tv, sv.Step, from, w.Until); ok {
				h.Window = w.Name
				row.Holes = append(row.Holes, h)
			}
		}
	}
	return nil
}

func TestSyncWindowSubsetDifferential(t *testing.T) {
	for _, fallback := range []string{"none", "target", "source"} {
		for _, variant := range []string{"holes", "clean", "bits", "retentions", "missing", "unheld"} {
			t.Run(fallback+"/"+variant, func(t *testing.T) {
				f, o, holes := threeSources(t)
				names := []string{"a.clean", "b.holes", "c.peerhole"}
				switch variant {
				case "clean":
					for _, store := range []string{syncT, syncD1, syncD2, syncP} {
						for _, n := range names {
							f.put(store, n, syncTestSchema)
						}
					}
				case "bits":
					for i, v := range []float64{0, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.MaxFloat64, math.NaN(), math.Inf(1), math.Inf(-1)} {
						ts := holes[0] + int64(i)*60
						f.store(syncT).points["a.clean"][60][ts] = v
						f.store(syncD1).points["a.clean"][60][ts] = float64(i)
					}
				case "retentions":
					f.put(syncP, "c.peerhole", []syncArchive{{Step: 300, Points: 10000}})
				case "missing":
					delete(f.store(syncD1).archives, "a.clean")
				}
				if fallback == "target" {
					f.noDigest[syncT] = true
				}
				if fallback == "source" {
					f.noDigest[syncD1] = true
				}
				s := &syncer{o: o, b: f}
				for _, n := range names {
					s.masks.Store(n, uint64(7))
				}
				if variant == "unheld" {
					s.masks.Store("a.clean", uint64(0))
				}
				w := syncRange{"test", holes[0] - 600, holes[0] + 600}
				var want, got syncRow
				if err := s.checkWindowReference(context.Background(), &want, names, nil, w); err != nil {
					t.Fatal(err)
				}
				if err := s.checkWindow(context.Background(), &got, names, nil, w); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %+v; want %+v", got, want)
				}
			})
		}
	}
}

// Record target detail reads to prove matching neighbors are omitted.
type windowSubsetSync struct {
	*fakeSync
	targetNames []string
}

func (b *windowSubsetSync) Fetch(ctx context.Context, store string, names []string, from, until int64) (map[string]syncSeries, error) {
	if store == syncT {
		b.targetNames = append(b.targetNames, names...)
	}
	return b.fakeSync.Fetch(ctx, store, names, from, until)
}
func TestSyncWindowSubsetReads(t *testing.T) {
	f, o, holes := threeSources(t)
	for _, store := range f.stores {
		for n, a := range store.archives {
			store.archives[n] = a[:1]
		}
	}
	o.from = []string{syncD1, syncD2}
	b := &windowSubsetSync{fakeSync: f}
	s := &syncer{o: o, b: b}
	s.masks.Store("a.clean", uint64(1))
	s.masks.Store("b.holes", uint64(2))
	var row syncRow
	if err := s.checkWindow(context.Background(), &row, []string{"a.clean", "b.holes"}, nil, syncRange{"test", holes[0] - 600, holes[0] + 600}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b.targetNames, []string{"b.holes"}) {
		t.Fatalf("target reads: %v", b.targetNames)
	}
}
