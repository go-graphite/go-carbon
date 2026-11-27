package v2read

import (
	"context"
	"errors"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

type sealedSource struct{ metric *metricState }

func (s *sealedSource) archive(step int64) int {
	for i, a := range s.metric.policy.Config.Archives {
		if a.Step == step {
			return i
		}
	}
	return -1
}
func (s *sealedSource) HasArchive(ctx context.Context, id uint64, step int64) (bool, error) {
	archive := s.archive(step)
	if archive < 0 || id != s.metric.meta.ID {
		return false, nil
	}
	found := errors.New("archive found")
	err := s.metric.sealed.ScanArchive(ctx, s.metric.name, archive, 0, s.metric.policy.Config.Archives[archive].Count, func(sealrollup.Record) error { return found })
	if errors.Is(err, found) {
		return true, nil
	}
	return false, err
}
func (s *sealedSource) Scan(ctx context.Context, id uint64, a Archive, from, until int64, yield func(Sample) error) error {
	archive := s.archive(a.Step)
	if archive < 0 || id != s.metric.meta.ID {
		return nil
	}
	ranges, n := SlotRanges(a, from, until)
	for _, r := range ranges[:n] {
		err := s.metric.sealed.ScanArchive(ctx, s.metric.name, archive, r.From, r.Until, func(record sealrollup.Record) error {
			return yield(Sample{Time: record.Sample.Time, Bits: record.Sample.Bits, Sequence: record.Order})
		})
		if err != nil {
			return err
		}
	}
	return nil
}
