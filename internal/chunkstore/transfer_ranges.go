package chunkstore

import (
	"bytes"
	"context"
	"slices"

	"github.com/cockroachdb/pebble"
)

type transferSpan struct{ start, end []byte }
type transferRanges struct{ spans []transferSpan }

func (r *transferRanges) add(prefix []byte) {
	r.spans = append(r.spans, transferSpan{bytes.Clone(prefix), prefixEnd(prefix)})
}
func (r *transferRanges) apply(b *pebble.Batch) error {
	slices.SortFunc(r.spans, func(a, c transferSpan) int { return bytes.Compare(a.start, c.start) })
	merged := r.spans[:0]
	for _, span := range r.spans {
		if len(merged) > 0 && bytes.Compare(span.start, merged[len(merged)-1].end) <= 0 {
			last := &merged[len(merged)-1]
			if bytes.Compare(span.end, last.end) > 0 {
				last.end = span.end
			}
		} else {
			merged = append(merged, span)
		}
	}
	r.spans = merged
	for _, span := range merged {
		if err := b.DeleteRange(span.start, span.end, nil); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) rangeTransferDelete(ctx context.Context, b *pebble.Batch, m Metadata, ranges *transferRanges) error {
	s.canonicalRun(m.ID).Clear(m.ID)
	if err := b.Delete(revisionKey(m), nil); err != nil {
		return err
	}
	ranges.add(transferReceiptKey(m)[:9])
	ranges.add(transferDeleteKey(m)[:9])
	ranges.add(chunkPrefix(m.ID, 0)[:9])
	if m.Continuation {
		ranges.add(continuationPrefix(m.ID, 0)[:9])
		if dropper, ok := s.continuation.(ContinuationDropper); ok {
			kv := s.newContinuationIO(ctx, s.db, nil, m).withIterator()
			defer kv.Close()
			buckets := &transferBuckets{ContinuationIO: kv}
			if err := dropper.Drop(ctx, buckets); err != nil {
				return err
			}
			for _, bucket := range buckets.prefixes {
				if !isContinuationBucketSuffix(bucket) {
					return ErrFormat
				}
				ranges.add(kv.key(bucket)[:14])
			}
		} else if s.timeBuckets {
			return ErrFormat
		}
	}
	return nil
}
