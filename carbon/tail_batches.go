package carbon

import (
	"io"
	"sync"

	"github.com/go-graphite/go-carbon/points"
	"github.com/zeebo/xxh3"
)

type tailRecord struct{ name, length, first, count int }
type tailBatch struct {
	names   []byte
	data    []points.Point
	records []tailRecord
}

// Borrowed decoding and recyclable arenas keep the serial reader free of
// per-point heap objects. Owned canonical names stay valid if cache retains a
// name; the small per-worker memo is bounded independently of tail cardinality.
// add borrows the record and point slice until it returns.
func replayInputTailBatches(reader io.Reader, workers int, add func(*points.Points)) (int, error) {
	queues := make([]chan *tailBatch, workers)
	free := make([]chan *tailBatch, workers)
	batches := make([]*tailBatch, workers)
	var wg sync.WaitGroup
	next := func(i int) *tailBatch {
		select {
		case b := <-free[i]:
			b.names, b.data, b.records = b.names[:0], b.data[:0], b.records[:0]
			return b
		default:
			return &tailBatch{names: make([]byte, 0, 32<<10), data: make([]points.Point, 0, 128), records: make([]tailRecord, 0, 128)}
		}
	}
	for i := range queues {
		queue := make(chan *tailBatch, 2)
		available := make(chan *tailBatch, 4)
		queues[i], free[i] = queue, available
		wg.Go(func() {
			memo := make(map[string]string)
			retained := 0
			var p points.Points
			for b := range queue {
				for _, r := range b.records {
					name := b.names[r.name : r.name+r.length]
					canonical, ok := memo[string(name)]
					if !ok {
						if retained+len(name) > 4<<20 || len(memo) >= 4096 {
							clear(memo)
							retained = 0
						}
						canonical = string(name)
						memo[canonical] = canonical
						retained += len(canonical)
					}
					p.Metric, p.Data = canonical, b.data[r.first:r.first+r.count]
					add(&p)
				}
				// Do not keep an unusually large legacy record's arena.
				if cap(b.names) <= 1<<20 && cap(b.data) <= 16384 {
					select {
					case available <- b:
					default:
					}
				}
			}
		})
	}
	count := 0
	err := points.ReadBinaryBorrowed(reader, func(name []byte, data []points.Point) {
		count += len(data)
		i := int(xxh3.Hash(name) % uint64(workers))
		b := batches[i]
		if b == nil {
			b = next(i)
			batches[i] = b
		}
		b.records = append(b.records, tailRecord{len(b.names), len(name), len(b.data), len(data)})
		b.names = append(b.names, name...)
		b.data = append(b.data, data...)
		if len(b.data) >= 128 || len(b.names) >= 64<<10 {
			queues[i] <- b
			batches[i] = nil
		}
	})
	for i, q := range queues {
		if b := batches[i]; b != nil {
			q <- b
		}
		close(q)
	}
	wg.Wait()
	return count, err
}
