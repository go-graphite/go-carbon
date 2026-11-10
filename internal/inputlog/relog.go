package inputlog

import (
	"errors"
	"hash/crc32"
	"time"

	"github.com/go-graphite/go-carbon/points"
)

// SnapshotSource visits a stable cache shard while persistence is stopped.
// It replaces that shard's staging and tracking only after emit succeeds.
// Newer input may continue on other shards.
type SnapshotSource interface {
	SnapshotLogShard(shard int, gen uint64, emit func(*points.Points) error) error
}

// Relog rewrites every unpersisted point using the existing journal format.
// Unlike age-based RelogBefore it streams large items without bounded staging
// losses. Persistence must be stopped and no external recovery source may be
// pending. Input can continue; the group committer serializes the boundary.
func (l *Log) Relog() error {
	_, err := l.requestCopy("relog")
	return err
}

func (l *Log) relogCache() error {
	source, ok := l.src.(SnapshotSource)
	if !ok || l.copyPin != nil {
		return errors.New("input log snapshot requires a cache source without copy retention")
	}
	if err := l.cycle(); err != nil {
		return err
	}
	// Start a new segment so dictionary reads do not scan the old history.
	if err := l.roll(); err != nil {
		return err
	}
	l.steady = 0
	from := position{l.active.seq, l.active.end}
	gen := l.gen
	l.gen++
	for p := range l.parts {
		l.parts[p].out, l.parts[p].frames = l.parts[p].out[:0], l.parts[p].frames[:0]
	}
	w := &l.workers[0]
	for shard := range l.shardsPer * partitions {
		p := shard / l.shardsPer
		part := &l.parts[p]
		seq := l.target()
		part.enc.begin(seq)
		flush := func() error {
			w.raw = part.enc.flush(&w.streamScratch, w.raw, func(raw []byte, rows int) []byte {
				l.compress(part, p, raw, rows)
				return raw
			})
			if len(part.frames) == 0 {
				return nil
			}
			err := l.write(gen, seq)
			part.out, part.frames = part.out[:0], part.frames[:0]
			seq = l.target()
			part.enc.begin(seq)
			return err
		}
		if err := source.SnapshotLogShard(shard, gen+1, func(batch *points.Points) error {
			ref := NewStreamRef(batch.Metric)
			for _, point := range batch.Data {
				part.enc.add(&w.streamScratch, ref.Entry(point.Value, point.Timestamp))
				if len(w.rows) >= maxSortRows || len(w.names) >= 2*maxFrameRaw {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if err := flush(); err != nil {
			return err
		}
	}
	// All snapshot frames are synced. Publish their start in a zero-row legacy
	// frame. A crash before this frame retains the earlier replay boundary;
	// afterward only the snapshot and subsequent live input need replaying.
	l.ticks = []tick{{gen: gen, pos: from, at: time.Now()}}
	l.lowWaters = nil
	l.durable = gen
	part := &l.parts[0]
	part.out = l.enc.EncodeAll(nil, part.out[:0])
	part.frames = append(part.frames[:0], frameMeta{h: frameHeader{
		codec: codecRecords, length: uint32(len(part.out)), crc: crc32.Checksum(part.out, castagnoli),
	}})
	err := l.write(gen, l.target())
	part.out, part.frames = part.out[:0], part.frames[:0]
	return err
}
