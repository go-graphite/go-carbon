//go:build linux || darwin

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-graphite/go-carbon/internal/chunkstore"
	"github.com/go-graphite/go-carbon/internal/partsconvert"
	"github.com/go-graphite/go-carbon/internal/partsformat"
	"github.com/go-graphite/go-carbon/internal/whisperio"
)

type copyConversionResult struct {
	OfflineCopy                            bool
	Source, Destination                    string
	Batches, Metrics, Points, SSTBytes     uint64
	DataIOBytes, DataIOCapMBPerSecond      uint64
	LegacySSTBefore, LegacySSTAfter        uint64
	Seconds, ReclaimedLegacyGBPerHour      float64
	StoredSlotsExact                       bool
	MixedRenderQualified, DiskCapQualified bool
}

// convertLegacyCopy is the first concrete consumer of the global batch cursor
// and physical reclaimer. It is deliberately offline: no daemon publication or
// running-store handle is accepted. Pebble's exclusive source LOCK is retained
// except during its own fenced close/reclaim/reopen sequence.
func convertLegacyCopy(ctx context.Context, copyPath, output string, batchMetrics int, maxBatchBytes uint64, options ...copyOptions) (copyConversionResult, error) {
	r := copyConversionResult{OfflineCopy: true}
	if copyPath == "" || output == "" || batchMetrics < 1 || batchMetrics > 4096 || maxBatchBytes == 0 {
		return r, errors.New("invalid offline copy conversion configuration")
	}
	sourcePath, err := filepath.Abs(copyPath)
	if err != nil {
		return r, err
	}
	targetPath, err := filepath.Abs(output)
	if err != nil {
		return r, err
	}
	if sourcePath == targetPath || strings.HasPrefix(targetPath, sourcePath+string(os.PathSeparator)) || strings.HasPrefix(sourcePath, targetPath+string(os.PathSeparator)) {
		return r, errors.New("legacy copy and output must be separate non-nested directories")
	}
	opts := copyOptions{}
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.OriginalAllocated > 0 {
		if err := admitCopyBatch(sourcePath, targetPath, opts.OriginalAllocated, 0); err != nil {
			return r, err
		}
	}
	// Check for a real offline source (and acquire its exclusive lock) before
	// writing any source identity file. Never open the running daemon's DB.
	var source *chunkstore.TransferSource
	openReaders := func() error {
		marker, err := os.ReadFile(filepath.Join(sourcePath, "CHUNKSTORE"))
		if err != nil {
			return err
		}
		opts := chunkstore.TransferSourceOptions{CacheBytes: 8 << 20}
		if !strings.Contains(string(marker), "chunks-v4\n") {
			opts.Continuation = &whisperio.ContinuationCodec{}
		}
		// A copied WALDIR must never send a maintenance open back into a live
		// source's WAL directory. Require a self-contained copy for both opens.
		if data, err := os.ReadFile(filepath.Join(sourcePath, "WALDIR")); err == nil {
			walPath, err := filepath.Abs(strings.TrimSpace(string(data)))
			if err != nil || walPath != sourcePath {
				return errors.New("offline copy has an external WALDIR; copy its WALs locally first")
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		source, err = chunkstore.OpenTransferSource(sourcePath, opts)
		return err
	}
	closeReaders := func() error {
		if source == nil {
			return nil
		}
		err := source.Close()
		source = nil
		return err
	}
	if err := openReaders(); err != nil {
		return r, err
	}
	defer func() { _ = closeReaders() }()
	sourceID, err := durableIdentity(filepath.Join(sourcePath, "CONVERSION-SOURCE"))
	if err != nil {
		return r, err
	}
	if err := os.Mkdir(targetPath, 0750); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return r, err
		}
		if _, err := os.Stat(filepath.Join(targetPath, "CONVERSION-TARGET")); err != nil {
			return r, errors.New("existing output directory has no conversion identity")
		}
	}
	targetID, err := durableIdentity(filepath.Join(targetPath, "CONVERSION-TARGET"))
	if err != nil {
		return r, err
	}
	r.Source, r.Destination = sourceID, targetID
	r.LegacySSTBefore, err = sstBytes(sourcePath)
	if err != nil {
		return r, err
	}
	db, err := pebble.Open(filepath.Join(targetPath, "parts"), &pebble.Options{})
	if err != nil {
		return r, err
	}
	defer db.Close()
	gate := &partsconvert.ReaderGate{}
	reclaimer := partsconvert.LegacyReclaimer{Gate: gate, CloseReaders: closeReaders, ReopenReaders: openReaders, Open: func() (*pebble.DB, error) {
		return chunkstore.OpenLegacyReclaimCopy(sourcePath)
	}, Identity: func(*pebble.DB) (string, error) {
		b, err := os.ReadFile(filepath.Join(sourcePath, "CONVERSION-SOURCE"))
		return strings.TrimSpace(string(b)), err
	}}
	start := time.Now()
	namesDirectory := filepath.Join(targetPath, "names")
	for {
		cursor, pending, err := partsconvert.NameState(namesDirectory)
		if err != nil {
			return r, err
		}
		if cursor.Source != "" && (cursor.Source != sourceID || cursor.Destination != targetID) {
			return r, errors.New("offline copy resume identity mismatch")
		}
		if opts.RangeOrder && pending == nil && !cursor.Done {
			ranges, err := convertIDRanges(ctx, func() *chunkstore.TransferSource { return source }, db, reclaimer, sourcePath, targetPath, sourceID, targetID, batchMetrics, maxBatchBytes, opts)
			r.Batches += ranges.Batches
			r.Metrics += ranges.Metrics
			r.Points += ranges.Points
			r.SSTBytes += ranges.SSTBytes
			if err != nil {
				return r, err
			}
			break
		}
		if cursor.Done && pending == nil {
			break
		}
		var plan partsconvert.LegacyPlan
		if pending != nil {
			plan = *pending
		} else {
			err = source.ConversionView(ctx, func(view pebble.Reader) error {
				it, err := view.NewIter(&pebble.IterOptions{LowerBound: append([]byte("m/"), cursor.Before...), UpperBound: []byte("m0")})
				if err != nil {
					return err
				}
				var upper []byte
				count := 0
				var estimated uint64
				for ok := it.First(); ok; ok = it.Next() {
					if count == batchMetrics {
						upper = append([]byte(nil), it.Key()[2:]...)
						break
					}
					// Plan sparse names together, but stop before the same
					// conservative aggregate byte bound used at admission.
					snapshot, err := source.StoredSnapshot(ctx, string(it.Key()[2:]))
					if err != nil {
						_ = it.Close()
						return err
					}
					n := storedOutputBound(it.Key()[2:], snapshot)
					if n > maxBatchBytes-estimated {
						if count == 0 {
							_ = it.Close()
							return errors.New("offline copy metric exceeds output bound")
						}
						upper = append([]byte(nil), it.Key()[2:]...)
						break
					}
					estimated += n
					count++
				}
				err = errors.Join(it.Error(), it.Close())
				if err == nil {
					plan, err = partsconvert.PlanLegacy(ctx, view, sourceID, []byte("legacy"), cursor.Before, upper, upper == nil, batchMetrics)
				}
				return err
			})
			if err != nil {
				return r, err
			}
		}
		archive := copyArchive(func() *chunkstore.TransferSource { return source }, plan, maxBatchBytes)
		conv := &partsconvert.Converter{Directory: filepath.Join(targetPath, "units"), Source: archive, Target: partsconvert.PebbleTarget{DB: db, DestinationID: targetID}, Space: &partsconvert.SpaceGuard{Path: targetPath}, ChunkBytes: 32 << 20, MaxTables: 100000}
		nc := partsconvert.NameConverter{Directory: namesDirectory, Converter: conv, MaxOutputBytes: maxBatchBytes, Publish: func(_ context.Context, receipt partsconvert.NameReceipt) error {
			// Offline tool has no daemon Reader pointer to swap. Its durable
			// name cursor is the complete publication; the physical adapter still
			// drains pins and closes/reopens the real read-only source handle.
			return nil
		}, Reclaim: reclaimer.Reclaim}
		if opts.OriginalAllocated > 0 {
			if err := admitCopyBatch(sourcePath, targetPath, opts.OriginalAllocated, maxBatchBytes); err != nil {
				return r, err
			}
		}
		receipt, err := nc.Run(ctx, plan)
		if err != nil {
			return r, err
		}
		r.Batches++
		r.Metrics += uint64(len(plan.Metrics))
		for _, unit := range receipt.Units {
			r.Points += unit.Points
			r.SSTBytes += unit.OutputBytes
		}
	}
	r.LegacySSTAfter, err = sstBytes(sourcePath)
	r.Seconds = time.Since(start).Seconds()
	if r.LegacySSTBefore > r.LegacySSTAfter && r.Seconds > 0 {
		r.ReclaimedLegacyGBPerHour = float64(r.LegacySSTBefore-r.LegacySSTAfter) / 1e9 * 3600 / r.Seconds
	}
	r.StoredSlotsExact = err == nil
	return r, err
}

// storedOutputBound charges the same key/envelope/table overhead during name
// planning and per-vshard admission. It counts existing physical slots only.
func storedOutputBound(name []byte, snapshot chunkstore.Snapshot) uint64 {
	var total uint64
	for _, a := range snapshot.Archives {
		blocks, used := uint64(1), 0
		var partition int64
		for i, p := range a.Points {
			q := p.Timestamp / 86400
			if p.Timestamp%86400 < 0 {
				q--
			}
			if i > 0 && (q != partition || used == 4096) {
				blocks++
				used = 0
			}
			partition = q
			used++
		}
		total += 2 * (16*uint64(len(a.Points)) + blocks*uint64(2*len(name)+8*len(snapshot.Archives)+8192))
	}
	return total
}

func durableIdentity(path string) (string, error) {
	if b, err := os.ReadFile(path); err == nil {
		if raw, err := hex.DecodeString(strings.TrimSpace(string(b))); err != nil || len(raw) != 16 {
			return "", errors.New("invalid offline copy identity")
		}
		return strings.TrimSpace(string(b)), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	identity := hex.EncodeToString(token[:])
	temporary := path + ".tmp-" + identity
	f, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer os.Remove(temporary)
	_, err = f.WriteString(identity + "\n")
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return "", err
	}
	if err = os.Link(temporary, path); errors.Is(err, os.ErrExist) {
		return durableIdentity(path)
	} else if err != nil {
		return "", err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	return identity, errors.Join(d.Sync(), d.Close())
}

func writeCopyResult(result copyConversionResult) error {
	return json.NewEncoder(os.Stdout).Encode(result)
}

func copyArchive(source func() *chunkstore.TransferSource, plan partsconvert.LegacyPlan, maxBatchBytes uint64) partsconvert.ArchiveSource {
	// Cache conservative output estimates from actual stored slots. This
	// reads each immutable image once for admission, then once for encoding.
	// Never substitute nominal schema capacity for actual legacy input GB.
	bounds := make(map[uint16]uint64)
	estimate := func(ctx context.Context, u partsconvert.Unit) (uint64, error) {
		if n, ok := bounds[u.VShard]; ok {
			return n, nil
		}
		var total uint64
		for _, m := range plan.Metrics {
			if partsformat.Route(partsformat.ID(m.Name)) != u.VShard {
				continue
			}
			snapshot, err := source().StoredSnapshot(ctx, string(m.Name))
			if err != nil {
				return 0, err
			}
			n := storedOutputBound(m.Name, snapshot)
			if n > maxBatchBytes || total > maxBatchBytes-n {
				return 0, errors.New("offline copy batch exceeds output bound; reduce copy-batch-metrics")
			}
			total += n
		}
		bounds[u.VShard] = total
		return total, nil
	}
	archive := partsconvert.ArchiveSource{SnapshotID: plan.Source, PartitionSeconds: 86400, BlockPoints: 4096, UpperBound: estimate, ScanMetrics: func(ctx context.Context, u partsconvert.Unit, yield func(partsconvert.StoredMetric) error) error {
		for _, m := range plan.Metrics {
			if partsformat.Route(partsformat.ID(m.Name)) != u.VShard {
				continue
			}
			snapshot, err := source().StoredSnapshot(ctx, string(m.Name))
			if err != nil {
				return err
			}
			image := partsconvert.StoredMetric{Name: partsformat.ID(m.Name), Group: partsformat.Group(plan.Group), Method: uint32(snapshot.Metadata.AggregationMethod), XFilesFactorBits: math.Float32bits(snapshot.Metadata.XFilesFactor)}
			for _, a := range snapshot.Archives {
				ar := partsconvert.StoredArchive{Step: uint32(a.Retention.Step), Slots: uint32(a.Retention.Count), Points: make([]partsconvert.StoredPoint, len(a.Points))}
				for i, p := range a.Points {
					ar.Points[i] = partsconvert.StoredPoint{Timestamp: p.Timestamp, ValueBits: math.Float64bits(p.Value)}
				}
				image.Archives = append(image.Archives, ar)
			}
			if err := yield(image); err != nil {
				return err
			}
		}
		return nil
	}}
	return archive
}
