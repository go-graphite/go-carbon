package chunkstore

import (
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// rangeDeleteSite names a code path that writes Pebble range tombstones.
//
// Every range tombstone committed to the memtable invalidates the fragment
// cache its readers share. The next iterator of any reader then re-fragments
// all of that memtable's tombstones under one sync.Once while every other
// reader waits, so a steady producer of tombstones stalls unrelated reads.
// Per-metric paths therefore use point deletes; each remaining site is
// counted so a new producer shows up in store_range_deletes_total{site}.
type rangeDeleteSite uint8

const (
	rangeDeleteContinuationPrefix rangeDeleteSite = iota
	rangeDeleteReplaceState
	rangeDeleteReplaceChunks
	rangeDeleteNativeIntent
	rangeDeleteNativePublish
	rangeDeleteNativeCombineImport
	rangeDeleteNativeCombineCleanup
	rangeDeleteFileMigration
	rangeDeleteTransferRanges
	rangeDeleteStoreClear
	rangeDeleteSites
)

var rangeDeleteSiteNames = [rangeDeleteSites]string{
	rangeDeleteContinuationPrefix:   "continuation_prefix",
	rangeDeleteReplaceState:         "replace_state",
	rangeDeleteReplaceChunks:        "replace_chunks",
	rangeDeleteNativeIntent:         "native_intent",
	rangeDeleteNativePublish:        "native_publish",
	rangeDeleteNativeCombineImport:  "native_combine_import",
	rangeDeleteNativeCombineCleanup: "native_combine_cleanup",
	rangeDeleteFileMigration:        "file_migration",
	rangeDeleteTransferRanges:       "transfer_ranges",
	rangeDeleteStoreClear:           "store_clear",
}

var rangeDeleteCounts [rangeDeleteSites]atomic.Uint64

// deleteRange adds a range tombstone to b and counts it against site.
func deleteRange(b *pebble.Batch, site rangeDeleteSite, start, end []byte) error {
	if err := b.DeleteRange(start, end, nil); err != nil {
		return err
	}
	rangeDeleteCounts[site].Add(1)
	return nil
}

// RangeDeleteCount is the cumulative, process-wide number of range tombstones
// one site added to store batches.
type RangeDeleteCount struct {
	Site  string
	Count uint64
}

// RangeDeleteCounts reports every site, including those at zero.
func RangeDeleteCounts() []RangeDeleteCount {
	out := make([]RangeDeleteCount, rangeDeleteSites)
	for i := range out {
		out[i] = RangeDeleteCount{Site: rangeDeleteSiteNames[i], Count: rangeDeleteCounts[i].Load()}
	}
	return out
}

func rangeDeleteTotal() uint64 {
	var total uint64
	for i := range rangeDeleteCounts {
		total += rangeDeleteCounts[i].Load()
	}
	return total
}
