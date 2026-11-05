package whisperio

import (
	"context"
	"math"
	"testing"

	store "github.com/go-graphite/go-carbon/internal/chunkstore"
)

func TestColdPackLifecycleCounters(t *testing.T) {
	ctx := context.Background()
	opts := coldOptions(&ContinuationCodec{DirectLateWrites: true, ReuseBlockKeys: true})
	dir := t.TempDir()
	db, err := store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	db.StopBackground()
	coldFixture(t, db)
	coldSweep(t, db, "exact")
	first := db.ColdPackStats()
	if first.Blocks == 0 || first.RepackedBlocks != 0 || first.RepackedPoints != 0 || first.RepackedInputBytes != 0 || first.RepackedOutputBytes != 0 {
		t.Fatalf("first packing counted as repacking: %+v", first)
	}
	before := ColdPackThaws()
	if err := db.UpdateMany(ctx, "cold.metric", []store.Point{{Timestamp: coldBase + 300*60, Value: math.Pi}, {Timestamp: coldBase + 1800*60, Value: 3}}); err != nil {
		t.Fatal(err)
	}
	after := ColdPackThaws()
	if after.Points <= before.Points || after.Bytes <= before.Bytes || after.Appends+after.Patches+after.Rewrites <= before.Appends+before.Patches+before.Rewrites || after.Unpacks != before.Unpacks {
		t.Fatalf("real indexed update did not count writer thaws: before=%+v after=%+v", before, after)
	}
	want, err := db.Snapshot(ctx, "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	// Stop/start the process between thaw and repack: history is durable and
	// cannot depend on process-local counters or a remembered metric name.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	db.StopBackground()
	coldSweep(t, db, "exact")
	repacked := db.ColdPackStats()
	if repacked.RepackedBlocks == 0 || repacked.RepackedBlocks != repacked.Blocks || repacked.RepackedPoints != repacked.Points || repacked.RepackedInputBytes != repacked.InputBytes || repacked.RepackedOutputBytes != repacked.OutputBytes {
		t.Fatalf("previously thawed ranges not classified as repacks: %+v", repacked)
	}
	got, err := db.Snapshot(ctx, "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	before = ColdPackThaws()
	coldSweep(t, db, "unpack")
	after = ColdPackThaws()
	unpacked := db.ColdPackStats()
	if after.Unpacks-before.Unpacks != first.Blocks || after.Appends != before.Appends || after.Patches != before.Patches || after.Rewrites != before.Rewrites || after.Points <= before.Points || after.Bytes <= before.Bytes {
		t.Fatalf("unpack reasons: before=%+v after=%+v first=%+v", before, after, first)
	}
	if unpacked.RepackedBlocks != repacked.RepackedBlocks || unpacked.UnpackedBlocks != after.Unpacks-before.Unpacks || unpacked.UnpackedPoints != after.Points-before.Points {
		t.Fatalf("unpacking counted as repacking: %+v", unpacked)
	}
	coldSweep(t, db, "unpack")
	if after != ColdPackThaws() {
		t.Fatal("ordinary ranges counted again by a second unpack sweep")
	}
	coldSweep(t, db, "exact")
	final := db.ColdPackStats()
	if final.RepackedBlocks-repacked.RepackedBlocks != first.Blocks || final.RepackedPoints-repacked.RepackedPoints != unpacked.UnpackedPoints {
		t.Fatalf("second repack lost history: %+v", final)
	}
}

func TestColdPackRepackFailureCounters(t *testing.T) {
	codec := &coldBarrierCodec{}
	db, err := store.Open(t.TempDir(), coldOptions(codec))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	coldFixture(t, db)
	coldSweep(t, db, "exact")
	coldSweep(t, db, "unpack")
	before := db.ColdPackStats()
	codec.fail = true
	coldSweep(t, db, "exact")
	after := db.ColdPackStats()
	if after.Errors <= before.Errors || after.RepackedBlocks != 0 || after.RepackedPoints != 0 || after.RepackedInputBytes != 0 || after.RepackedOutputBytes != 0 || after.Blocks != before.Blocks {
		t.Fatalf("unpublished repack counted: %+v", after)
	}
	codec.fail = false
	coldSweep(t, db, "exact")
	if got := db.ColdPackStats(); got.RepackedBlocks == 0 {
		t.Fatalf("retry lost thaw marker: %+v", got)
	}
}

func TestThawedBucketTransferAndDowngrade(t *testing.T) {
	ctx := context.Background()
	opts := coldOptions(&ContinuationCodec{})
	db, err := store.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.StopBackground()
	want := coldFixture(t, db)
	coldSweep(t, db, "exact")
	packed := db.ColdPackStats().Blocks
	coldSweep(t, db, "unpack")
	if err := db.DowngradeColdPack(ctx); err != nil {
		t.Fatal(err)
	}
	if db.TransferFormat() != 7 {
		t.Fatal("thaw marker prevented v7 downgrade")
	}
	records, err := db.ExportTransfer(ctx, []string{"cold.metric"})
	if err != nil {
		t.Fatal(err)
	}
	target, err := store.Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	target.StopBackground()
	if _, err := target.ImportTransfer(ctx, records); err != nil {
		t.Fatal(err)
	}
	got, err := target.Snapshot(ctx, "cold.metric")
	if err != nil {
		t.Fatal(err)
	}
	coldEqual(t, want, got, false)
	coldSweep(t, target, "exact")
	if st := target.ColdPackStats(); st.Blocks != packed || st.RepackedBlocks != packed {
		t.Fatalf("transfer lost thaw history: %+v", st)
	}
}
