package v2core

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestCorePipelinedSealCreditOrderAndDurability(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	var orders []uint64
	cfg := coreConfig{Shards: 1, Producers: 1, MaxRows: 16, PartitionSeconds: 3600}
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(_ int, in coreSealInput) (coreSealPlan, error) {
		for _, b := range in.Batches {
			if string(b.Policy) != "policy" || b.Clock != int64(b.Order)-14 {
				return coreSealPlan{}, errors.New("apply metadata changed")
			}
			orders = append(orders, b.Order)
		}
		once.Do(func() { close(entered); <-release })
		return coreSealPlan{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { releaseOnce.Do(func() { close(release) }); _ = e.close() }()
	// One original group, including its detached apply boundary, may be admitted.
	rows := []row{{ID: 7, Time: -1, Bits: 1 << 63, Seq: 1}}
	batch := []coreBatch{{ID: 7, Order: 11, Clock: -3, Policy: []byte("policy"), Points: rows}}
	e.streamBytes = corePacketBytes + 2*uint64(unsafe.Sizeof(row{})) + uint64(unsafe.Sizeof(coreBatch{})) + 6
	if err = e.submitPacketMode(0, rows, batch, 1, true, false); err != nil {
		t.Fatal(err)
	}
	<-entered
	if e.durableFrontiers()[0][0] != 0 {
		t.Fatal("frontier advanced before seal")
	}
	if e.routers[0].bytes.Load() == 0 {
		t.Fatal("seal returned its credit before completion")
	}
	rows[0].Bits = 0
	batch[0].Policy[0] = 'X'
	next := []row{{ID: 7, Time: -1, Bits: 0x7ff8000000000042, Seq: 2}}
	nextBatch := []coreBatch{{ID: 7, Order: 12, Clock: -2, Policy: []byte("policy"), Points: next}}
	done := make(chan error, 1)
	go func() { done <- e.submitPacketMode(0, next, nextBatch, 2, true, false) }()
	deadline := time.Now().Add(5 * time.Second)
	for !e.routers[0].leaseWaiting.Load() {
		if time.Now().After(deadline) {
			t.Fatal("producer did not enter credit wait")
		}
		time.Sleep(time.Microsecond)
	}
	select {
	case err := <-done:
		t.Fatalf("credit did not block: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	// The original synchronous API is a barrier for earlier pipelined seals.
	if err = e.submit(0, nil, 2, true); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(orders, []uint64{11, 12}) {
		t.Fatal(orders)
	}
	if e.durableFrontiers()[0][0] != 2 {
		t.Fatal("barrier missed pipeline")
	}
	snap := e.owners[0].db.NewSnapshot()
	got, err := coreRead(snap, 7)
	snap.Close()
	if err != nil {
		t.Fatal(err)
	}
	want := []row{{ID: 7, Time: -1, Bits: 1 << 63, Seq: 1}, next[0]}
	if !slices.Equal(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestCorePipelinedMatchesSynchronous(t *testing.T) {
	bits := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000042, 0x7ff0000000000001}
	var engines []*coreEngine
	for range 2 {
		e, err := openCore(t.TempDir(), coreConfig{Shards: 3, Producers: 4, MaxRows: 128, PartitionSeconds: 3600, AppendParts: true})
		if err != nil {
			t.Fatal(err)
		}
		engines = append(engines, e)
		defer e.close()
	}
	for round := range 32 {
		for p := range 4 {
			var rows []row
			for name := range 7 {
				rows = append(rows, row{ID: uint64(p*7 + name + 1), Time: int64(round%5 - 2), Bits: bits[(round+name)%len(bits)], Seq: uint64(round + 1)})
			}
			if err := engines[0].Append(p, rows, uint64(round+1), true); err != nil {
				t.Fatal(err)
			}
			if err := engines[1].AppendPipelined(p, rows, uint64(round+1), true); err != nil {
				t.Fatal(err)
			}
			clear(rows)
		}
	}
	for _, e := range engines {
		for p := range 4 {
			if err := e.Append(p, nil, 32, true); err != nil {
				t.Fatal(err)
			}
		}
	}
	for id := uint64(1); id <= 28; id++ {
		var reads [][]row
		for _, e := range engines {
			snap := e.owners[coreShard(id, 3)].db.NewSnapshot()
			rows, err := coreRead(snap, id)
			snap.Close()
			if err != nil {
				t.Fatal(err)
			}
			reads = append(reads, rows)
		}
		if !slices.Equal(reads[0], reads[1]) {
			t.Fatalf("id %d differs: %#v / %#v", id, reads[0], reads[1])
		}
	}
}

func TestCorePipelinedSealFailureIsSticky(t *testing.T) {
	failure := errors.New("injected seal failure")
	entered, release := make(chan struct{}), make(chan struct{})
	e, err := openCoreWithHooks(t.TempDir(), coreConfig{Shards: 1, Producers: 1, MaxRows: 16, PartitionSeconds: 3600}, coreHooks{PlanSeal: func(_ int, _ coreSealInput) (coreSealPlan, error) {
		close(entered)
		<-release
		return coreSealPlan{}, failure
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.AppendPipelined(0, nil, 1, true); err != nil {
		t.Fatal(err)
	}
	<-entered
	close(release)
	// A synchronous barrier observes the same failure without publishing a frontier.
	if err = e.Append(0, nil, 2, true); !errors.Is(err, failure) {
		t.Fatalf("barrier: %v", err)
	}
	if err = e.AppendPipelined(0, nil, 3, true); !errors.Is(err, failure) {
		t.Fatalf("sticky error: %v", err)
	}
	if e.durableFrontiers()[0][0] != 0 {
		t.Fatal("failed seal published frontier")
	}
	if err = e.close(); !errors.Is(err, failure) {
		t.Fatalf("close: %v", err)
	}
}
