package v2core

import (
	"math"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
)

func TestRollupRegisteredPolicyAfterReplay(t *testing.T) {
	_, encoded := rollupTestPolicy(t, "restart.metric", "1s:1h,1m:1d", sealrollup.Average, .5)
	registered, err := NewApplicationPolicy("restart.metric", encoded)
	if err != nil {
		t.Fatal(err)
	}
	_, changed := rollupTestPolicy(t, "restart.metric", "1s:1h,1m:1d", sealrollup.Last, .5)
	// A foreign catalog can allocate the same numeric ID for different bytes.
	// An ID match alone must never authorize the direct routing path.
	wrong, err := (&PolicyCatalog{}).intern(7, "restart.metric", changed)
	if err != nil {
		t.Fatal(err)
	}
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.Rollups = 1, 1, true
	root := t.TempDir()
	core, err := Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	batch := func(order uint64, policy []byte, handle *ApplicationPolicy) Batch {
		return Batch{ID: 7, Order: order, Clock: 1000 + int64(order), Policy: policy, Registered: handle, Points: []Row{{ID: 7, Time: 990 + int64(order), Bits: math.Float64bits(float64(order)), Seq: order}}}
	}
	if err = core.AppendBatches(0, []Batch{batch(1, encoded, nil)}, 1, true); err != nil {
		t.Fatal(err)
	}
	if err = core.Close(); err != nil {
		t.Fatal(err)
	}
	core, err = Open(root, cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	recovered, _, err := recoverCoreRollups(core.DBs()[0])
	if err != nil {
		t.Fatal(err)
	}
	metric := recovered.installed(7)
	if metric == nil || metric.PolicyIdentity == nil {
		t.Fatal("expected recovered metric with shared immutable identity")
	}
	// Journal replay admits canonical bytes before live producer registration.
	if err = core.AppendBatches(0, []Batch{batch(2, encoded, nil)}, 2, true); err != nil {
		t.Fatal(err)
	}
	identity := core.identities.lookup(7)
	if identity == nil {
		t.Fatal("replay did not intern identity")
	}
	if err = core.AppendBatches(0, []Batch{batch(3, nil, wrong)}, 3, true); err == nil {
		t.Fatal("accepted changed live policy after replay")
	}
	if core.identities.lookup(7) != identity {
		t.Fatal("rejected policy replaced shared identity")
	}
	if err = core.AppendBatches(0, []Batch{batch(3, nil, registered)}, 3, true); err != nil {
		t.Fatal(err)
	}
	shared, err := core.RegisterApplicationPolicy(7, "restart.metric", encoded)
	if err != nil || shared != identity {
		t.Fatal("matching live registration did not reuse replay identity", err)
	}
	// The owner must retain recovered state until this matching seal commits.
	in := SealInput{Batches: []Batch{batch(4, encoded, registered)}}
	plan, err := recovered.plan(in)
	if err != nil {
		t.Fatal(err)
	}
	recoveredIdentity := metric.PolicyIdentity
	if recovered.metaOf(7).Frontier.Order != 1 {
		t.Fatal("planning changed recovered frontier")
	}
	plan.Abort()
	if metric.PolicyIdentity != recoveredIdentity || recovered.metaOf(7).Frontier.Order != 1 {
		t.Fatal("aborted seal changed recovered metric")
	}
	if _, err = recovered.plan(SealInput{Batches: []Batch{batch(4, changed, wrong)}}); err == nil {
		t.Fatal("accepted changed policy for recovered metric")
	}
	if metric.PolicyIdentity != recoveredIdentity {
		t.Fatal("failed plan replaced recovered identity")
	}
	plan, err = recovered.plan(in)
	if err != nil {
		t.Fatal(err)
	}
	plan.Commit()
	if metric.PolicyIdentity != recoveredIdentity || recovered.metaOf(7).Frontier.Order != 4 {
		t.Fatal("committed matching seal did not bind recovered metric")
	}
}

func TestCorePlanRejectsTypedRecordInGenericExtension(t *testing.T) {
	record := &sealrollup.Record{}
	for _, file := range [][]coreKV{
		{{Key: []byte{'R', 1}, Value: []byte("ignored"), Record: record}},
		{{Key: []byte{'R', 1}, Value: []byte("first")}, {Key: []byte{'R', 2}, Record: record}},
	} {
		if err := validateCoreSealPlan(coreSealPlan{Files: [][]coreKV{file}}); err == nil {
			t.Fatal("accepted typed record on generic writer path")
		}
	}
}
