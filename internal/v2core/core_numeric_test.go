package v2core

import (
	"bytes"
	"reflect"
	"slices"
	"testing"
)

func TestNumericAPILeavesRawRecordsUnchanged(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.RawCodec = 1, 1, "run1"
	rows := []row{{1, 11, 0x8000000000000000, 1}, {1, 12, 0x7ff8000000000042, 2}, {2, 13, 1, 1}}
	var records [2][]byte
	for path := range 2 {
		e, err := openCore(t.TempDir(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if path == 0 {
			err = e.Append(0, slices.Clone(rows), 1, true)
		} else {
			err = e.AppendNumeric(0, slices.Clone(rows), []coreApply{{ID: 1, Order: 1, Clock: 100, Count: 2}, {ID: 2, Order: 2, Clock: 101, Offset: 2, Count: 1}}, 1, true)
		}
		if err != nil {
			t.Fatal(err)
		}
		it, err := e.owners[0].db.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		for it.First(); it.Valid(); it.Next() {
			records[path] = append(records[path], it.Key()...)
			records[path] = append(records[path], it.Value()...)
		}
		if err = it.Error(); err != nil {
			t.Fatal(err)
		}
		if err = it.Close(); err != nil {
			t.Fatal(err)
		}
		if err = e.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(records[0], records[1]) {
		t.Fatal("numeric metadata changed raw records or frontier")
	}
}

func TestNumericAppliesPreserveBoundariesAndDetach(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers, cfg.MaxRows = 3, 1, 32
	type observed struct {
		apply coreApply
		rows  []row
	}
	var got [3][]observed
	e, err := openCoreWithHooks(t.TempDir(), cfg, coreHooks{PlanSeal: func(owner int, in coreSealInput) (coreSealPlan, error) {
		for _, a := range in.Applies {
			got[owner] = append(got[owner], observed{a, slices.Clone(in.Rows[a.Offset : a.Offset+a.Count])})
		}
		return coreSealPlan{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	rows := []row{{1, -1, 0x8000000000000000, 1}, {1, 9, 0x7ff8000000000042, 2}, {2, 8, 1, 1}, {1, 9, 0x7ff0000000000000, 1}}
	apps := []coreApply{{ID: 1, Order: 10, Clock: 101, Offset: 0, Count: 2}, {ID: 2, Order: 11, Clock: 102, Offset: 2, Count: 1}, {ID: 1, Order: 12, Clock: 103, Offset: 3, Count: 1}}
	wantRows, wantApps := slices.Clone(rows), slices.Clone(apps)
	if err = e.AppendNumeric(0, rows, apps, 7, false); err != nil {
		t.Fatal(err)
	}
	clear(rows)
	clear(apps)
	if err = e.AppendNumeric(0, nil, nil, 7, true); err != nil {
		t.Fatal(err)
	}
	for _, want := range wantApps {
		owner := coreShard(want.ID, cfg.Shards)
		if len(got[owner]) == 0 {
			t.Fatal("lost apply", want)
		}
		v := got[owner][0]
		got[owner] = got[owner][1:]
		if v.apply.ID != want.ID || v.apply.Order != want.Order || v.apply.Clock != want.Clock || v.apply.Cursor != 7 || v.apply.Producer != 0 || !reflect.DeepEqual(v.rows, wantRows[want.Offset:want.Offset+want.Count]) {
			t.Fatalf("changed original apply: %+v want%+v", v, want)
		}
	}
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNumericApplyValidation(t *testing.T) {
	cfg := coreTestConfig()
	cfg.Shards, cfg.Producers = 1, 1
	e, err := openCore(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, a := range [][]coreApply{nil, {{ID: 1, Order: 1, Count: 0}}, {{ID: 1, Order: 1, Offset: 1, Count: 1}}, {{ID: 2, Order: 1, Count: 1}}, {{ID: 1, Count: 1}}, {{ID: 1, Order: 1, Count: 2}}} {
		if err = e.AppendNumeric(0, []row{{1, 1, 1, 1}}, a, 1, false); err == nil {
			t.Fatal("accepted invalid spans", a)
		}
	}
}
