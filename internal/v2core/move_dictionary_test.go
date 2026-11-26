package v2core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2dict"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// idMovePart deliberately assigns a donor ID far from the receiver's first
// ID, to catch accidental resolution through the receiving store dictionary.
func idMovePart(t *testing.T, part MovePart) MovePart {
	t.Helper()
	d := part.Descriptor
	spec := v2part.Spec{Scope: d.Scope, Source: d.Source, Policies: d.Coverage.Policies, MinClock: d.Coverage.MinClock, MinTime: d.Coverage.MinTime, MaxTime: d.Coverage.MaxTime, MinExpiry: d.Coverage.MinExpiry, MaxExpiry: d.Coverage.MaxExpiry}
	path := filepath.Join(t.TempDir(), "id-donor.sst")
	w, err := v2part.NewDenseWriter(path, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err = w.IDKeyed(10); err != nil {
		t.Fatal(err)
	}
	names := &MoveDictionary{Generation: 10}
	err = v2part.Read(t.Context(), part.Path, &d, func(k v2part.Key, b []byte) error {
		if k.Archive == v2part.MetadataArchive {
			names.Names = append(names.Names, MoveDictionaryName{VShard: k.VShard, Name: string(k.Metric), Entry: v2dict.Entry{ID: 17}})
		}
		k.Metric, k.ByID, k.Local = "", true, 17
		key, e := v2part.Encode(k)
		if e != nil {
			return e
		}
		if k.Archive == v2part.MetadataArchive {
			return w.SetEncoded(key, b)
		}
		return w.SetSlotsEncoded(key, b)
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
	d.VShards = part.Descriptor.VShards
	if err = names.validate(d); err != nil {
		t.Fatal(err)
	}
	if err = VerifyMovePart(context.Background(), path, d, names); err != nil {
		t.Fatal(err)
	}
	return MovePart{Path: path, Descriptor: d, Dictionary: names}
}

func TestMoveIDKeyedVersions(t *testing.T) {
	saved := v2part.IDKeys.Swap(true)
	defer v2part.IDKeys.Store(saved)
	for _, receiverID := range []bool{false, true} {
		for _, donorID := range []bool{false, true} {
			for _, before := range []bool{false, true} {
				for _, mat := range []bool{false, true} {
					t.Run(fmt.Sprintf("receiver-ID=%v/donor-ID=%v/merge-before=%v/mat=%v", receiverID, donorID, before, mat), func(t *testing.T) {
						testMoveAdoptVersionsAcrossMerge(t, before, mat, true, receiverID, donorID)
					})
				}
			}
		}
	}
}

func TestMoveDictionaryValidation(t *testing.T) {
	saved := v2part.IDKeys.Swap(true)
	defer v2part.IDKeys.Store(saved)
	p := idMovePart(t, adoptDonor(t, adoptPolicy(t, sealrollup.Sum), moveSample(1000, 1)))
	for _, which := range []string{"missing", "stale", "wrong-ID", "wrong-shard", "deleted"} {
		t.Run(which, func(t *testing.T) {
			d := *p.Dictionary
			d.Names = slices.Clone(d.Names)
			dict := &d
			switch which {
			case "missing":
				dict = nil
			case "stale":
				d.Generation = p.Descriptor.DictGeneration - 1
			case "wrong-ID":
				d.Names[0].Entry.ID++
			case "wrong-shard":
				d.Names[0].VShard++
			case "deleted":
				d.Names[0].Entry.Flags = v2dict.FlagDeleted
			}
			if err := VerifyMovePart(t.Context(), p.Path, p.Descriptor, dict); err == nil {
				t.Fatal("invalid donor dictionary accepted")
			}
		})
	}
}

func TestMoveIDKeyedSnapshot(t *testing.T) {
	saved := v2part.IDKeys.Swap(true)
	defer v2part.IDKeys.Store(saved)
	cfg := adoptConfig()
	cfg.DeferMerges = true
	donor, err := Open(t.TempDir(), cfg, Hooks{IDKeys: true})
	if err != nil {
		t.Fatal(err)
	}
	defer donor.Close()
	adoptLive(t, donor, adoptPolicy(t, sealrollup.Sum), 1, 1, moveSample(1000, 1), moveSample(1001, 2))
	pin := donor.DBs()[0].NewSnapshot()
	defer pin.Close()
	reader := SealedSnapshotReader{Snapshot: pin, Directory: donor.owners[0].dir, Dictionary: donor.dict}
	files, err := reader.ReadPartFiles(t.Context())
	if err != nil || len(files) != 2 {
		t.Fatalf("files=%d err=%v", len(files), err)
	}
	b, err := os.ReadFile(files[1].Path)
	if err != nil {
		t.Fatal(err)
	}
	d, err := v2part.ParseDescriptor(b)
	if err != nil {
		t.Fatal(err)
	}
	part := MovePart{Path: files[0].Path, Descriptor: d, Dictionary: files[0].Dictionary}
	if part.Dictionary == nil || part.Dictionary.Generation < part.Descriptor.DictGeneration {
		t.Fatal("snapshot lost dictionary generation")
	}
	prepared, err := PrepareVerifiedMove(t.Context(), t.TempDir(), []MovePart{part})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	target, err := Open(t.TempDir(), cfg, Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err = target.AdoptMove(t.Context(), prepared, map[string]uint64{"move.metric": writerID(t, target.identities, 7, adoptPolicy(t, sealrollup.Sum))}, 1010); err != nil {
		t.Fatal(err)
	}
	_, records := adoptedRecords(t, target)
	if len(records) == 0 {
		t.Fatal("copied snapshot has no records")
	}
	reader.Dictionary = nil
	if _, err = reader.ReadPartFiles(t.Context()); err == nil {
		t.Fatal("ID-keyed export without donor dictionary succeeded")
	}
	manifest, err := LinkPartSnapshot(t.Context(), filepath.Join(t.TempDir(), "pinned"), SealedSnapshotReader{Snapshot: pin, Directory: donor.owners[0].dir, Dictionary: donor.dict})
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Parts) != 2 || manifest.Parts[0].Dictionary == nil || manifest.Parts[0].Dictionary.Generation != part.Dictionary.Generation {
		t.Fatal("published snapshot lost donor dictionary")
	}
}
