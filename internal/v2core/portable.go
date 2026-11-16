package v2core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// WritePortableRollupPart is the one-way adapter from a current published GCP3
// core seal file to the shared SST/descriptor contract. Keys and values remain
// byte-identical; original source order, occupied NaNs, masks/flags and policy
// metadata are never reconstructed from rendered points. source names the
// original ordering stream; order bounds are derived from the stored values.
//
// This covers the archive/metadata extension file, not the engine's numeric raw
// records, journal or owner frontier. The caller still owns pinning, manifest
// installation, directory sync and receipt publication.
func WritePortableRollupPart(path string, file []KV, source v2part.Provenance) (v2part.Descriptor, error) {
	return writePortableRollupPart(path, file, source, false)
}

// WriteDenseRollupPart writes the same logical records using bounded multi-slot values.
func WriteDenseRollupPart(path string, file []KV, source v2part.Provenance) (v2part.Descriptor, error) {
	return writePortableRollupPart(path, file, source, true)
}

func writePortableRollupPart(path string, file []KV, source v2part.Provenance, dense bool) (v2part.Descriptor, error) {
	if len(file) == 0 {
		return v2part.Descriptor{}, errors.New("empty rollup file")
	}
	first, err := v2part.Decode(file[0].Key)
	if err != nil {
		return v2part.Descriptor{}, err
	}
	spec := v2part.Spec{Scope: v2part.Scope{Group: first.Group, VShard: first.VShard, Partition: first.Partition, Part: first.Part}, Encoding: "core-rollup-v1", Source: source, MinTime: math.MaxInt64, MaxTime: math.MinInt64}
	if dense {
		spec.Scope.VShard = v2part.GroupVShard
	}
	spec.Source.MinOrder = math.MaxUint64
	spec.Source.MaxOrder = 0
	type definition struct {
		policy RollupPolicy
		meta   RollupMetadata
		digest v2part.Digest
		bytes  []byte
	}
	definitions := map[v2part.ID]definition{}
	seen := map[struct {
		archive uint32
		digest  v2part.Digest
	}]bool{}
	for _, kv := range file {
		k, e := v2part.Decode(kv.Key)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		if k.Archive != v2part.MetadataArchive {
			continue
		}
		if k.BlockStart != 0 || k.Ordinal != 0 {
			return v2part.Descriptor{}, errors.New("invalid metadata key")
		}
		var d definition
		if e = json.Unmarshal(kv.Value, &d.meta); e != nil {
			return v2part.Descriptor{}, e
		}
		if d.meta.Version != 1 || d.meta.Frontier.Order == 0 {
			return v2part.Descriptor{}, errors.New("unsupported rollup metadata")
		}
		if e = json.Unmarshal(d.meta.Policy, &d.policy); e != nil {
			return v2part.Descriptor{}, e
		}
		canonical, e := EncodeRollupPolicy(d.policy)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		if !bytes.Equal(canonical, d.meta.Policy) || d.policy.Name != k.Metric || d.policy.Group != k.Group {
			return v2part.Descriptor{}, errors.New("rollup metadata identity mismatch")
		}
		policy, e := sealrollup.PolicyDigest(d.policy.Config)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		if policy != d.meta.Frontier.Policy {
			return v2part.Descriptor{}, errors.New("rollup policy frontier mismatch")
		}
		if _, ok := definitions[k.Metric]; ok {
			return v2part.Descriptor{}, errors.New("duplicate rollup metadata")
		}
		d.bytes, e = json.Marshal(d.policy.Config)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		d.digest = sha256.Sum256(d.bytes)
		definitions[k.Metric] = d
		for archive := range d.policy.Config.Archives {
			id := struct {
				archive uint32
				digest  v2part.Digest
			}{uint32(archive), d.digest}
			if !seen[id] {
				seen[id] = true
				spec.Policies = append(spec.Policies, v2part.ArchivePolicy{Archive: uint32(archive), Digest: d.digest, Definition: d.bytes})
			}
		}
	}
	for _, kv := range file {
		k, e := v2part.Decode(kv.Key)
		if e != nil {
			return v2part.Descriptor{}, e
		}
		d, ok := definitions[k.Metric]
		if !ok {
			return v2part.Descriptor{}, errors.New("rollup record lacks policy metadata")
		}
		t, order := d.meta.Frontier.MaxTime, d.meta.Frontier.Order
		if k.Archive != v2part.MetadataArchive {
			if uint64(k.Archive) >= uint64(len(d.policy.Config.Archives)) || k.Ordinal != 0 || k.BlockStart < 0 || k.BlockStart >= d.policy.Config.Archives[k.Archive].Count {
				return v2part.Descriptor{}, errors.New("rollup archive/slot mismatch")
			}
			r, e := sealrollup.DecodeRecord(int(k.Archive), k.BlockStart, kv.Value)
			if e != nil {
				return v2part.Descriptor{}, e
			}
			if r.Order > d.meta.Frontier.Order {
				return v2part.Descriptor{}, errors.New("rollup order exceeds metadata frontier")
			}
			t, order = r.Sample.Time, r.Order
		}
		spec.MinTime = min(spec.MinTime, t)
		spec.MaxTime = max(spec.MaxTime, t)
		spec.Source.MinOrder = min(spec.Source.MinOrder, order)
		spec.Source.MaxOrder = max(spec.Source.MaxOrder, order)
	}
	var w interface {
		SetEncoded([]byte, []byte) error
		Close() (v2part.Descriptor, error)
		Abort() error
	}
	if dense {
		w, err = v2part.NewDenseWriter(path, spec)
	} else {
		w, err = v2part.NewWriter(path, spec)
	}
	if err != nil {
		return v2part.Descriptor{}, err
	}
	defer w.Abort()
	for _, kv := range file {
		if err = w.SetEncoded(kv.Key, kv.Value); err != nil {
			return v2part.Descriptor{}, err
		}
	}
	return w.Close()
}
