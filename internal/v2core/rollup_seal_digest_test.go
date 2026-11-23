package v2core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/go-graphite/go-carbon/internal/sealrollup"
	"github.com/go-graphite/go-carbon/internal/v2part"
)

// sealPlanDigest hashes every planned key, value, record and part spec.
func sealPlanDigest(plan coreSealPlan) string { return sealPlanDigestMode(plan, false) }

// sealPlanDecodedDigest hashes metadata rows as the full encoding of what they
// decode to, so it is independent of whether envelopes store the definition.
func sealPlanDecodedDigest(plan coreSealPlan) string { return sealPlanDigestMode(plan, true) }

func sealPlanDigestMode(plan coreSealPlan, decoded bool) string {
	var h io.Writer = sha256.New()
	sum := h.(hash.Hash)
	if dump := os.Getenv("SEAL_DIGEST_DUMP"); dump != "" {
		f, _ := os.OpenFile(dump, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		defer f.Close()
		h = io.MultiWriter(h, f)
	}
	// The part UUID is random per seal: mask it in keys and specs.
	for i, file := range plan.Files {
		part := plan.PartSpecs[i].Scope.Part
		// The goldens cover what per-slot readers resolve: a slot's
		// superseded versions (followed by a record of the same key) and the
		// record kind are outside them; TestRollupHistoryDifferential and
		// TestDenseRecordVersions cover those.
		superseded := func(j int) bool {
			return file[j].Record != nil && j+1 < len(file) && bytes.Equal(file[j].Key, file[j+1].Key)
		}
		n := 0
		for j := range file {
			if !superseded(j) {
				n++
			}
		}
		fmt.Fprintf(h, "file %d %d\n", i, n)
		for j, kv := range file {
			if superseded(j) {
				continue
			}
			value := kv.Value
			if decoded && kv.Record == nil {
				k, err := v2part.Decode(kv.Key)
				if err != nil {
					panic(err)
				}
				m, err := DecodeRollupMetadata(k.Metric, k.Group, value)
				if err != nil {
					panic(err)
				}
				value = AppendRollupMetadata(nil, m)
			}
			fmt.Fprintf(h, "%x %x", bytes.Replace(kv.Key, part[:], make([]byte, len(part)), 1), value)
			if r := kv.Record; r != nil {
				fmt.Fprintf(h, " %+v", struct {
					sealrollup.Delta
					Correction bool
				}{r.Delta, r.Correction})
			}
			h.Write([]byte{'\n'})
		}
	}
	for _, spec := range plan.PartSpecs {
		spec.Scope.Part = v2part.PartID{}
		// Parallel lanes merge policies in map order (pre-existing); compare as a set.
		spec.Policies = slices.Clone(spec.Policies)
		slices.SortFunc(spec.Policies, func(a, b v2part.ArchivePolicy) int {
			if c := bytes.Compare(a.Digest[:], b.Digest[:]); c != 0 {
				return c
			}
			return int(a.Archive) - int(b.Archive)
		})
		// Preserve the pre-admission-coverage golden surface. MinClock is
		// independently checked by TestMergeAdmissionBound; no rows are masked.
		fmt.Fprint(h, withoutSeq(fmt.Sprintf("spec {Scope:%+v Encoding:%s Source:%+v MinTime:%d MaxTime:%d Policies:%+v MinExpiry:%d MaxExpiry:%d}\n",
			spec.Scope, spec.Encoding, spec.Source, spec.MinTime, spec.MaxTime, spec.Policies, spec.MinExpiry, spec.MaxExpiry)))
	}
	return hex.EncodeToString(sum.Sum(nil))
}

// withoutSeq drops a zero Provenance.Seq from a %+v dump, so digests recorded
// before the field existed still cover every other field byte for byte.
func withoutSeq(s string) string { return strings.Replace(s, " Seq:0}", "}", 1) }

func sealPlanDigestJoin(d []string) string {
	h := sha256.New()
	for _, s := range d {
		h.Write([]byte(s))
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
