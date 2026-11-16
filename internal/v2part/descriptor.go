package v2part

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"unicode/utf8"
)

type Digest = [32]byte

// ArchivePolicy carries the exact immutable schema/aggregation/encoding bytes.
// Multiple policies for the same archive are allowed in one part. Definitions
// use the producer's versioned Encoding, never a host-local catalogue ordinal.
type ArchivePolicy struct {
	Archive    uint32
	Digest     Digest
	Definition []byte
}

// Provenance identifies the original source incarnation and serial ordering
// stream, not the current location. Orders are inclusive producer assertions.
type Provenance struct {
	Incarnation PartID
	Stream      string
	MinOrder    uint64
	MaxOrder    uint64
}

type Coverage struct {
	MinTime  int64
	MaxTime  int64 // inclusive event time, NOT GCP3's physical BlockStart
	Names    uint64
	Records  uint64
	Policies []ArchivePolicy
}

// Spec is supplied before a sorted stream is written. Coverage times and policy
// definitions must be derived from authoritative stored records by the producer;
// the generic SST layer treats values as opaque and never reinterprets history.
type Spec struct {
	Scope    Scope
	Encoding string
	Source   Provenance
	MinTime  int64
	MaxTime  int64
	Policies []ArchivePolicy
}

type Descriptor struct {
	Version    uint32
	KeyVersion uint32
	Scope      Scope
	Encoding   string
	Source     Provenance
	Bytes      uint64
	SHA256     Digest
	FirstKey   []byte
	LastKey    []byte
	Coverage   Coverage
}

func validateSpec(s Spec) error {
	if s.Scope.Group == "" || s.Scope.Part == (PartID{}) || s.Source.Incarnation == (PartID{}) || s.Source.Stream == "" || s.Encoding == "" {
		return errors.New("part identity, encoding and source required")
	}
	if !utf8.ValidString(string(s.Scope.Group)) || !utf8.ValidString(s.Encoding) || !utf8.ValidString(s.Source.Stream) {
		return errors.New("invalid UTF-8 descriptor identity")
	}
	if _, err := PartitionPrefixForGroup(s.Scope.Group, s.Scope.VShard, s.Scope.Partition); err != nil {
		return err
	}
	if s.MinTime > s.MaxTime || s.Source.MinOrder > s.Source.MaxOrder {
		return errors.New("inverted part coverage")
	}
	for _, p := range s.Policies {
		if p.Archive == MetadataArchive || len(p.Definition) == 0 || sha256.Sum256(p.Definition) != p.Digest {
			return errors.New("invalid archive policy")
		}
	}
	if _, err := canonicalPolicies(s.Policies); err != nil {
		return err
	}
	return nil
}

func (d Descriptor) Validate() error {
	if d.Version != SchemaVersion || d.KeyVersion != KeyVersion {
		return errors.New("unsupported part schema/key version")
	}
	if err := validateSpec(d.spec()); err != nil {
		return err
	}
	if d.Bytes == 0 || d.SHA256 == (Digest{}) || d.Coverage.Names == 0 || d.Coverage.Records < d.Coverage.Names {
		return errors.New("empty part descriptor")
	}
	first, err := Decode(d.FirstKey)
	if err != nil {
		return err
	}
	last, err := Decode(d.LastKey)
	if err != nil {
		return err
	}
	if !d.Scope.contains(first) || !d.Scope.contains(last) || bytes.Compare(d.FirstKey, d.LastKey) > 0 {
		return errors.New("part key range outside scope")
	}
	return nil
}

func (d Descriptor) spec() Spec {
	return Spec{Scope: d.Scope, Encoding: d.Encoding, Source: d.Source, MinTime: d.Coverage.MinTime, MaxTime: d.Coverage.MaxTime, Policies: d.Coverage.Policies}
}

func canonicalPolicies(in []ArchivePolicy) ([]ArchivePolicy, error) {
	out := slices.Clone(in)
	slices.SortFunc(out, func(a, b ArchivePolicy) int {
		if a.Archive < b.Archive {
			return -1
		}
		if a.Archive > b.Archive {
			return 1
		}
		return bytes.Compare(a.Digest[:], b.Digest[:])
	})
	for i := range out {
		out[i].Definition = bytes.Clone(out[i].Definition)
		if i > 0 && out[i].Archive == out[i-1].Archive && out[i].Digest == out[i-1].Digest {
			return nil, errors.New("duplicate archive policy")
		}
	}
	return out, nil
}

// MarshalBinary is canonical JSON of a versioned fixed-field struct. Byte arrays
// are JSON integer arrays and byte slices are base64, as in encoding/json.
// No maps, floating point, whitespace or unknown fields enter the digest.
func (d Descriptor) MarshalBinary() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	policies, err := canonicalPolicies(d.Coverage.Policies)
	if err != nil {
		return nil, err
	}
	d.Coverage.Policies = policies
	b, err := json.Marshal(d)
	if len(b) > MaxDescriptorBytes {
		return nil, errors.New("descriptor too large")
	}
	return b, err
}
func (d Descriptor) Digest() (Digest, error) {
	b, err := d.MarshalBinary()
	if err != nil {
		return Digest{}, err
	}
	return sha256.Sum256(append([]byte("go-carbon-v2part-descriptor-v1\x00"), b...)), nil
}

const MaxDescriptorBytes = 16 << 20

// ParseDescriptor accepts only the canonical representation, rejecting unknown
// versions, extra/trailing fields, duplicate fields and noncanonical encodings.
func ParseDescriptor(b []byte) (Descriptor, error) {
	var d Descriptor
	if len(b) > MaxDescriptorBytes {
		return d, errors.New("descriptor too large")
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return d, err
	}
	canonical, err := d.MarshalBinary()
	if err != nil {
		return Descriptor{}, err
	}
	if !bytes.Equal(b, canonical) {
		return Descriptor{}, errors.New("noncanonical part descriptor")
	}
	return d, nil
}
