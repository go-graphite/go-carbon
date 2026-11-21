package v2part

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"unicode/utf8"
)

type StreamFrontier struct {
	Incarnation PartID
	Stream      string
	Cursor      uint64
}

// Manifest describes one coherent placement-group generation. Roots bind the
// external canonical-name catalog, policies and deletion authority; use the
// digest of an explicitly empty inventory rather than a zero/unknown root.
// Its owner must pin all dependencies. This is not a cross-DB transaction.
type Manifest struct {
	Version       uint32
	Group         Group
	Generation    uint64
	CatalogRoot   Digest
	PolicyRoot    Digest
	TombstoneRoot Digest
	Frontiers     []StreamFrontier
	Parts         []Descriptor
}

func scopeBytes(s Scope) []byte {
	v := s.VShard
	if v == GroupVShard {
		v = 0
	}
	b, _ := PartitionPrefixForGroup(s.Group, v, s.Partition)
	if s.VShard == GroupVShard {
		b = append(b, 0)
	} else {
		b = append(b, 1)
	}
	return append(b, s.Part[:]...)
}

func (m Manifest) MarshalBinary() ([]byte, error) {
	if m.Version != SchemaVersion || m.Group == "" || m.Generation == 0 || m.CatalogRoot == (Digest{}) || m.PolicyRoot == (Digest{}) || m.TombstoneRoot == (Digest{}) {
		return nil, errors.New("incomplete or unsupported manifest")
	}
	if !utf8.ValidString(string(m.Group)) {
		return nil, errors.New("invalid UTF-8 manifest identity")
	}
	if _, _, err := GroupRange(m.Group); err != nil {
		return nil, err
	}
	m.Parts = slices.Clone(m.Parts)
	for i := range m.Parts {
		d := &m.Parts[i]
		if err := d.Validate(); err != nil {
			return nil, err
		}
		if d.Scope.Group != m.Group {
			return nil, errors.New("manifest crosses placement groups")
		}
		var err error
		d.Coverage.Policies, err = canonicalPolicies(d.Coverage.Policies)
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(m.Parts, func(a, b Descriptor) int { return bytes.Compare(scopeBytes(a.Scope), scopeBytes(b.Scope)) })
	for i := 1; i < len(m.Parts); i++ {
		if m.Parts[i-1].Scope == m.Parts[i].Scope {
			return nil, errors.New("duplicate part scope")
		}
	}
	type partScope struct {
		Partition int64
		Part      PartID
	}
	groupFiles := map[partScope]bool{}
	for _, d := range m.Parts {
		if d.Scope.VShard == GroupVShard {
			groupFiles[partScope{d.Scope.Partition, d.Scope.Part}] = true
		}
	}
	for _, d := range m.Parts {
		if d.Scope.VShard != GroupVShard && groupFiles[partScope{d.Scope.Partition, d.Scope.Part}] {
			return nil, errors.New("overlapping group and vshard files")
		}
	}
	m.Frontiers = slices.Clone(m.Frontiers)
	slices.SortFunc(m.Frontiers, func(a, b StreamFrontier) int {
		if c := bytes.Compare(a.Incarnation[:], b.Incarnation[:]); c != 0 {
			return c
		}
		return bytes.Compare([]byte(a.Stream), []byte(b.Stream))
	})
	for i, f := range m.Frontiers {
		if f.Incarnation == (PartID{}) || f.Stream == "" || !utf8.ValidString(f.Stream) {
			return nil, errors.New("missing frontier identity")
		}
		if i > 0 && f.Incarnation == m.Frontiers[i-1].Incarnation && f.Stream == m.Frontiers[i-1].Stream {
			return nil, errors.New("duplicate frontier stream")
		}
	}
	b, err := json.Marshal(m)
	if len(b) > MaxManifestBytes {
		return nil, errors.New("manifest too large")
	}
	return b, err
}
func (m Manifest) Digest() (Digest, error) {
	b, err := m.MarshalBinary()
	if err != nil {
		return Digest{}, err
	}
	return sha256.Sum256(append([]byte("go-carbon-v2part-manifest-v1\x00"), b...)), nil
}

const MaxManifestBytes = 64 << 20

func ParseManifest(b []byte) (Manifest, error) {
	var m Manifest
	if len(b) > MaxManifestBytes {
		return m, errors.New("manifest too large")
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	canonical, err := m.MarshalBinary()
	if err != nil {
		return Manifest{}, err
	}
	if !bytes.Equal(b, canonical) {
		return Manifest{}, errors.New("noncanonical manifest")
	}
	return m, nil
}

// Receipt binds a verified physical part AND its coverage claims to a pinned
// manifest and a target storage incarnation's durably installed generation.
// Only the installer may issue/persist it after file+directory sync and atomic
// durable installation. Download completion alone cannot issue a receipt.
// It is transport evidence, not proof that all historical donors are covered.
type Receipt struct {
	Version             uint32
	Manifest            Digest
	Part                Digest // descriptor digest includes SHA256, policies, names and times
	TargetIncarnation   PartID
	InstalledGeneration uint64
}

func (r Receipt) Validate(manifest Digest, d Descriptor) error {
	digest, err := d.Digest()
	if err != nil {
		return err
	}
	if r.Version != SchemaVersion || manifest == (Digest{}) || r.Manifest != manifest || r.Part != digest || r.TargetIncarnation == (PartID{}) || r.InstalledGeneration == 0 {
		return errors.New("receipt identity/coverage mismatch")
	}
	return nil
}

// Covers additionally requires that this exact descriptor belongs to m, and
// that the receipt is for the requested target incarnation/generation.
func (r Receipt) Covers(m Manifest, d Descriptor, target PartID, generation uint64) error {
	root, err := m.Digest()
	if err != nil {
		return err
	}
	if err = r.Validate(root, d); err != nil {
		return err
	}
	if r.TargetIncarnation != target || r.InstalledGeneration != generation {
		return errors.New("receipt target generation mismatch")
	}
	for _, p := range m.Parts {
		digest, e := p.Digest()
		if e != nil {
			return e
		}
		if digest == r.Part {
			return nil
		}
	}
	return errors.New("receipt part absent from manifest")
}
