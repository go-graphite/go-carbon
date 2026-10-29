package ringplan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
)

// Placement describes an independently started target worker. Its fixed stage
// directory/preflight are checked on that host; credentials never enter a plan.
type Placement struct {
	Worker    string `json:"worker"`
	Directory string `json:"directory"`
	Preflight string `json:"preflight"`
}
type WholeCopy struct {
	Source      string    `json:"source"`
	Destination string    `json:"destination"`
	SourceURL   string    `json:"source_url"`
	TargetURL   string    `json:"target_url"`
	Placement   Placement `json:"placement"`
}
type Manifest = manifest

func identity(m member, hash string) string {
	n, _ := hashing.NewNodeParser(m.Node)
	switch hash {
	case "carbon":
		return n.CarbonKeyValue()
	case "fnv1a":
		return n.FNV1aKeyValue()
	case "jump_fnv1a":
		if n.Instance != "" {
			return n.Instance
		}
	}
	return n.String()
}

// Resize plans hardware replacements and ownership changes together. An exact
// identity-set replacement needs no metric inventory. Mixed changes inventory
// with bounded catalog pages unless immutable input files were supplied.
func Resize(ctx context.Context, path, out string, apply bool, workers int) (Manifest, error) {
	s, old, next, _, err := loadSpec(path)
	if err != nil {
		return manifest{}, err
	}
	for _, ring := range []*topology{&s.Old, &s.New} {
		for i := range ring.Nodes {
			ring.Nodes[i].Endpoint = strings.TrimRight(ring.Nodes[i].Endpoint, "/")
		}
	}
	whole, sameIdentities, err := replacements(s)
	if err != nil {
		return manifest{}, err
	}
	if sameIdentities {
		s.Inputs = nil
		s.PartialInventory = true
	} else if len(s.Inputs) == 0 {
		for _, m := range s.Old.Nodes {
			token := ""
			if os.Getenv("BUCKYD_TOKEN") != "" {
				token = "BUCKYD_TOKEN"
			}
			s.Inputs = append(s.Inputs, input{Source: m.ID, URL: m.Endpoint + "/v1/transfer/list", Format: "transfer", TokenEnv: token})
		}
	}
	blob, err := json.Marshal(s)
	if err != nil {
		return manifest{}, err
	}
	sum := sha256.Sum256(append([]byte("resize-v1:"), blob...))
	id := hex.EncodeToString(sum[:])
	opt := options{Out: out, Apply: apply, Workers: workers, PerSourceWorkers: 1, allowEmpty: sameIdentities, SegmentMetrics: 100000, HTTPTimeout: 30 * time.Minute}
	opt.transform = func(m *manifest) error {
		m.Format = "go-carbon-resize"
		m.Whole = whole
		kept := m.Streams[:0]
		for _, stream := range m.Streams {
			covered := false
			for _, copy := range whole {
				if stream.Source == copy.Source && stream.Destination == copy.Destination {
					covered = true
					break
				}
			}
			if !covered {
				kept = append(kept, stream)
			}
		}
		m.Streams = kept
		return nil
	}
	return plan(ctx, s, old, next, id, opt)
}

type Specification = specification

func ValidateResize(data []byte) error {
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	blob, err := json.Marshal(m.Spec)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(append([]byte("resize-v1:"), blob...))
	if m.Format != "go-carbon-resize" || m.PlanID != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("resize specification digest differs")
	}
	expected, _, err := replacements(m.Spec)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, m.Whole) {
		return fmt.Errorf("replacement jobs differ from specification")
	}
	return nil
}

func replacements(s specification) ([]WholeCopy, bool, error) {
	var whole []WholeCopy
	oldIDs := make(map[string]bool)
	byIdentity := make(map[string]member)
	for _, m := range s.Old.Nodes {
		oldIDs[m.ID] = true
		byIdentity[identity(m, s.Old.Hash)] = m
	}
	sameSettings := s.Old.Hash == s.New.Hash && s.Old.Replicas == s.New.Replicas && s.Old.VirtualNodes == s.New.VirtualNodes
	sameIdentities := sameSettings && len(s.Old.Nodes) == len(s.New.Nodes)
	for _, m := range s.New.Nodes {
		previous, ok := byIdentity[identity(m, s.New.Hash)]
		if !ok {
			sameIdentities = false
		}
		if sameSettings && ok && !oldIDs[m.ID] {
			placement, ok := s.Placements[m.ID]
			if !ok || placement.Worker == "" || placement.Directory == "" || placement.Preflight == "" {
				return nil, false, fmt.Errorf("replacement %s needs worker, directory and preflight placement", m.ID)
			}
			if err := validURL(placement.Worker); err != nil {
				return nil, false, err
			}
			if !filepath.IsAbs(placement.Directory) || !filepath.IsAbs(placement.Preflight) {
				return nil, false, fmt.Errorf("replacement %s requires absolute staging/preflight paths", m.ID)
			}
			for _, source := range s.Old.Nodes {
				if strings.TrimRight(source.Endpoint, "/") == strings.TrimRight(m.Endpoint, "/") {
					return nil, false, fmt.Errorf("replacement target aliases a live source")
				}
			}
			whole = append(whole, WholeCopy{previous.ID, m.ID, previous.Endpoint, m.Endpoint, placement})
		}
	}
	return whole, sameIdentities, nil
}
