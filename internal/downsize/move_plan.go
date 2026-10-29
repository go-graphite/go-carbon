package downsize

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/go-graphite/go-carbon/internal/downsize/ringplan"
)

type member struct{ ID, Node, Endpoint string }
type topology struct {
	Hash     string
	Replicas int
	Nodes    []member
}
type specification struct {
	Placements    map[string]ringplan.Placement `json:"placements,omitempty"`
	Version       int
	Cluster, Site string
	Old, New      topology
	AllowUnowned  bool `json:"allow_unowned"`
}
type route struct {
	Path, Source, Destination string
	Metrics                   int64
	EstimatedBytes            int64 `json:"estimated_bytes"`
	SHA256                    string
	sourceURL, targetURL      string
}
type manifest struct {
	Whole   []ringplan.WholeCopy `json:"whole,omitempty"`
	Format  string
	Version int
	PlanID  string `json:"plan_id"`
	Spec    specification
	Streams []route
	root    string
}
type planRow struct {
	Type, Format                string
	Version                     int
	PlanID                      string `json:"plan_id"`
	Source, Destination, Metric string
	Metrics                     int64
	EstimatedBytes              int64 `json:"estimated_bytes"`
}

func readPlan(dir string) (*manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("committed manifest required: %w", err)
	}
	var p manifest
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	if p.Format != "go-carbon-ring" && p.Format != "go-carbon-resize" || p.Version != 1 || p.Spec.Version != 1 || len(p.PlanID) != 64 {
		return nil, errors.New("unsupported plan format/version/id")
	}
	if _, err := hex.DecodeString(p.PlanID); err != nil {
		return nil, err
	}
	if p.Format == "go-carbon-resize" {
		if err := ringplan.ValidateResize(data); err != nil {
			return nil, err
		}
	}
	p.root, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	p.root, err = filepath.Abs(p.root)
	if err != nil {
		return nil, err
	}
	oldIDs, newIDs := map[string]bool{}, map[string]bool{}
	for _, m := range p.Spec.Old.Nodes {
		oldIDs[m.ID] = true
	}
	for _, m := range p.Spec.New.Nodes {
		newIDs[m.ID] = true
	}
	if p.Spec.Cluster == "" || p.Spec.Site == "" || len(oldIDs) == 0 || len(newIDs) == 0 {
		return nil, errors.New("cluster, site and both topologies are required")
	}
	for _, ring := range []topology{p.Spec.Old, p.Spec.New} {
		switch ring.Hash {
		case "", "carbon", "carbon_ch", "fnv1a", "fnv1a_ch", "jump_fnv1a", "jump_fnv1a_ch":
		default:
			return nil, errors.New("unsupported ingestion hash")
		}
		if ring.Replicas < 0 || ring.Replicas > len(ring.Nodes) {
			return nil, errors.New("invalid physical replica count")
		}
		seen := map[string]bool{}
		for _, m := range ring.Nodes {
			if seen[m.ID] {
				return nil, errors.New("duplicate topology member")
			}
			seen[m.ID] = true
		}
	}
	nodes := map[string]string{}
	for _, ring := range []topology{p.Spec.Old, p.Spec.New} {
		for _, m := range ring.Nodes {
			if m.ID == "" {
				return nil, errors.New("missing physical member ID")
			}
			endpoint, err := origin(m.Endpoint)
			if err != nil {
				return nil, err
			}
			if old, ok := nodes[m.ID]; ok && old != endpoint {
				return nil, errors.New("physical member changes endpoint")
			}
			nodes[m.ID] = endpoint
		}
	}
	wholeTargets := map[string]bool{}
	for _, job := range p.Whole {
		if !oldIDs[job.Source] || !newIDs[job.Destination] || job.Source == job.Destination || wholeTargets[job.Destination] || nodes[job.Source] != job.SourceURL || nodes[job.Destination] != job.TargetURL {
			return nil, errors.New("invalid whole-store replacement")
		}
		if _, err := origin(job.Placement.Worker); err != nil {
			return nil, err
		}
		if !filepath.IsAbs(job.Placement.Directory) || !filepath.IsAbs(job.Placement.Preflight) {
			return nil, errors.New("replacement requires absolute worker staging/preflight paths")
		}
		wholeTargets[job.Destination] = true
	}
	paths := map[string]bool{}
	hashes := map[string]bool{}
	for i := range p.Streams {
		r := &p.Streams[i]
		if !oldIDs[r.Source] || !newIDs[r.Destination] || r.Source == r.Destination || r.Metrics < 1 || r.EstimatedBytes < 0 || len(r.SHA256) != 64 || paths[r.Path] || hashes[r.SHA256] {
			return nil, errors.New("invalid/duplicate stream")
		}
		paths[r.Path] = true
		hashes[r.SHA256] = true
		r.sourceURL, r.targetURL = nodes[r.Source], nodes[r.Destination]
		if r.sourceURL == "" || r.targetURL == "" || r.sourceURL == r.targetURL {
			return nil, errors.New("unknown or aliased route member")
		}
		if _, err := p.streamPath(*r); err != nil {
			return nil, err
		}
		if err := p.walk(*r, func(_ int64, _ planRow) error { return nil }); err != nil {
			return nil, err
		}
	}
	return &p, nil
}
func origin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
		return "", fmt.Errorf("invalid endpoint origin %q", raw)
	}
	return strings.TrimSuffix(raw, "/"), nil
}
func (p *manifest) streamPath(r route) (string, error) {
	if !filepath.IsLocal(r.Path) || filepath.IsAbs(r.Path) {
		return "", errors.New("stream path escapes plan")
	}
	path, err := filepath.EvalSymlinks(filepath.Join(p.root, r.Path))
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(p.root, path)
	if err != nil || !filepath.IsLocal(relative) {
		return "", errors.New("stream symlink escapes plan")
	}
	return path, nil
}

// Validate the complete committed stream before any mutation, then hash again
// while consuming it. Plans must stay immutable for the duration of a run.
func (p *manifest) walk(r route, fn func(int64, planRow) error) error {
	path, err := p.streamPath(r)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(f, h), 65536)
	var ordinal, total int64
	header, ended := false, false
	for {
		line, err := reader.ReadSlice('\n')
		if len(line) > 65536 {
			return errors.New("oversize plan row")
		}
		if err == io.EOF {
			if len(line) != 0 {
				return errors.New("truncated plan row")
			}
			break
		}
		if err != nil {
			return err
		}
		if !utf8.Valid(line) {
			return errors.New("plan stream is not UTF-8")
		}
		var row planRow
		if err := json.Unmarshal(line, &row); err != nil {
			return err
		}
		if !header {
			if row.Type != "header" || row.Format != "go-carbon-ring" || row.Version != 1 || row.PlanID != p.PlanID || row.Source != r.Source || row.Destination != r.Destination {
				return errors.New("stream header mismatch")
			}
			header = true
			continue
		}
		if ended {
			return errors.New("rows after stream end")
		}
		switch row.Type {
		case "move":
			if row.Metric == "" || strings.ContainsAny(row.Metric, "/\r\n\x00") || row.EstimatedBytes < 0 {
				return errors.New("invalid metric row")
			}
			if err := fn(ordinal, row); err != nil {
				return err
			}
			ordinal++
			total += row.EstimatedBytes
		case "end":
			if row.Metrics != ordinal || row.EstimatedBytes != total {
				return errors.New("stream footer count mismatch")
			}
			ended = true
		default:
			return errors.New("unknown stream row type")
		}
	}
	if !header || !ended || ordinal != r.Metrics || total != r.EstimatedBytes || hex.EncodeToString(h.Sum(nil)) != r.SHA256 {
		return errors.New("stream checksum/count mismatch")
	}
	return nil
}

// Inventory is a copy plan, not a relay cutover proof. Deletion needs a fresh
// operator-produced barrier and is limited to physically retired old owners.
type barrier struct {
	PlanID            string `json:"plan_id"`
	ExpiresAt         int64  `json:"expires_at"`
	RelaySwitched     bool   `json:"relay_switched"`
	OldWritersDrained bool   `json:"old_writers_drained"`
	Reconciled        bool
	Sources           []string
}
