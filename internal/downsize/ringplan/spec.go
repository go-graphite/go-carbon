package ringplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
)

const formatName = "go-carbon-ring"
const version = 1

type member struct {
	ID       string `json:"id"`
	Node     string `json:"node"`
	Endpoint string `json:"endpoint"`
	MTCP     int    `json:"mtcp,omitempty"`
}

type topology struct {
	Hash         string   `json:"hash"`
	Replicas     int      `json:"replicas"`
	VirtualNodes int      `json:"virtual_nodes,omitempty"`
	Nodes        []member `json:"nodes"`
}

type input struct {
	Source         string `json:"source"`
	Path           string `json:"path,omitempty"`
	URL            string `json:"url,omitempty"`
	Format         string `json:"format"`
	EstimatedBytes int64  `json:"estimated_bytes"`
	TokenEnv       string `json:"token_env,omitempty"`
}

type specification struct {
	Placements       map[string]Placement `json:"placements,omitempty"`
	Version          int                  `json:"version"`
	Cluster          string               `json:"cluster"`
	Site             string               `json:"site"`
	Old              topology             `json:"old"`
	New              topology             `json:"new"`
	Inputs           []input              `json:"inputs"`
	AllowUnowned     bool                 `json:"allow_unowned,omitempty"`
	PartialInventory bool                 `json:"partial_inventory,omitempty"`
}

type ring struct {
	compiled *hashing.Compiled
	members  []member
}

func newRing(t *topology) (*ring, error) {
	switch t.Hash {
	case "carbon_ch", "carbon", "":
		t.Hash = "carbon"
	case "fnv1a_ch", "fnv1a":
		t.Hash = "fnv1a"
	case "jump_fnv1a_ch", "jump_fnv1a":
		t.Hash = "jump_fnv1a"
	default:
		return nil, fmt.Errorf("unsupported hash %q", t.Hash)
	}
	if t.Replicas == 0 {
		t.Replicas = 1
	}
	if t.VirtualNodes == 0 {
		t.VirtualNodes = 100
	}
	if t.VirtualNodes < 1 || t.VirtualNodes > 10000 || len(t.Nodes)*t.VirtualNodes > 1000000 {
		return nil, errors.New("virtual_nodes must be 1..10000; at most 1000000 ring tokens")
	}
	nodes := make([]hashing.Node, len(t.Nodes))
	byNode := make(map[hashing.Node]member, len(t.Nodes))
	ids := make(map[string]bool, len(t.Nodes))
	identities := make(map[string]bool, len(t.Nodes))
	for i, m := range t.Nodes {
		if m.ID == "" || ids[m.ID] || strings.ContainsAny(m.ID, "\r\n\x00") || m.MTCP < 0 {
			return nil, fmt.Errorf("empty or duplicate/invalid member id %q", m.ID)
		}
		ids[m.ID] = true
		hostPort, _, _ := strings.Cut(m.Node, "=")
		if _, port, hasPort := strings.Cut(hostPort, ":"); hasPort {
			n, err := strconv.Atoi(port)
			if err != nil || strconv.Itoa(n) != port {
				return nil, fmt.Errorf("node %q needs a canonical decimal port", m.Node)
			}
		}
		n, err := hashing.NewNodeParser(m.Node)
		if err != nil || n.Server == "" || n.Port < 0 || n.Port > 65535 || strings.ContainsAny(m.Node, " \t\r\n\x00'\\") {
			return nil, fmt.Errorf("invalid HOST[:PORT][=INSTANCE] %q", m.Node)
		}
		key := n.String()
		if t.Hash == "carbon" {
			key = n.CarbonKeyValue()
		}
		if t.Hash == "fnv1a" {
			key = n.FNV1aKeyValue()
		}
		if t.Hash == "jump_fnv1a" && n.Instance != "" {
			key = n.Instance
		}
		if identities[key] {
			return nil, fmt.Errorf("duplicate hash identity %q", key)
		}
		identities[key] = true
		if _, exists := byNode[n]; exists {
			return nil, fmt.Errorf("duplicate node %q", m.Node)
		}
		if err := validURL(m.Endpoint); err != nil {
			return nil, fmt.Errorf("member %s endpoint: %w", m.ID, err)
		}
		byNode[n] = m
		nodes[i] = n
	}
	c, err := hashing.Compile(t.Hash, nodes, t.Replicas, t.VirtualNodes)
	if err != nil {
		return nil, err
	}
	r := &ring{compiled: c}
	for _, n := range c.Nodes() {
		r.members = append(r.members, byNode[n])
	}
	return r, nil
}

func validURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("expected http(s) URL without credentials, query or fragment")
	}
	return nil
}

func loadSpec(path string) (specification, *ring, *ring, string, error) {
	var s specification
	f, err := os.Open(path)
	if err != nil {
		return s, nil, nil, "", err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 8<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return s, nil, nil, "", err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return s, nil, nil, "", errors.New("trailing specification data")
	}
	if s.Version != version || s.Cluster == "" || s.Site == "" {
		return s, nil, nil, "", errors.New("spec needs version=1, cluster and site")
	}
	old, err := newRing(&s.Old)
	if err != nil {
		return s, nil, nil, "", fmt.Errorf("old: %w", err)
	}
	next, err := newRing(&s.New)
	if err != nil {
		return s, nil, nil, "", fmt.Errorf("new: %w", err)
	}
	oldIDs := make(map[string]member)
	for _, m := range s.Old.Nodes {
		oldIDs[m.ID] = m
	}
	for _, m := range s.New.Nodes {
		if prev, ok := oldIDs[m.ID]; ok && prev.Endpoint != m.Endpoint {
			return s, nil, nil, "", fmt.Errorf("member %s changed physical endpoint: assign replacement a new ID", m.ID)
		}
	}
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return s, nil, nil, "", err
	}
	for i := range s.Inputs {
		in := &s.Inputs[i]
		if _, ok := oldIDs[in.Source]; !ok {
			return s, nil, nil, "", fmt.Errorf("unknown input source %q", in.Source)
		}
		if in.EstimatedBytes < 0 {
			return s, nil, nil, "", errors.New("negative estimated bytes")
		}
		switch in.Format {
		case "lines", "tsv", "jsonl":
			if in.Path == "" || in.URL != "" {
				return s, nil, nil, "", errors.New("file inventory needs path only")
			}
			if !filepath.IsAbs(in.Path) {
				in.Path = filepath.Join(base, in.Path)
			}
		case "buckyd", "transfer":
			if in.Path != "" {
				return s, nil, nil, "", errors.New("buckyd inventory needs url only")
			}
			if err := validURL(in.URL); err != nil {
				return s, nil, nil, "", err
			}
		default:
			return s, nil, nil, "", fmt.Errorf("unsupported inventory format %q", in.Format)
		}
	}
	blob, err := json.Marshal(s)
	if err != nil {
		return s, nil, nil, "", err
	}
	sum := sha256.Sum256(blob)
	return s, old, next, hex.EncodeToString(sum[:]), nil
}
