package ringplan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	hashing "github.com/go-graphite/go-carbon/internal/downsize/buckyhash"
)

func generateConfig(s specification, old, next *ring, id string, opt options, out io.Writer) error {
	if s.Old.Hash != "jump_fnv1a" || s.Old.Replicas != 1 || s.New.Hash != "jump_fnv1a" || s.New.Replicas != 1 {
		return errors.New("jump relay configuration generation requires jump_fnv1a and replicas=1")
	}
	files := make(map[string][]byte)
	relay := func(r *ring) []byte {
		var b bytes.Buffer
		fmt.Fprintf(&b, "# plan %s; replace only this cluster, preserve other clusters and rules\n[[cluster]]\nname = %q\ntype = \"jump\"\n", id, s.Cluster+"-"+s.Site)
		for i, m := range r.members {
			n, _ := hashing.NewNodeParser(m.Node)
			fmt.Fprintf(&b, "[[cluster.hosts]]\nname = %q\nindex = %d\nport = %d\n", n.Server, i, ingestionPort(n.Port))
			if m.MTCP > 0 {
				fmt.Fprintf(&b, "mtcp = %d\n", m.MTCP)
			}
		}
		return b.Bytes()
	}
	files["relay-old.toml"] = relay(old)
	files["relay-new.toml"] = relay(next)
	union := append([]member(nil), s.Old.Nodes...)
	seen := make(map[string]bool)
	for _, m := range union {
		seen[m.ID] = true
	}
	for _, m := range s.New.Nodes {
		if !seen[m.ID] {
			union = append(union, m)
			seen[m.ID] = true
		}
	}
	backends := func(nodes []member) []byte {
		var b bytes.Buffer
		fmt.Fprintf(&b, "# Merge under backendsByDC[%s].clusters[%s]; keep all other sites/clusters.\n- name: %q\n  protocolBackends:\n", s.Site, s.Cluster, s.Cluster)
		for _, m := range nodes {
			n, _ := hashing.NewNodeParser(m.Node)
			fmt.Fprintf(&b, "    - http: %q\n", "http://"+n.Server+":8080")
		}
		return b.Bytes()
	}
	files["carbonzipper-during.yaml"] = backends(union)
	files["carbonzipper-final.yaml"] = backends(s.New.Nodes)
	var sources []string
	seenSources := make(map[string]bool)
	for _, member := range s.Old.Nodes {
		node, _ := hashing.NewNodeParser(member.Node)
		origin, err := url.Parse(member.Endpoint)
		if err != nil {
			return fmt.Errorf("source endpoint: %w", err)
		}
		for _, host := range []string{node.Server, origin.Hostname()} {
			key := strings.ToLower(host)
			if host != "" && !seenSources[key] {
				seenSources[key] = true
				sources = append(sources, host)
			}
		}
	}
	for i, m := range union {
		var b bytes.Buffer
		parsed, _ := hashing.NewNodeParser(m.Node)
		fmt.Fprintf(&b, "# Member %s; merge this block with its existing buckyd limits/auth/backend.\n[buckyd]\nnode = %q\nhash = %q\nreplicas = %d\nnodes = [\n", m.ID, parsed.Server, s.New.Hash, s.New.Replicas)
		// Explicit instances pin lexical buckytools order to jump relay indices.
		for j, n := range next.members {
			parsed, _ := hashing.NewNodeParser(n.Node)
			fmt.Fprintf(&b, "  %q,\n", fmt.Sprintf("%s:%d=%03d", parsed.Server, ingestionPort(parsed.Port), j))
		}
		b.WriteString("]\ntransfer-sources = [\n")
		for _, source := range sources {
			fmt.Fprintf(&b, "  %q,\n", source)
		}
		b.WriteString("]\n")
		files[fmt.Sprintf("buckyd-%03d.toml", i)] = b.Bytes()
	}
	files["ORDER.txt"] = []byte("LOCAL DRAFT ONLY. Review README and generated configuration drafts before deployment.\n1. Expand reads to union(old,new), prove split-history merging and invalidate routing caches.\n2. Prepare new stores and bulk baseline-copy with old ingestion unchanged.\n3. Switch ALL relay writers to the new ring, drain old relay queues and old store caches/WAL; update buckyd descriptors on ALL old/new nodes.\n4. Re-list, final delta-merge, verify archive/point parity; keep all old read backends.\n5. Guarded source-version delete only after verified durable targets and writer fencing.\n6. Remove retired read backends after deletion/refresh verification; reclaim hosts last.\nRollback before deletes: keep union reads and copy new deltas back before restoring old writers.\n")
	files["spec.json"], _ = json.MarshalIndent(s, "", "  ")
	for _, name := range sortedKeys(files) {
		if _, err := fmt.Fprintf(out, "--- %s ---\n%s\n", name, files[name]); err != nil {
			return err
		}
	}
	if !opt.Apply {
		return nil
	}
	if opt.Out == "" {
		return errors.New("config --apply needs --out")
	}
	if err := os.Mkdir(opt.Out, 0700); err != nil {
		return fmt.Errorf("config output must be a new directory: %w", err)
	}
	for _, name := range sortedKeys(files) {
		if err := os.WriteFile(filepath.Join(opt.Out, name), files[name], 0600); err != nil {
			return err
		}
	}
	return nil
}

func ingestionPort(port int) int {
	if port == 0 {
		return 2003
	}
	return port
}

func sortedKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
