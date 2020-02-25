package persister

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ephemeralMatcher struct {
	idx   int
	nodes []string
}

type EphemeralFilter struct {
	home             string
	matchers         []ephemeralMatcher
	recycleThreshold time.Duration
}

const ephemeralNode = "$ephemeral"

func NewEphemeralFilter(home, filename string, recycleThreshold string) (*EphemeralFilter, error) {
	config, err := parseIniFile(filename)
	if err != nil {
		return nil, err
	}

	var ef EphemeralFilter
	ef.home = home
	ef.recycleThreshold, err = time.ParseDuration(recycleThreshold)
	if err != nil {
		return nil, fmt.Errorf("[persister] Failed to parse recycle-threshold %q", recycleThreshold)
	}
	for _, e := range config {
		p := e["pattern"]
		if p == "" {
			return nil, fmt.Errorf("[persister] Empty pattern in section %q", e["name"])
		}
		var em ephemeralMatcher
		em.idx = -1
		for i, n := range strings.Split(p, ".") {
			if n != ephemeralNode {
				em.nodes = append(em.nodes, n)
				continue
			}

			if em.idx != -1 {
				return nil, fmt.Errorf("[persister] Multiple %q found in pattern of section %q", ephemeralNode, e["name"])
			}
			em.idx = i
			em.nodes = append(em.nodes, "*")
		}
		if em.idx == -1 {
			return nil, fmt.Errorf("[persister] %q not found in pattern of section %q", ephemeralNode, e["name"])
		}
		ef.matchers = append(ef.matchers, em)
	}

	return &ef, nil
}

// TODO: checks issues filepath.Glob and os.Stat calls, which could be
// optimized away with trie index (if we include a modification
// timestamp on file nodes) if enabled.
func (ef *EphemeralFilter) check(metric string) (matched bool, path string, err error) {
	mns := strings.Split(metric, ".")

mloop:
	for _, em := range ef.matchers {
		if len(em.nodes) > len(mns) {
			continue
		}
		for i, n := range mns {
			if i >= len(em.nodes) {
				break
			}
			if em.nodes[i] == "*" || em.nodes[i] == n {
				continue
			}

			continue mloop
		}

		// TODO: handle em.idx == 0
		prefix := filepath.Join(append(append([]string{}, ef.home), mns[:em.idx]...)...)
		for i, n := range em.nodes {
			if n == "*" {
				mns[i] = "*"
			}
		}

		var matches []string
		matches, err = filepath.Glob(filepath.Join(append(append([]string{}, ef.home), mns...)...))
		if err != nil {
			return
		}

		// prefer matches that have the same prefix as the target metric
		var idx int
		for i := 0; i < len(matches); i++ {
			if !strings.HasPrefix(matches[i], prefix) {
				continue
			}
			tmp := matches[idx]
			matches[idx] = matches[i]
			matches[i] = tmp
			idx++
		}

		now := time.Now()
		for _, p := range matches {
			var stat os.FileInfo
			stat, err = os.Stat(p)
			if err != nil {
				return
			}

			// TODO: support more complex recycle policy
			if now.Sub(stat.ModTime()) >= ef.recycleThreshold {
				path = p
				matched = true
				return
			}
		}

		return
	}

	return
}
