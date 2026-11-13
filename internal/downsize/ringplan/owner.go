package ringplan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// TargetOwner reads a ring file and returns a predicate reporting whether the
// member whose endpoint is target owns a metric name. The file is either a
// resize specification (its "new" ring is used) or a bare topology
// {"hash":"jump_fnv1a","nodes":[{"id","node","endpoint"}...]}. Ownership uses
// the same compiled buckytools hash as the planner.
func TargetOwner(path, target string) (func(string) bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var probe struct {
		New *topology `json:"new"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, fmt.Errorf("ring %s: %w", path, err)
	}
	var t topology
	if probe.New != nil {
		t = *probe.New
	} else {
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if err := d.Decode(&t); err != nil {
			return nil, fmt.Errorf("ring %s: expected a resize spec or a topology: %w", path, err)
		}
	}
	if len(t.Nodes) == 0 {
		return nil, errors.New("ring has no nodes")
	}
	r, err := newRing(&t)
	if err != nil {
		return nil, err
	}
	target = strings.TrimRight(target, "/")
	index := -1
	for i, m := range r.members {
		if strings.TrimRight(m.Endpoint, "/") == target {
			index = i
		}
	}
	if index < 0 {
		return nil, fmt.Errorf("target %s is not an endpoint of the ring", target)
	}
	return func(name string) bool {
		var owners [8]int
		for _, owner := range r.compiled.Lookup(name, owners[:0]) {
			if owner == index {
				return true
			}
		}
		return false
	}, nil
}
