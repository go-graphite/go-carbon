package floatresearch

import "fmt"

// Select evaluates complete frames, including framing and inner compression.
// The original durable frame always wins ties and bounds logical size.
func (c Codec) Select(policy string, g Group, original []byte) (string, []byte, error) {
	best, winner := original, "sealed"
	kinds := []string{}
	switch policy {
	case "current":
		b, err := c.Encode("sealed", g)
		return "sealed", b, err
	case "chimp":
		kinds = []string{"chimp128"}
	case "alp":
		kinds = []string{"alp"}
	case "minfour":
		kinds = []string{"chimp128", "alp", "dictionary"}
	case "prefix256":
		selected, err := c.Prefix(g)
		if err != nil {
			return "", nil, err
		}
		if selected != "sealed" {
			kinds = []string{selected}
		}
	default:
		return "", nil, fmt.Errorf("unknown policy %q", policy)
	}
	for _, k := range kinds {
		v, e := c.Encode(k, g)
		if e != nil {
			return "", nil, e
		}
		if len(v) < len(best) {
			best, winner = v, k
		}
	}
	return winner, best, nil
}

// Prefix predicts a winner using independently encoded first-256-point frames.
func (c Codec) Prefix(g Group) (string, error) {
	prefix := Group{Points: g.Points[:min(256, len(g.Points))], Step: g.Step}
	b, err := c.Encode("sealed", prefix)
	if err != nil {
		return "", err
	}
	selected := "sealed"
	for _, k := range []string{"chimp128", "alp", "dictionary"} {
		v, e := c.Encode(k, prefix)
		if e != nil {
			return "", e
		}
		if len(v) < len(b) {
			b, selected = v, k
		}
	}
	return selected, nil
}
