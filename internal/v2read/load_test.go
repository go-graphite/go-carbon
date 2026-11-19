package v2read

import "context"

// load gives a published generation the full discovered directory and pending
// metrics of a request view, for tests that inspect a publication directly.
func (g *generation) load() {
	v, err := g.discover(context.Background())
	if g.err = err; err == nil {
		g.sealed, g.metrics = v.sealed, v.metrics
	}
}
