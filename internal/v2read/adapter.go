package v2read

import (
	"context"
	"time"
)

// Adapter is the fetch/find boundary for a read server. Select resolves a metric
// name to its owner publication; Catalog resolves find against a pinned index.
// Consume executes while the snapshot and byte reservation are still held.
// Serialize/stream its response before returning; do not retain series values.
type Adapter struct {
	Select    func(string) *Publication
	Catalog   *Publication
	Budget    *Budget
	WorkBytes int64
	Now       func() time.Time
}

func (a *Adapter) Fetch(ctx context.Context, name string, from, until int64, consume func(*Series) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := a.Select(name)
	if p == nil {
		return ErrNotFound
	}
	h, err := p.Pin(a.Budget, a.WorkBytes)
	if err != nil {
		return err
	}
	defer h.Close()
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	s, err := h.Render(ctx, name, from, until, now().Unix())
	if err != nil {
		return err
	}
	return consume(s)
}
func (a *Adapter) Find(ctx context.Context, pattern string, yield func(string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h, err := a.Catalog.Pin(a.Budget, a.WorkBytes)
	if err != nil {
		return err
	}
	defer h.Close()
	return h.Find(ctx, pattern, yield)
}
