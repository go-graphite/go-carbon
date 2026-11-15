package partsread

import "errors"

var ErrHandleActive = errors.New("snapshot handle is still active")

// PinInto pins into caller-owned storage. Close before reuse. Results remain
// borrowed until Close, exactly as with Pin.
func (p *Publication) PinInto(h *Handle, b *Budget, workBytes int64) error {
	if h == nil || b == nil || workBytes < 0 {
		return ErrAdmission
	}
	if h.v != nil && !h.closed.Load() {
		return ErrHandleActive
	}
	for {
		v := p.current.Load()
		if v == nil {
			return ErrClosed
		}
		if !v.retain() {
			continue
		}
		retained := v.view.RetainedBytes
		if retained < 0 || retained > int64(^uint64(0)>>1)-workBytes || !b.take(retained+workBytes) {
			v.release()
			return ErrAdmission
		}
		*h = Handle{v: v, budget: b, charged: retained + workBytes, work: workBytes}
		return nil
	}
}

// Snapshot retains one immutable generation for a published reader. The owner
// MUST drain every caller before Close. Close must not race with borrowers or
// another Close. BorrowInto avoids publication retain/
// release on the request path; its byte reservation is still enforced.
type Snapshot struct{ v *version }

func (p *Publication) Snapshot() (*Snapshot, error) {
	for {
		v := p.current.Load()
		if v == nil {
			return nil, ErrClosed
		}
		if v.retain() {
			return &Snapshot{v: v}, nil
		}
	}
}
func (s *Snapshot) Close() {
	if s != nil && s.v != nil {
		s.v.release()
		s.v = nil
	}
}
func (s *Snapshot) BorrowInto(h *Handle, b *Budget, workBytes int64) error {
	if s == nil || s.v == nil {
		return ErrClosed
	}
	if h == nil || b == nil || workBytes < 0 {
		return ErrAdmission
	}
	if h.v != nil && !h.closed.Load() {
		return ErrHandleActive
	}
	retained := s.v.view.RetainedBytes
	if retained < 0 || retained > int64(^uint64(0)>>1)-workBytes || !b.take(retained+workBytes) {
		return ErrAdmission
	}
	*h = Handle{v: s.v, budget: b, charged: retained + workBytes, work: workBytes, borrowed: true}
	return nil
}

// HasLegacyCatalog reports whether Resolve carries explicit legacy ownership.
func (s *Snapshot) HasLegacyCatalog() bool {
	if s == nil || s.v == nil {
		return false
	}
	_, ok := s.v.view.Catalog.(LegacyCatalog)
	return ok
}
