package cache

import "testing"

func TestStreamPointSlots(t *testing.T) {
	c := New()
	var slot any
	c.AddStreamPoint([]byte("example.a"), 1, 10, &slot)
	ref, ok := slot.(*PointRef)
	if !ok || ref.item == nil {
		t.Fatal("slot was not bound")
	}
	c.AddStreamPoint([]byte("example.a"), 2, 11, &slot)
	if slot != ref || len(c.Get("example.a")) != 2 {
		t.Fatal("known id did not reuse its reference")
	}
	c.Pop("example.a")
	c.AddStreamPoint([]byte("example.a"), 3, 12, &slot)
	if got := c.Get("example.a"); len(got) != 1 || got[0].Value != 3 {
		t.Fatal("stale reference lost point")
	}
	slot = nil // Dictionary reset or inherited snapshot.
	c.AddStreamPoint([]byte("example.b"), 4, 13, &slot)
	c.AddStreamPoint([]byte("example.c"), 5, 14, nil)
	if len(c.Get("example.b")) != 1 || len(c.Get("example.c")) != 1 {
		t.Fatal("reset or nil slot lost point")
	}
}
