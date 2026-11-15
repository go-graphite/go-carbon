package mixedread

import (
	"encoding/json"
	"testing"
)

func TestCursorByteExactRoundTrip(t *testing.T) {
	c := Cursor{Version: 1, Before: []byte{'n', 0xff, 0, 0x80}}
	data, e := json.Marshal(c)
	if e != nil {
		t.Fatal(e)
	}
	var got Cursor
	if e = json.Unmarshal(data, &got); e != nil {
		t.Fatal(e)
	}
	if string(got.Before) != string(c.Before) {
		t.Fatal("cursor changed exact metric bytes")
	}
	if e = got.Validate(); e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		name       string
		suppressed bool
	}{{"a", true}, {string(c.Before), false}, {string(append(append([]byte{}, c.Before...), 0)), false}, {"z", false}} {
		if got.Suppresses(tc.name) != tc.suppressed {
			t.Fatalf("routing %q", tc.name)
		}
	}
	if (Cursor{Version: 1}).Suppresses("") {
		t.Fatal("empty cursor converts names")
	}
	if !(Cursor{Version: 1, Done: true}).Suppresses("z") {
		t.Fatal("done cursor retained legacy")
	}
	if (Cursor{Version: 2}).Validate() == nil || (Cursor{Version: 1, Done: true, Before: []byte("a")}).Validate() == nil {
		t.Fatal("invalid cursor accepted")
	}
}

func TestCursorFilterFreezesBoundary(t *testing.T) {
	c := Cursor{Version: 1, Before: []byte("middle")}
	f, err := c.Filter()
	if err != nil {
		t.Fatal(err)
	}
	c.Before[0] = 'a'
	if !f("first") || f("z") {
		t.Fatal("published cursor followed mutable caller memory")
	}
	if _, err = (Cursor{Version: 2}).Filter(); err == nil {
		t.Fatal("invalid cursor published")
	}
}
