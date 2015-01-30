package carbon

import "testing"

func TestCache(t *testing.T) {

	c := NewCache()

	c.Add("hello.world", 42, 10)

	if c.Size() != 1 {
		t.FailNow()
	}

	c.Add("hello.world", 15, 12)

	if c.Size() != 2 {
		t.FailNow()
	}

	key, values := c.Get()

	if key != "hello.world" {
		t.FailNow()
	}

	if len(values.Data) != 2 {
		t.FailNow()
	}

	c.Remove(key)

	if c.Size() != 0 {
		t.FailNow()
	}
}
