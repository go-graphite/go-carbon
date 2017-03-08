package cache

import (
	"testing"

	"github.com/lomik/go-carbon/points"
)

func TestInFlight(t *testing.T) {
	var data []points.Point

	c := New(nil)

	c.Add(points.OnePoint("hello.world", 42, 10))

	p1 := c.WriteoutQueue().GetNotConfirmed(nil)
	if !p1.Eq(points.OnePoint("hello.world", 42, 10)) {
		t.FailNow()
	}

	data = c.Get("hello.world")
	if len(data) != 1 || data[0].Value != 42 {
		t.FailNow()
	}

	c.Add(points.OnePoint("hello.world", 43, 10))

	// 42 in flight, 43 in cache
	data = c.Get("hello.world")
	if len(data) != 2 || data[0].Value != 42 || data[1].Value != 43 {
		t.FailNow()
	}

	p2 := c.WriteoutQueue().GetNotConfirmed(nil)
	if !p2.Eq(points.OnePoint("hello.world", 43, 10)) {
		t.FailNow()
	}

	// 42, 43 in flight
	data = c.Get("hello.world")
	if len(data) != 2 || data[0].Value != 42 || data[1].Value != 43 {
		t.FailNow()
	}

	c.Confirm(p1)

	c.Add(points.OnePoint("hello.world", 44, 10))
	p3 := c.WriteoutQueue().GetNotConfirmed(nil)
	if !p3.Eq(points.OnePoint("hello.world", 44, 10)) {
		t.FailNow()
	}

	// 43, 44 in flight
	data = c.Get("hello.world")
	if len(data) != 2 || data[0].Value != 43 || data[1].Value != 44 {
		t.FailNow()
	}
}

func BenchmarkPopNotConfirmed(b *testing.B) {
	c := New(nil)
	p1 := points.OnePoint("hello.world", 42, 10)
	var p2 *points.Points

	for n := 0; n < b.N; n++ {
		c.Add(p1)
		p2, _ = c.PopNotConfirmed("hello.world")
		c.Confirm(p2)
	}

	if !p1.Eq(p2) {
		b.FailNow()
	}
}

func BenchmarkPopNotConfirmed100(b *testing.B) {
	c := New(nil)

	for i := 0; i < 100; i++ {
		c.Add(points.OnePoint("hello.world", 42, 10))
		c.PopNotConfirmed("hello.world")
	}

	p1 := points.OnePoint("hello.world", 42, 10)
	var p2 *points.Points

	for n := 0; n < b.N; n++ {
		c.Add(p1)
		p2, _ = c.PopNotConfirmed("hello.world")
		c.Confirm(p2)
	}

	if !p1.Eq(p2) {
		b.FailNow()
	}
}

func BenchmarkPop(b *testing.B) {
	c := New(nil)
	p1 := points.OnePoint("hello.world", 42, 10)
	var p2 *points.Points

	for n := 0; n < b.N; n++ {
		c.Add(p1)
		p2, _ = c.Pop("hello.world")
	}

	if !p1.Eq(p2) {
		b.FailNow()
	}
}
func BenchmarkGet(b *testing.B) {
	c := New(nil)
	c.Add(points.OnePoint("hello.world", 42, 10))

	var d []points.Point
	for n := 0; n < b.N; n++ {
		d = c.Get("hello.world")
	}

	if len(d) != 1 {
		b.FailNow()
	}
}

func BenchmarkGetNotConfirmed1(b *testing.B) {
	c := New(nil)

	c.Add(points.OnePoint("hello.world", 42, 10))
	c.PopNotConfirmed("hello.world")

	var d []points.Point
	for n := 0; n < b.N; n++ {
		d = c.Get("hello.world")
	}

	if len(d) != 1 {
		b.FailNow()
	}
}

func BenchmarkGetNotConfirmed100(b *testing.B) {
	c := New(nil)

	for i := 0; i < 100; i++ {
		c.Add(points.OnePoint("hello.world", 42, 10))
		c.PopNotConfirmed("hello.world")
	}

	var d []points.Point
	for n := 0; n < b.N; n++ {
		d = c.Get("hello.world")
	}

	if len(d) != 100 {
		b.FailNow()
	}
}

func BenchmarkGetNotConfirmed100Miss(b *testing.B) {
	c := New(nil)

	for i := 0; i < 100; i++ {
		c.Add(points.OnePoint("hello.world", 42, 10))
		c.PopNotConfirmed("hello.world")
	}

	var d []points.Point
	for n := 0; n < b.N; n++ {
		d = c.Get("metric.name")
	}

	if d != nil {
		b.FailNow()
	}
}
