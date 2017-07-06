package points

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestRemoveDoubleDot(t *testing.T) {
	table := [](struct {
		input    string
		expected string
	}){
		{"", ""},
		{".....", "."},
		{"hello.world", "hello.world"},
		{"hello..world", "hello.world"},
		{"..hello..world..", ".hello.world."},
	}

	for _, p := range table {
		v := RemoveDoubleDot([]byte(p.input))
		if string(v) != p.expected {
			t.Fatalf("%#v != %#v", string(v), p.expected)
		}
	}
}

func TestPlainParseLine(t *testing.T) {
	table := [](struct {
		b         string
		name      string
		value     float64
		timestamp int64
	}){
		{b: "42"},
		{b: ""},
		{b: "\n"},
		{b: "metric..name 42 \n"},
		{b: "metric..name 42"},
		{b: "metric.name 42 a1422642189\n"},
		{b: "metric.name 42a 1422642189\n"},
		{b: "metric.name NaN 1422642189\n"},
		{b: "metric.name 42 NaN\n"},
		{"metric.name -42.76 1422642189\n", "metric.name", -42.76, 1422642189},
		{"metric.name 42.15 1422642189\n", "metric.name", 42.15, 1422642189},
		{"metric..name 42.15 1422642189\n", "metric.name", 42.15, 1422642189},
		{"metric...name 42.15 1422642189\n", "metric.name", 42.15, 1422642189},
		{"metric.name 42.15 1422642189\r\n", "metric.name", 42.15, 1422642189},
	}

	for _, p := range table {
		name, value, timestamp, err := PlainParseLine([]byte(p.b))
		if p.name == "" {
			// expected error
			if err == nil {
				t.Fatal("error expected")
			}
		} else {
			if string(name) != p.name {
				t.Fatalf("%#v != %#v", string(name), p.name)
			}
			if value != p.value {
				t.Fatalf("%#v != %#v", value, p.value)
			}
			if timestamp != p.timestamp {
				t.Fatalf("%d != %d", timestamp, p.timestamp)
			}
		}
	}
}

func BenchmarkParsePlain(b *testing.B) {
	now := time.Now().Unix()

	msg := fmt.Sprintf("carbon.agents.localhost.cache.size 1412351 %d\n", now)
	buf1 := new(bytes.Buffer)
	for i := 0; i < 50; i++ {
		buf1.Write([]byte(msg))
	}

	msg2 := fmt.Sprintf("carbon.agents.server.udp.received 42 %d\n", now)
	buf2 := new(bytes.Buffer)
	for i := 0; i < 50; i++ {
		buf2.Write([]byte(msg2))
	}

	body1 := buf1.Bytes()
	body2 := buf2.Bytes()

	var err error
	b.ResetTimer()

	for i := 0; i < b.N; i += 100 {
		_, err = ParsePlain(body1)
		if err != nil {
			b.Fatal(err)
		}
		_, err = ParsePlain(body2)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParsePlainV0(b *testing.B) {
	now := time.Now().Unix()

	msg := fmt.Sprintf("carbon.agents.localhost.cache.size 1412351 %d\n", now)
	buf1 := new(bytes.Buffer)
	for i := 0; i < 50; i++ {
		buf1.Write([]byte(msg))
	}

	msg2 := fmt.Sprintf("carbon.agents.server.udp.received 42 %d\n", now)
	buf2 := new(bytes.Buffer)
	for i := 0; i < 50; i++ {
		buf2.Write([]byte(msg2))
	}

	body1 := buf1.Bytes()
	body2 := buf2.Bytes()

	var err error
	b.ResetTimer()

	for i := 0; i < b.N; i += 100 {
		_, err = ParsePlainV0(body1)
		if err != nil {
			b.Fatal(err)
		}
		_, err = ParsePlainV0(body2)
		if err != nil {
			b.Fatal(err)
		}
	}
}
