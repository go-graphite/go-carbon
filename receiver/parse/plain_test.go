package parse

import "testing"

func TestPlainLine(t *testing.T) {
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
		{"metric..name 42.15 1422642189\n", "metric..name", 42.15, 1422642189},
		{"metric...name 42.15 1422642189\n", "metric...name", 42.15, 1422642189},
		{"metric.name 42.15 1422642189\r\n", "metric.name", 42.15, 1422642189},
		{"aerospike-test1.host.test-as04.context.histogram.objsz.bucket_15 0 1553244874 \n", "aerospike-test1.host.test-as04.context.histogram.objsz.bucket_15", 0, 1553244874},
	}

	for _, p := range table {
		name, value, timestamp, err := PlainLine([]byte(p.b))
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
