package points

import "testing"

func TestParseText(t *testing.T) {

	assertError := func(line string) {
		m, err := ParseText(line)
		if err == nil {
			t.Fatalf("Bad message parsed without error: %#v", line)
			return
		}
		if m != nil {
			t.Fatalf("Wrong message %#v != nil", m)
			return
		}
	}

	assertOk := func(line string, points *Points) {
		p, err := ParseText(line)

		if err != nil {
			t.Fatalf("Normal message not parsed: %#v", line)
			return
		}

		if !points.Eq(p) {
			t.Fatalf("%#v != %#v", p, points)
			return
		}
	}

	assertError("42")
	assertError("")
	assertError("\n")
	assertError("metric..name 42 \n")

	assertError("metric..name 42 1422642189\n")
	assertError("metric.name.. 42 1422642189\n")
	assertError("metric..name 42 1422642189")
	assertError("metric.name 42a 1422642189\n")
	assertError("metric.name 42 10\n")

	assertError("metric.name 42 2524708500\n")

	assertOk("metric.name -42.76 1422642189\n",
		OnePoint("metric.name", -42.76, 1422642189))

	assertOk("metric.name 42.15 1422642189\n",
		OnePoint("metric.name", 42.15, 1422642189))

}
