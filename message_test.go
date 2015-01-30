package carbon

import (
	"testing"
	"time"
)

func TestParseTextMessage(t *testing.T) {

	assertError := func(line string) {
		msg, err := ParseTextMessage(line)
		if err == nil {
			t.Fatalf("Bad message parsed without error: %#v", line)
			return
		}
		if msg != nil {
			t.Fatalf("Wrong message %#v != nil", msg)
			return
		}
	}

	assertOk := func(line string, message *Message) {
		msg, err := ParseTextMessage(line)

		if err != nil {
			t.Fatalf("Normal message not parsed: %#v", line)
			return
		}

		if msg.Name != message.Name || msg.Time != message.Time || msg.Value != message.Value {
			t.Fatalf("%#v != %#v", msg, message)
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
	assertError("metric..name 42a 1422642189\n")
	assertError("metric.name 42 10\n")

	assertOk("metric.name 42 1422642189", &Message{
		Name:  "metric.name",
		Value: 42,
		Time:  time.Unix(1422642189, 0),
	})
	assertOk("metric.name 42 1422642189\n", &Message{
		Name:  "metric.name",
		Value: 42,
		Time:  time.Unix(1422642189, 0),
	})

	assertOk("metric.name 42.15 1422642189\n", &Message{
		Name:  "metric.name",
		Value: 42.15,
		Time:  time.Unix(1422642189, 0),
	})

}
