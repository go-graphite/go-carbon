package parse

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
	"github.com/vmihailenco/msgpack/v5"
)

func TestMsgpack(t *testing.T) {
	var testCaseOK = Datapoint{
		Name:  "test.case.number.1",
		Value: 60.2,
		Time:  1423931224,
	}

	var testCaseEmptyName = Datapoint{
		Name:  "",
		Value: 60.2,
		Time:  1423931224,
	}

	msgOk, _ := msgpack.Marshal(testCaseOK)
	msgFail, _ := msgpack.Marshal(testCaseEmptyName)

	msgpacks := []testcase{
		{"One metric with one datapoint",
			msgOk,
			[]*points.Points{points.OnePoint("test.case.number.1", 60.2, 1423931224)},
			false,
		},
		{"Empty metric name with one datapoint",
			msgFail,
			[]*points.Points{points.OnePoint("", 60.2, 1423931224)},
			true,
		},
	}
	run(t, msgpacks, Msgpack)
}
