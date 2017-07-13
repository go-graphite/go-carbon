package parse

import (
	"fmt"
	"testing"

	"github.com/lomik/go-carbon/points"
	"github.com/stretchr/testify/assert"
)

type testcase struct {
	description string
	input       []byte
	output      []*points.Points
	wantError   bool
}

func run(t *testing.T, table []testcase, parser func(body []byte) ([]*points.Points, error)) {
	for i := 0; i < len(table); i++ {
		tc := table[i]

		output, err := parser(tc.input)
		if !tc.wantError {
			assert.Nil(t, err)
			assert.Equal(t, tc.output, output, fmt.Sprintf("failed while parsing: %#v", tc.description))
		} else {
			assert.Error(t, err, fmt.Sprintf("bad message not raises error: %#v", tc.description))
		}
	}
}
