package points

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteRead(t *testing.T) {
	assert := assert.New(t)

	p1 := OnePoint("hello.world", 42.0, 1491504040)
	p2 := OnePoint("hello.world", 45.0, 1491504100)
	p12 := OnePoint("hello.world", 42.0, 1491504040).Add(45.0, 1491504100)
	p123 := OnePoint("hello.world", 42.0, 1491504040).Add(42.0, 1491504100).Add(45.0, 1491504100).Add(37.0, 1491501100)

	sp1 := []*Points{p1}
	sp12 := []*Points{p12}
	sp123 := []*Points{p123}
	empty := []*Points{}

	table := []struct {
		method        string // plain, binary
		p             []*Points
		cutOffRight   int
		expected      []*Points
		expectedError bool
	}{
		{"plain", sp1, 0, sp1, false},
		{"plain", sp1, 1, empty, true},
		{"plain", sp1, 10, empty, true},
		{"plain", sp12, 0, []*Points{p1, p2}, false},
		{"plain", sp12, 1, sp1, true},

		{"binary", sp1, 0, sp1, false},
		{"binary", sp1, 1, empty, true},
		{"binary", sp1, 10, empty, true},
		{"binary", sp12, 0, sp12, false},
		{"binary", sp12, 1, sp1, true},
		{"binary", []*Points{p12, p1, p2}, 6, []*Points{p12, p1}, true},
		{"binary", sp123, 0, sp123, false},
	}

	for testID, tt := range table {
		func() {
			wb := new(bytes.Buffer)

			// Write to buffer
			if tt.p != nil {
				for _, p := range tt.p {
					switch tt.method {
					case "plain":
						p.WriteTo(wb)
					case "binary":
						p.WriteBinaryTo(wb)
					}
				}
			}

			data := wb.Bytes()
			if tt.cutOffRight > 0 {
				data = data[:len(data)-tt.cutOffRight]
			}

			rb := bytes.NewBuffer(data)

			var err error

			result := make([]*Points, 0)

			switch tt.method {
			case "plain":
				err = ReadPlain(rb, func(p *Points) {
					result = append(result, p)
				})
			case "binary":
				err = ReadBinary(rb, func(p *Points) {
					result = append(result, p)
				})
			}

			errorMessage := fmt.Sprintf("test %d", testID)

			if tt.expectedError {
				assert.Error(err, errorMessage)
			} else {
				assert.NoError(err, errorMessage)
			}

			if assert.Equal(len(tt.expected), len(result), errorMessage) {
				for i := 0; i < len(result); i++ {
					assert.True(result[i].Eq(tt.expected[i]), errorMessage)
				}
			}
		}()
	}

}
