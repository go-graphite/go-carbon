package persister

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRetentionDefs(t *testing.T) {
	assert := assert.New(t)
	ret, err := ParseRetentionDefs("10s:24h,60s:30d,1h:5y")

	if assert.Nil(err) && assert.Equal(3, ret.Len()) {
		assert.Equal(8640, ret[0].NumberOfPoints())
		assert.Equal(10, ret[0].SecondsPerPoint())

		assert.Equal(43200, ret[1].NumberOfPoints())
		assert.Equal(60, ret[1].SecondsPerPoint())

		assert.Equal(43800, ret[2].NumberOfPoints())
		assert.Equal(3600, ret[2].SecondsPerPoint())
	}

	ret, err = ParseRetentionDefs("10s:24h, 60s:30d, 1h:5y")

	if assert.Nil(err) && assert.Equal(3, ret.Len()) {
		assert.Equal(8640, ret[0].NumberOfPoints())
		assert.Equal(10, ret[0].SecondsPerPoint())

		assert.Equal(43200, ret[1].NumberOfPoints())
		assert.Equal(60, ret[1].SecondsPerPoint())

		assert.Equal(43800, ret[2].NumberOfPoints())
		assert.Equal(3600, ret[2].SecondsPerPoint())
	}

	// old format of retentions
	ret, err = ParseRetentionDefs("60:43200,3600:43800")
	if assert.Nil(err) && assert.Equal(2, ret.Len()) {
		assert.Equal(43200, ret[0].NumberOfPoints())
		assert.Equal(60, ret[0].SecondsPerPoint())

		assert.Equal(43800, ret[1].NumberOfPoints())
		assert.Equal(3600, ret[1].SecondsPerPoint())
	}
}
