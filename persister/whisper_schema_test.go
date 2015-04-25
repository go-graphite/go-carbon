package persister

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/lomik/go-whisper"
	"github.com/stretchr/testify/assert"
)

func assertRetentionsEq(t *testing.T, ret whisper.Retentions, s string) {
	assert := assert.New(t)
	// s - good retentions string for compare ret with expected
	expected, err := ParseRetentionDefs(s)
	if err != nil {
		// wtf?
		t.Fatal(err)
		return
	}

	if assert.Nil(err) && assert.Equal(ret.Len(), expected.Len()) {
		for i := 0; i < expected.Len(); i++ {
			assert.Equal(expected[i].NumberOfPoints(), ret[i].NumberOfPoints())
			assert.Equal(expected[i].SecondsPerPoint(), ret[i].SecondsPerPoint())
		}
	}
}

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

	// test strip spaces
	if ret, err = ParseRetentionDefs("10s:24h, 60s:30d, 1h:5y"); assert.Nil(err) {
		assertRetentionsEq(t, ret, "10s:1d,1m:30d,1h:5y")
	}

	// old format of retentions
	if ret, err = ParseRetentionDefs("60:43200,3600:43800"); assert.Nil(err) {
		assertRetentionsEq(t, ret, "1m:30d,1h:5y")
	}

	// unknown letter
	ret, err = ParseRetentionDefs("10s:24v")
	assert.Error(err)
}

func parseSchemas(t *testing.T, content string) (*WhisperSchemas, error) {
	tmpFile, err := ioutil.TempFile("", "schemas-")
	if err != nil {
		t.Fatal(err)
		return nil, nil
	}
	tmpFile.Write([]byte(content))
	tmpFile.Close()

	schemas, err := ReadWhisperSchemas(tmpFile.Name())

	if removeErr := os.Remove(tmpFile.Name()); removeErr != nil {
		t.Fatal(removeErr)
	}

	return schemas, err
}

type testcase struct {
	name       string
	pattern    string
	retentions string
}

func assertSchemas(t *testing.T, content string, expected []testcase) {
	schemas, err := parseSchemas(t, content)
	if expected == nil {
		assert.Error(t, err)
		return
	}
	assert.Equal(t, len(expected), len(schemas.Data))
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i].name, schemas.Data[i].name)
		assert.Equal(t, expected[i].pattern, schemas.Data[i].pattern.String())
		assertRetentionsEq(t, schemas.Data[i].retentions, expected[i].retentions)
	}
}

func TestParseSchemas1(t *testing.T) {
	// Simple parse
	assertSchemas(t, `
[carbon]
pattern = ^carbon\.
retentions = 60s:90d

[default]
pattern = .*
retentions = 1m:30d,1h:5y
	`,
		[]testcase{
			testcase{"carbon", "^carbon\\.", "60s:90d"},
			testcase{"default", ".*", "1m:30d,1h:5y"},
		},
	)
}
