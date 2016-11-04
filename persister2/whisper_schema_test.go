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

func parseSchemas(t *testing.T, content string) (WhisperSchemas, error) {
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

func assertSchemas(t *testing.T, content string, expected []testcase, msg ...interface{}) WhisperSchemas {
	schemas, err := parseSchemas(t, content)
	if expected == nil {
		assert.Error(t, err, msg...)
		return nil
	}
	assert.Equal(t, len(expected), len(schemas))
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i].name, schemas[i].Name)
		assert.Equal(t, expected[i].pattern, schemas[i].Pattern.String())
		assertRetentionsEq(t, schemas[i].Retentions, expected[i].retentions)
	}

	return schemas
}

func TestParseSchemasSimple(t *testing.T) {
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

func TestParseSchemasComment(t *testing.T) {
	assertSchemas(t, `
# This is a wild comment
[carbon]
pattern = ^carbon\.
retentions = 60s:90d
	`,
		[]testcase{
			testcase{"carbon", "^carbon\\.", "60s:90d"},
		},
	)
}

func TestParseSchemasSortingAndMatch(t *testing.T) {
	// mixed record with and without priority. match metrics
	assert := assert.New(t)

	schemas := assertSchemas(t, `
[carbon]
pattern = ^carbon\.
retentions = 60s:90d

[db]
pattern = ^db\.
retentions = 1m:30d,1h:5y

[collector]
pattern = ^.*\.collector\.
retentions = 5s:300s,300s:30d
priority = 10

[gitlab]
pattern = ^gitlab\.
retentions = 1s:7d
priority = 100

[jira]
pattern = ^server\.
retentions = 1s:7d
priority = 10
	`,
		[]testcase{
			testcase{"gitlab", "^gitlab\\.", "1s:7d"},
			testcase{"collector", "^.*\\.collector\\.", "5s:5m,5m:30d"},
			testcase{"jira", "^server\\.", "1s:7d"},
			testcase{"carbon", "^carbon\\.", "1m:90d"},
			testcase{"db", "^db\\.", "1m:30d,1h:5y"},
		},
	)

	matched, ok := schemas.Match("db.collector.cpu")
	if assert.True(ok) {
		assert.Equal("collector", matched.Name)
	}

	matched, ok = schemas.Match("db.mysql.rps")
	if assert.True(ok) {
		assert.Equal("db", matched.Name)
	}

	matched, ok = schemas.Match("unknown")
	assert.False(ok)
}

func TestSchemasNotFound(t *testing.T) {
	// create and remove file
	assert := assert.New(t)

	tmpFile, err := ioutil.TempFile("", "schemas-")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	if err := os.Remove(tmpFile.Name()); err != nil {
		t.Fatal(err)
	}

	schemas, err := ReadWhisperSchemas(tmpFile.Name())

	assert.Nil(schemas)
	assert.Error(err)
}

func TestParseWrongSchemas(t *testing.T) {
	/*
		Cases:
			1. no pattern
			2. no retentions
			3. wrong pattern
			4. wrong retentions
			5. wrong priority
			6. empty pattern
			7. empty retentions
			8. empty priority
	*/

	// has no pattern
	assertSchemas(t, `
[carbon]
parrent = ^carbon\.
retentions = 60s:90d

[db]
pattern = ^db\.
retentions = 1m:30d,1h:5y
`, nil, "No pattern")

	// no retentions
	assertSchemas(t, `
[carbon]
pattern = ^carbon\.
ret = 60s:90d
`, nil, "No retentions")

	// wrong pattern
	assertSchemas(t, `
[carbon]
pattern = ^carb(on\.
retentions = 60s:90d
`, nil, "Wrong pattern")

	// wrong retentions
	assertSchemas(t, `
[carbon]
pattern = ^carbon\.
retentions = 60v:90d
`, nil, "Wrong retentions")

	// wrong priority
	assertSchemas(t, `
[carbon]
pattern = ^carbon\.
retentions = 60s:90d
priority = ab
`, nil, "Wrong priority")

	// empty pattern
	assertSchemas(t, `
[carbon]
pattern =
retentions = 60s:90d
`, nil, "Empty pattern")

	// empty retentions
	assertSchemas(t, `
[carbon]
pattern = ^carbon\.
retentions =
`, nil, "Empty retentions")

	// empty priority
	assertSchemas(t, `
[carbon]
pattern = ^carb(on\.
retentions = 60s:90d
priority =
`, nil, "Empty priority")
}
