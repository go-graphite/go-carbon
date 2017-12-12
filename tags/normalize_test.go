package tags

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type normalizeTestCase struct {
	in  string
	ex  string
	err bool
}

var normalizeTestTable = []normalizeTestCase{
	{"some.metric;tag1=value2;tag2=value.2;tag1=value3", "some.metric;tag1=value3;tag2=value.2", false},
	{"some.metric;tag1=value2;tag2=value.2;tag1=value0", "some.metric;tag1=value0;tag2=value.2", false},
	{"some.metric;c=1;b=2;a=3", "some.metric;a=3;b=2;c=1", false},
	{"some.metric;k=a;k=_;k2=3;k=0;k=42", "some.metric;k2=3;k=42", false}, // strange order but as in python-carbon
	{"some.metric", "some.metric", false},
}

var benchmarkMetric = "used;host=dfs1;what=diskspace;mountpoint=srv/node/dfs10;unit=B;metric_type=gauge;agent=diamond;processed_by=statsd2"

func TestNormalizeOriginal(t *testing.T) {
	assert := assert.New(t)

	for i := 0; i < len(normalizeTestTable); i++ {
		n, err := normalizeOriginal(normalizeTestTable[i].in)

		if !normalizeTestTable[i].err {
			assert.NoError(err)
		} else {
			assert.Error(err)
		}

		assert.Equal(normalizeTestTable[i].ex, n)
	}
}

func TestNormalize(t *testing.T) {
	assert := assert.New(t)

	for i := 0; i < len(normalizeTestTable); i++ {
		n, err := Normalize(normalizeTestTable[i].in)

		if !normalizeTestTable[i].err {
			assert.NoError(err)
		} else {
			assert.Error(err)
		}

		assert.Equal(normalizeTestTable[i].ex, n)
	}
}

func TestFilePath(t *testing.T) {
	assert := assert.New(t)
	p := FilePath("/data/", "some.metric;tag1=value2;tag2=value.2")
	assert.Equal("/data/_tagged/eff/aae/some_DOT_metric;tag1=value2;tag2=value_DOT_2", p)
}

func BenchmarkNormalizeOriginal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := normalizeOriginal(benchmarkMetric)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNormalize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Normalize(benchmarkMetric)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFilePath(b *testing.B) {
	m, _ := Normalize(benchmarkMetric)
	var x string

	for i := 0; i < b.N; i++ {
		x = FilePath("/data", m)
	}

	if len(x) < 0 {
		b.FailNow()
	}
}
