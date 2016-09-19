package persister

import (
	"github.com/lomik/go-carbon/points"

	"github.com/stretchr/testify/assert"

	"math/rand"
	"testing"
)

func TestNewWhisper(t *testing.T) {
	schemas := WhisperSchemas{}
	aggrs := WhisperAggregation{}
	output := NewWhisper("foo", schemas, &aggrs, nil, nil)
	expected := Whisper{
		schemas:      schemas,
		aggregation:  &aggrs,
		workersCount: 1,
		rootPath:     "foo",
	}
	assert.Equal(t, *output, expected)
}

func TestSetWorkers(t *testing.T) {
	fixture := Whisper{}
	fixture.SetWorkers(10)
	expected := 10
	assert.Equal(t, fixture.workersCount, expected)
}

func randomPoints(num int, out chan *points.Points) {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var i int
	for i = 0; i < num; i++ {
		b := make([]rune, 32)
		for i := range b {
			b[i] = letters[rand.Intn(len(letters))]
		}
		metric := string(b)
		p := points.OnePoint(metric, rand.Float64(), rand.Int63())
		out <- p
	}
}
