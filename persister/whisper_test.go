package persister

import (
	"sync"

	"github.com/lomik/go-carbon/points"

	"github.com/stretchr/testify/assert"

	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestNewWhisper(t *testing.T) {
	inchan := make(chan *points.Points)
	schemas := WhisperSchemas{}
	aggrs := WhisperAggregation{}
	output := NewWhisper("foo", schemas, &aggrs, inchan, nil)
	expected := Whisper{
		in:           inchan,
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

func TestShuffler(t *testing.T) {
	rand.Seed(time.Now().Unix())
	fixture := Whisper{}
	in := make(chan *points.Points)
	out1 := make(chan *points.Points)
	out2 := make(chan *points.Points)
	out3 := make(chan *points.Points)
	out4 := make(chan *points.Points)
	out := [](chan *points.Points){out1, out2, out3, out4}
	go fixture.shuffler(in, out, nil)
	buckets := [4]int{0, 0, 0, 0}
	runlength := 10000

	var wg sync.WaitGroup
	wg.Add(4)

	for index, _ := range out {
		outChan := out[index]
		i := index
		go func() {
			for {
				_, ok := <-outChan
				if !ok {
					break
				}
				buckets[i]++
			}
			wg.Done()
		}()
	}

	randomPoints(runlength, in)

	close(in)
	wg.Wait()

	total := 0
	for b := range buckets {
		assert.InEpsilon(t, float64(runlength)/4, buckets[b], (float64(runlength)/4)*.005, fmt.Sprintf("shuffle distribution is greater than .5%% across 4 buckets after %d inputs", runlength))
		total += buckets[b]
	}
	assert.Equal(t, runlength, total, "total output of shuffle is not equal to input")

}
