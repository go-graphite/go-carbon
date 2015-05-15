package persister

import (
	"github.com/lomik/go-carbon/points"
	//"github.com/lomik/go-whisper"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/mock"

	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewWhisper(t *testing.T) {
	inchan := make(chan *points.Points)
	schemas := WhisperSchemas{}
	aggrs := WhisperAggregation{}
	output := NewWhisper("foo", &schemas, &aggrs, inchan)
	expected := Whisper{
		in:           inchan,
		schemas:      &schemas,
		aggregation:  &aggrs,
		workersCount: 1,
		rootPath:     "foo",
	}
	assert.NotNil(t, output.exit, "Failed to init exit channel")
	// copy exit channel into out expected struct
	expected.exit = output.exit
	assert.Equal(t, *output, expected)
}

func TestSetGraphPrefix(t *testing.T) {
	fixture := Whisper{}
	fixture.SetGraphPrefix("foo.bar")
	expected := "foo.bar"
	assert.Equal(t, fixture.graphPrefix, expected)
}

func TestLoadCommited(t *testing.T) {
	/*
	   https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	   On both ARM and x86-32, it is the caller's responsibility to arrange for 64-bit alignment of 64-bit words accessed atomically. The first word in a global variable or in an allocated struct or slice can be relied upon to be 64-bit aligned.

	   Bug in go-carbon <= 0.4.3
	*/
	inchan := make(chan *points.Points)
	schemas := WhisperSchemas{}
	aggrs := WhisperAggregation{}
	p := NewWhisper("foo", &schemas, &aggrs, inchan)
	atomic.LoadUint64(&p.commited)
}

func TestSetWorkers(t *testing.T) {
	fixture := Whisper{}
	fixture.SetWorkers(10)
	expected := 10
	assert.Equal(t, fixture.workersCount, expected)
}

func TestStat(t *testing.T) {
	mock := clock.NewMock()
	app.Clock = mock
	fixture := Whisper{
		graphPrefix: "bing.bang.",
	}
	fixture.in = make(chan *points.Points)
	go func() {
		output := <-fixture.in
		expected := points.OnePoint(
			"bing.bang.persister.foo.bar",
			1.5,
			0,
		)
		assert.Equal(t, output, expected)
	}()
	fixture.Stat("foo.bar", 1.5)

}

/* This mock and associated test doesn't work quite right... I'm not sure why.

type TestWhisperFactory struct {
	mock.Mock
}

func (o TestWhisperFactory) Open(path string) (w *whisper.Whisper, err error) {
	args := o.Called(path)
	return args.Get(0).(*whisper.Whisper), args.Error(1)
}

func (o TestWhisperFactory) Create(path string, retentions whisper.Retentions, aggregationMethod whisper.AggregationMethod, xFilesFactor float32) (w *whisper.Whisper, err error) {
	args := o.Called(path, retentions, aggregationMethod, xFilesFactor)
	return args.Get(0).(*whisper.Whisper), args.Error(1)
}

func TestStore(t *testing.T) {
	factory := TestWhisperFactory{}
	app.Whisper = &factory
	inchan := make(chan *points.Points)
	schemas := WhisperSchemas{}
	aggrs := WhisperAggregation{}
	fixture := NewWhisper("foo", &schemas, &aggrs, inchan)
	p := points.OnePoint("foo.bar", 1.5, 0)
	factory.On("Open", "foo/foo/bar.wsp").Return(&whisper.Whisper{}, nil)
	fixture.store(p)
	// This fails, for unknown reasons
	//factory.AssertExpectations(t)
}
*/

// I don't see a good way to isolate this unit from store
func TestWorker(t *testing.T) {
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
	fixture := Whisper{exit: make(chan bool)}
	in := make(chan *points.Points)
	out1 := make(chan *points.Points)
	out2 := make(chan *points.Points)
	out3 := make(chan *points.Points)
	out4 := make(chan *points.Points)
	out := [](chan *points.Points){out1, out2, out3, out4}
	go fixture.shuffler(in, out)
	buckets := [4]int{0, 0, 0, 0}
	dotest := make(chan bool)
	runlength := 10000
	go func() {
		for {
			select {
			case <-out1:
				buckets[0]++
			case <-out2:
				buckets[1]++
			case <-out3:
				buckets[2]++
			case <-out4:
				buckets[3]++
			case <-dotest:
				total := 0
				for b := range buckets {
					assert.InEpsilon(t, float64(runlength)/4, buckets[b], (float64(runlength)/4)*.005, fmt.Sprintf("shuffle distribution is greater than .5% across 4 buckets after %d inputs", runlength))
					total += buckets[b]
				}
				assert.Equal(t, runlength, total, "total output of shuffle is not equal to input")

			}

		}
	}()
	randomPoints(runlength, in)
	fixture.exit <- true
	dotest <- true

}

func TestDoCheckpoint(t *testing.T) {
}

func TestStatWorker(t *testing.T) {
}

func TestStart(t *testing.T) {
}

func TestStop(t *testing.T) {
	fixture := Whisper{exit: make(chan bool)}
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(1 * time.Second)
		timeout <- true
	}()
	fixture.Stop()
	select {
	case _, ok := <-fixture.exit:
		assert.False(t, ok, "close caused a write to the exit channel")
		// a read from ch has occurred
	case _, ok := <-timeout:
		assert.False(t, ok, "close failed to close the exit channel in a reasonable time")
		// the read from ch has timed out
	}
}
