package persister

import (
	"math/rand"
	"testing"

	"github.com/lomik/go-carbon/points"
)

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

func TestMaxCreatesSoftThrottlingZero(t *testing.T) {
	p := &Whisper{
		maxCreatesTicker: NewSoftThrottleTicker(0),
	}

	exp := throttlingOff
	got := p.maxCreatesThrottling()
	if got != exp {
		t.Errorf("Expected %s, got %s", exp, got)
	}
}

func TestMaxCreatesHardThrottlingZero(t *testing.T) {
	p := &Whisper{
		maxCreatesTicker:        NewHardThrottleTicker(0),
		hardMaxCreatesPerSecond: true,
	}

	exp := throttlingHard
	got := p.maxCreatesThrottling()
	if got != exp {
		t.Errorf("Expected %s, got %s", exp, got)
	}
}

func TestMaxCreatesHardThrottlingOne(t *testing.T) {
	p := &Whisper{
		maxCreatesTicker:        NewHardThrottleTicker(1),
		hardMaxCreatesPerSecond: true,
	}

	exp := throttlingOff
	got := p.maxCreatesThrottling()
	if got != exp {
		t.Errorf("Expected %s, got %s", exp, got)
	}

	exp = throttlingHard
	got = p.maxCreatesThrottling()
	if got != exp {
		t.Errorf("Expected %s, got %s", exp, got)
	}
}

func TestMaxCreatesHardThrottlingMany(t *testing.T) {
	p := &Whisper{
		maxCreatesTicker:        NewHardThrottleTicker(1),
		hardMaxCreatesPerSecond: true,
	}

	exp := throttlingOff
	got := p.maxCreatesThrottling()
	if got != exp {
		t.Errorf("Expected %s, got %s", exp, got)
	}

	for i := 0; i < 10; i++ {
		exp = throttlingHard
		got = p.maxCreatesThrottling()
		if got != exp {
			t.Errorf("Run %d: Expected %s, got %s", i, exp, got)
		}
	}
}
