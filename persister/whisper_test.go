package persister

import (
	"testing"
)

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
