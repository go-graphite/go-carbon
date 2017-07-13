package tcp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lomik/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func TestPickle(t *testing.T) {
	test := newTCPTestCase(t, "pickle")
	defer test.Finish()

	test.Send("\x00\x00\x00#\x80\x02]q\x00U\x0bhello.worldq\x01J\xf8\xd3\x8eVK*\x86q\x02\x86q\x03a.")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42, 1452200952))
	default:
		t.Fatalf("Message #0 not received")
	}
}

func TestBadPickle(t *testing.T) {
	defer zapwriter.Test()()

	assert := assert.New(t)
	test := newTCPTestCase(t, "pickle")
	defer test.Finish()

	test.Send("\x00\x00\x00#\x80\x02]q\x00q\x0bhello.worldq\x01Rixf8\xd3\x8eVK*\x86q\x02\x86q\x03a.")
	time.Sleep(10 * time.Millisecond)
	assert.Contains(zapwriter.TestString(), "can't parse message")
}

// https://github.com/lomik/go-carbon/issues/30
func TestPickleMemoryError(t *testing.T) {
	defer zapwriter.Test()()

	assert := assert.New(t)
	test := newTCPTestCase(t, "pickle")
	defer test.Finish()

	test.Send("\x80\x00\x00\x01") // 2Gb message length
	time.Sleep(10 * time.Millisecond)

	assert.Contains(zapwriter.TestString(), "bad message size")
}
