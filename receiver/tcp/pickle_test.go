package tcp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/go-graphite/go-carbon/points"
	"github.com/lomik/zapwriter"
)

func TestPickle(t *testing.T) {
	test := newTCPTestCase(t, "pickle")
	defer test.Finish()

	test.Send("\x00\x00\x00#\x80\x02]q\x00U\x0bhello.worldq\x01J\xf8\xd3\x8eVK*\x86q\x02\x86q\x03a.")

	test.Eq(test.Receive(), points.OnePoint("hello.world", 42, 1452200952))
}

func TestBadPickle(t *testing.T) {
	defer zapwriter.Test()()

	assert := assert.New(t)
	test := newTCPTestCase(t, "pickle")
	defer test.Finish()

	test.Send("\x00\x00\x00#\x80\x02]q\x00q\x0bhello.worldq\x01Rixf8\xd3\x8eVK*\x86q\x02\x86q\x03a.")
	test.WaitClosed()
	assert.Contains(zapwriter.TestString(), "can't parse message")
}

// https://github.com/go-graphite/go-carbon/issues/30
func TestPickleMemoryError(t *testing.T) {
	defer zapwriter.Test()()

	assert := assert.New(t)
	test := newTCPTestCase(t, "pickle")
	defer test.Finish()

	test.Send("\x80\x00\x00\x01") // 2Gb message length
	test.WaitClosed()

	assert.Contains(zapwriter.TestString(), "bad message size")
}
