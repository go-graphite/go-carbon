package tcp

import (
	"testing"

	"github.com/go-graphite/go-carbon/points"
)

func TestProtobuf(t *testing.T) {
	test := newTCPTestCase(t, "protobuf")
	defer test.Finish()

	test.Send("\x00\x00\x00 \n\x1e\n\x0bhello.world\x12\x0f\x08\xf8\xa7\xbb\xb4\x05\x11\x00\x00\x00\x00\x00\x00E@")

	test.Eq(test.Receive(), points.OnePoint("hello.world", 42, 1452200952))
}
