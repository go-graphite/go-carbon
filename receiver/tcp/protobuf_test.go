package tcp

import (
	"testing"
	"time"

	"github.com/lomik/go-carbon/points"
)

func TestProtobuf(t *testing.T) {
	test := newTCPTestCase(t, "protobuf")
	defer test.Finish()

	test.Send("\x00\x00\x00 \n\x1e\n\x0bhello.world\x12\x0f\x08\xf8\xa7\xbb\xb4\x05\x11\x00\x00\x00\x00\x00\x00E@")

	time.Sleep(10 * time.Millisecond)

	select {
	case msg := <-test.rcvChan:
		test.Eq(msg, points.OnePoint("hello.world", 42, 1452200952))
	default:
		t.Fatalf("Message #0 not received")
	}
}
