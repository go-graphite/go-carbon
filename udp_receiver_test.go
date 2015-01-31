package carbon

import (
	"net"
	"testing"
	"time"

	"github.com/k0kubun/pp"
)

func TestUdpReceiver(t *testing.T) {

	addr, err := net.ResolveUDPAddr("udp", "localhost:1818")
	if err != nil {
		t.Fatal(err)
	}

	rcv := make(chan *Message, 128)
	receiver := NewUdpReceiver(rcv)

	defer receiver.Stop()

	if err = receiver.Listen(addr); err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("hello.world 42.15 1422698155\n")); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 1; i++ {
		select {
		case msg := <-rcv:
			pp.Println(msg)
		default:
			t.Fatalf("Message #%d not received", i)
		}
	}

}
