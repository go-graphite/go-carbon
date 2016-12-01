package framing_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lomik/go-carbon/helper/framing"
)

func Test1BigEndian(t *testing.T) {
	run(t, 1, binary.BigEndian)
}

func Test2BigEndian(t *testing.T) {
	run(t, 2, binary.BigEndian)
}

func Test4BigEndian(t *testing.T) {
	run(t, 4, binary.BigEndian)
}

func Test1LittleEndian(t *testing.T) {
	run(t, 1, binary.LittleEndian)
}

func Test2LittleEndian(t *testing.T) {
	run(t, 2, binary.LittleEndian)
}

func Test4LittleEndian(t *testing.T) {
	run(t, 4, binary.LittleEndian)
}

func run(t *testing.T, size byte, endianess binary.ByteOrder) {
	message := "13 bytes long"

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}

		framed, err := framing.NewConn(conn, size, endianess)
		if err != nil {
			t.Fatal(err)
		}
		defer framed.Close()

		for i := 1; i <= 2; i++ {
			if _, err := fmt.Fprint(framed, message); err != nil {
				t.Fatal(err)
			}
		}
	}()

	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	framed, err := framing.NewConn(conn, size, endianess)
	if err != nil {
		t.Fatal(err)
	}
	defer framed.Close()

	buf, err := framed.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	if msg := string(buf[:]); msg != message {
		t.Fatalf("Unexpected message:\nGot:\t\t%s\nExpected:\t%s\n", msg, message)
	}

	fixed := make([]byte, 20) // More than 13
	n, err := framed.Read(fixed)
	if err != nil {
		t.Fatal(err)
	}
	if n != 13 {
		t.Fatal("Frame is not of correct size")
	}
	if msg := string(fixed[:13]); msg != message {
		t.Fatalf("Unexpected message:\nGot:\t\t%s\nExpected:\t%s\n", msg, message)
	}
}

func TestInvalidFrameSiz(t *testing.T) {
	_, err := framing.NewConn(nil, 3, nil)
	if err != framing.ErrPrefixLength {
		t.Fail()
	}
}

func TestPacketTooLarge(t *testing.T) {
	max := uint(16)
	size := byte(4)
	conn, err := framing.NewConn(nil, size, nil)
	conn.MaxFrameSize = max
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, max+1)
	_, err = conn.Write(b)
	if err != framing.ErrFrameTooLarge {
		t.Fail()
	}
}

func TestMessageSmallerThanBuffer(t *testing.T) {
	conn := &fake{ReadWriter: bytes.NewBuffer([]byte{4, 0, 1, 2, 3})}
	framed, _ := framing.NewConn(&fake{ReadWriter: conn}, 1, nil)

	b1 := make([]byte, 6)
	n1, err1 := framed.Read(b1)
	if n1 != 4 || err1 != nil {
		t.Fatalf("n1 = %d, err1 = %#v", n1, err1)
	}
	if !bytes.Equal(b1, []byte{0, 1, 2, 3, 0, 0}) {
		t.Fatal(b1)
	}
}

func TestMessageBiggerThanBuffer(t *testing.T) {
	conn := &fake{ReadWriter: bytes.NewBuffer([]byte{
		6, 0, 1, 2, 3, 4, 5,
		// [message 1      ]
		//          ^ first read ends
		4, 7, 8, 9, 10,
		// [message 2 ]
		// ^ second read ends
		//          ^ third read ends
	})}
	framed, _ := framing.NewConn(&fake{ReadWriter: conn}, 1, nil)

	b1 := make([]byte, 4)
	n1, err1 := framed.Read(b1)
	if n1 != 4 || err1 != nil {
		t.Fatal(n1, err1)
	}
	if !bytes.Equal(b1, []byte{0, 1, 2, 3}) {
		t.Fatal(b1)
	}

	b2 := make([]byte, 3)
	n2, err2 := framed.Read(b2)
	if n2 != 2 || err2 != nil {
		t.Fatal(n2, err2)
	}
	if !bytes.Equal(b2, []byte{4, 5, 0}) {
		t.Fatal(b2)
	}

	b3 := make([]byte, 5)
	n3, err3 := framed.Read(b3)
	if n3 != 4 || err3 != nil {
		t.Fatal(n3, err3)
	}
	if !bytes.Equal(b3, []byte{7, 8, 9, 10, 0}) {
		t.Fatal(b3)
	}

	b4 := make([]byte, 1)
	n4, err4 := framed.Read(b4)
	if n4 != 0 || err4 != io.EOF {
		t.Fatal(n4, err4)
	}

}

func TestInnerReadSizeError(t *testing.T) {
	conn := &fake{
		ReadWriter: bytes.NewBuffer([]byte{}),
		err:        errors.New("InnerReadSizeError"),
	}
	framed, _ := framing.NewConn(conn, 1, nil)
	b := make([]byte, 4)
	_, err := framed.Read(b)
	if err.Error() != "InnerReadSizeError" {
		t.Fatal(err)
	}
}

func TestInnerReadError(t *testing.T) {
	conn := &fake{
		ReadWriter: bytes.NewBuffer([]byte{4}),
		err:        errors.New("InnerReadError"),
	}
	framed, _ := framing.NewConn(conn, 1, nil)
	b := make([]byte, 4)
	_, err := framed.Read(b)
	if err.Error() != "InnerReadError" {
		t.Fatal(err)
	}
}

func TestInnerReadFrameSizeError(t *testing.T) {
	conn := &fake{
		ReadWriter: bytes.NewBuffer([]byte{}),
		err:        errors.New("InnerReadFrameSizeError"),
	}
	framed, _ := framing.NewConn(conn, 1, nil)
	_, err := framed.ReadFrame()
	if err.Error() != "InnerReadFrameSizeError" {
		t.Fatal(err)
	}
}

func TestInnerReadFrameError(t *testing.T) {
	conn := &fake{
		ReadWriter: bytes.NewBuffer([]byte{4}),
		err:        errors.New("InnerReadFrameError"),
	}
	framed, _ := framing.NewConn(conn, 1, nil)
	_, err := framed.ReadFrame()
	if err.Error() != "InnerReadFrameError" {
		t.Fatal(err)
	}
}

func TestWriteError(t *testing.T) {
	conn := &fake{
		ReadWriter: bytes.NewBuffer([]byte{}),
		err:        errors.New("WriteError"),
	}
	framed, _ := framing.NewConn(conn, 1, nil)
	_, err := framed.Write([]byte{0})
	if err.Error() != "WriteError" {
		t.Fatal(err)
	}
}

type fake struct {
	io.ReadWriter
	err error
}

func (f *fake) Read(b []byte) (int, error) {
	n, err := f.ReadWriter.Read(b)
	if n == 0 {
		if f.err != nil {
			return 0, f.err
		}
		return 0, err
	}
	return n, err
}

func (f *fake) Write(b []byte) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.ReadWriter.Write(b)
}

func (f *fake) Close() error {
	return nil
}

func (f *fake) LocalAddr() net.Addr {
	return nil
}

func (f *fake) RemoteAddr() net.Addr {
	return nil
}

func (f *fake) SetDeadline(time time.Time) error {
	return nil
}

func (f *fake) SetReadDeadline(time time.Time) error {
	return nil
}

func (f *fake) SetWriteDeadline(time time.Time) error {
	return nil
}
