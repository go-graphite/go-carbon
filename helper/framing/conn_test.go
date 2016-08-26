package framing_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
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

	port := l.Addr().(*net.TCPAddr).Port

	go func() {
		conn, err := net.Dial("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			t.Fatal(err)
		}

		framed, err := framing.NewConn(conn, size, endianess)
		if err != nil {
			t.Fatal(err)
		}
		defer framed.Close()

		for i := 1; i <= 2; i++ {
			if _, err := fmt.Fprintf(framed, message); err != nil {
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

	first_b := make([]byte, 6)
	first_n, first_err := framed.Read(first_b)
	if first_n != 4 || first_err != nil {
		t.Fatalf("first_n = %d, first_err = %#v", first_n, first_err)
	}
	if !bytes.Equal(first_b, []byte{0, 1, 2, 3, 0, 0}) {
		t.Fatal(first_b)
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

	first_b := make([]byte, 4)
	first_n, first_err := framed.Read(first_b)
	if first_n != 4 || first_err != nil {
		t.Fatal(first_n, first_err)
	}
	if !bytes.Equal(first_b, []byte{0, 1, 2, 3}) {
		t.Fatal(first_b)
	}

	second_b := make([]byte, 3)
	second_n, second_err := framed.Read(second_b)
	if second_n != 2 || second_err != nil {
		t.Fatal(second_n, second_err)
	}
	if !bytes.Equal(second_b, []byte{4, 5, 0}) {
		t.Fatal(second_b)
	}

	third_b := make([]byte, 5)
	third_n, third_err := framed.Read(third_b)
	if third_n != 4 || third_err != nil {
		t.Fatal(third_n, third_err)
	}
	if !bytes.Equal(third_b, []byte{7, 8, 9, 10, 0}) {
		t.Fatal(third_b)
	}

	fourth_b := make([]byte, 1)
	fourth_n, fourth_err := framed.Read(fourth_b)
	if fourth_n != 0 || fourth_err != io.EOF {
		t.Fatal(fourth_n, fourth_err)
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
