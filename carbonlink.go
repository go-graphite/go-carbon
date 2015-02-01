package carbon

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hydrogen18/stalecucumber"
)

// Request ...
type Request struct {
	Type   string
	Metric string
	Key    string
	Value  string
}

// NewRequest creates instance of Request
func NewRequest() *Request {
	return &Request{}
}

// ParseRequest from pickle encoded data
func ParseRequest(data []byte) (*Request, error) {
	reader := bytes.NewReader(data)
	req := NewRequest()

	if err := stalecucumber.UnpackInto(req).From(stalecucumber.Unpickle(reader)); err != nil {
		return nil, err
	}

	return req, nil
}

// ReadRequest from socket/buffer and unpack
func ReadRequest(reader io.Reader) (*Request, error) {
	var msgLen uint32

	if err := binary.Read(reader, binary.BigEndian, &msgLen); err != nil {
		return nil, fmt.Errorf("Can't read message length: %s", err.Error())
	}

	data := make([]byte, msgLen)

	if err := binary.Read(reader, binary.BigEndian, data); err != nil {
		return nil, fmt.Errorf("Can't read message body: %s", err.Error())
	}

	return ParseRequest(data)
}
