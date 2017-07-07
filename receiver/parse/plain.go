package parse

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"unsafe"

	"github.com/lomik/go-carbon/points"
)

// https://github.com/golang/go/issues/2632#issuecomment-66061057
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func PlainLine(p []byte) ([]byte, float64, int64, error) {
	i1 := bytes.IndexByte(p, ' ')
	if i1 < 1 {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	i2 := bytes.IndexByte(p[i1+1:], ' ')
	if i2 < 1 {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}
	i2 += i1 + 1

	i3 := len(p)
	if p[i3-1] == '\n' {
		i3--
	}
	if p[i3-1] == '\r' {
		i3--
	}

	value, err := strconv.ParseFloat(unsafeString(p[i1+1:i2]), 64)
	if err != nil || math.IsNaN(value) {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	tsf, err := strconv.ParseFloat(unsafeString(p[i2+1:i3]), 64)
	if err != nil || math.IsNaN(tsf) {
		return nil, 0, 0, fmt.Errorf("bad message: %#v", string(p))
	}

	return p[:i1], value, int64(tsf), nil
}

func Plain(body []byte) ([]*points.Points, error) {
	size := len(body)
	offset := 0

	result := make([]*points.Points, 0, 4)

MainLoop:
	for offset < size {
		lineEnd := bytes.IndexByte(body[offset:size], '\n')
		if lineEnd < 0 {
			return result, errors.New("unfinished line")
		} else if lineEnd == 0 {
			// skip empty line
			offset++
			continue MainLoop
		}

		name, value, timestamp, err := PlainLine(body[offset : offset+lineEnd+1])
		offset += lineEnd + 1

		if err != nil {
			return result, err
		}

		result = append(result, points.OnePoint(string(name), value, timestamp))
	}

	return result, nil
}

// old version. for benchmarks only
func oldPlain(body []byte) ([]*points.Points, error) {
	result := make([]*points.Points, 4)

	reader := bytes.NewBuffer(body)

	for {
		line, err := reader.ReadBytes('\n')

		if err != nil && err != io.EOF {
			return result, err
		}

		if len(line) == 0 {
			break
		}

		if line[len(line)-1] != '\n' {
			return result, errors.New("unfinished line in file")
		}

		p, err := points.ParseText(string(line))

		if err != nil {
			return result, err
		}

		result = append(result, p)
	}

	return result, nil
}
