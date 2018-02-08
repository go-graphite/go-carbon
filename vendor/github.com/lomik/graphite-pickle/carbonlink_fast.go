package pickle

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/*
Thanks Maxim Ivanov https://github.com/redbaron for this implementation
*/

func pickleMaybeMemo(b *[]byte) bool { //"consumes" memo tokens
	if len(*b) > 1 && (*b)[0] == 'q' {
		*b = (*b)[2:]
	}
	return true
}

func pickleGetStr(buf *[]byte) (string, bool) {
	if len(*buf) == 0 {
		return "", false
	}
	b := *buf

	if b[0] == 'U' { // short string
		if len(b) >= 2 {
			sLen := int(uint8(b[1]))
			if len(b) >= 2+sLen {
				*buf = b[2+sLen:]
				return string(b[2 : 2+sLen]), true
			}
		}
	} else if b[0] == 'T' || b[0] == 'X' { //long string or utf8 string
		if len(b) >= 5 {
			sLen := int(binary.LittleEndian.Uint32(b[1:]))
			if len(b) >= 5+sLen {
				*buf = b[5+sLen:]
				return string(b[5 : 5+sLen]), true
			}
		}
	}
	return "", false
}

func expectBytes(b *[]byte, v []byte) bool {
	if bytes.Index(*b, v) == 0 {
		*b = (*b)[len(v):]
		return true
	} else {
		return false
	}
}

var badErr error = fmt.Errorf("Bad pickle message")

// ParseCarbonlinkRequestFast from pickle encoded data. Supports only protocol=2
func ParseCarbonlinkRequestFast(d []byte) (*CarbonlinkRequest, error) {

	if !(expectBytes(&d, []byte("\x80\x02}")) && pickleMaybeMemo(&d) && expectBytes(&d, []byte("("))) {
		return nil, badErr
	}

	req := NewCarbonlinkRequest()

	var Metric, Type string
	var ok bool

	if expectBytes(&d, []byte("U\x06metric")) {
		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}
		if Metric, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !(pickleMaybeMemo(&d) && expectBytes(&d, []byte("U\x04type")) && pickleMaybeMemo(&d)) {
			return nil, badErr
		}

		if Type, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}

		req.Metric = Metric
		req.Type = Type
	} else if expectBytes(&d, []byte("U\x04type")) {
		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}

		if Type, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !(pickleMaybeMemo(&d) && expectBytes(&d, []byte("U\x06metric")) && pickleMaybeMemo(&d)) {
			return nil, badErr
		}

		if Metric, ok = pickleGetStr(&d); !ok {
			return nil, badErr
		}

		if !pickleMaybeMemo(&d) {
			return nil, badErr
		}

		req.Metric = Metric
		req.Type = Type
	} else {
		return nil, badErr
	}

	return req, nil
}
