package filtering

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// The following code is adapted from
// https://github.com/google/cel-go/blob/f6d3c92171c2c8732a8d0a4b24d6729df4261520/parser/unescape.go#L1-L237.

// Unescape takes a quoted string, unquotes, and unescapes it.
//
// This function performs escaping compatible with GoogleSQL.
func unescape(value string) (string, error) {
	// All strings normalize newlines to the \n representation.
	value = newlineNormalizer.Replace(value)
	n := len(value)

	// Nothing to unescape / decode.
	if n < 2 {
		return value, errors.New("unable to unescape string")
	}

	// Quoted string of some form, must have same first and last char.
	if value[0] != value[n-1] || (value[0] != '"' && value[0] != '\'') {
		return value, errors.New("unable to unescape string")
	}

	value = value[1 : n-1]
	// If there is nothing to escape, then return.
	if !strings.ContainsRune(value, '\\') {
		return value, nil
	}

	// Otherwise the string contains escape characters.
	// The following logic is adapted from `strconv/quote.go`
	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*n/2)
	for len(value) > 0 {
		c, encode, rest, err := unescapeChar(value, false)
		if err != nil {
			return "", err
		}
		value = rest
		if c < utf8.RuneSelf || !encode {
			buf = append(buf, byte(c))
		} else {
			n := utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
		}
	}
	return string(buf), nil
}

// unescapeChar takes a string input and returns the following info:
//
//	value - the escaped unicode rune at the front of the string.
//	encode - the value should be unicode-encoded
//	tail - the remainder of the input string.
//	err - error value, if the character could not be unescaped.
//
// When encode is true the return value may still fit within a single byte,
// but unicode encoding is attempted which is more expensive than when the
// value is known to self-represent as a single byte.
//
// If isBytes is set, unescape as a bytes literal so octal and hex escapes
// represent byte values, not unicode code points.
func unescapeChar(s string, isBytes bool) (value rune, encode bool, tail string, err error) {
	// 1. Character is not an escape sequence.
	switch c := s[0]; {
	case c >= utf8.RuneSelf:
		r, size := utf8.DecodeRuneInString(s)
		return r, true, s[size:], nil
	case c != '\\':
		return rune(s[0]), false, s[1:], nil
	}

	// 2. Last character is the start of an escape sequence.
	if len(s) <= 1 {
		return value, encode, tail, errors.New("unable to unescape string, found '\\' as last character")
	}

	c := s[1]
	s = s[2:]
	// 3. Common escape sequences shared with Google SQL
	switch c {
	case 'a':
		value = '\a'
	case 'b':
		value = '\b'
	case 'f':
		value = '\f'
	case 'n':
		value = '\n'
	case 'r':
		value = '\r'
	case 't':
		value = '\t'
	case 'v':
		value = '\v'
	case '\\':
		value = '\\'
	case '\'':
		value = '\''
	case '"':
		value = '"'
	case '`':
		value = '`'
	case '?':
		value = '?'

	// 4. Unicode escape sequences, reproduced from `strconv/quote.go`
	case 'x', 'X', 'u', 'U':
		n := 0
		encode = true
		switch c {
		case 'x', 'X':
			n = 2
			encode = !isBytes
		case 'u':
			n = 4
			if isBytes {
				return value, encode, tail, errors.New("unable to unescape string")
			}
		case 'U':
			n = 8
			if isBytes {
				return value, encode, tail, errors.New("unable to unescape string")
			}
		}
		var v rune
		if len(s) < n {
			return value, encode, tail, errors.New("unable to unescape string")
		}
		for j := 0; j < n; j++ {
			x, ok := unhex(s[j])
			if !ok {
				return value, encode, tail, errors.New("unable to unescape string")
			}
			v = v<<4 | x
		}
		s = s[n:]
		if !isBytes && !utf8.ValidRune(v) {
			return value, encode, tail, errors.New("invalid unicode code point")
		}
		value = v

	// 5. Octal escape sequences, must be three digits \[0-3][0-7][0-7]
	case '0', '1', '2', '3':
		if len(s) < 2 {
			return value, encode, tail, errors.New("unable to unescape octal sequence in string")
		}
		v := rune(c - '0')
		for j := 0; j < 2; j++ {
			x := s[j]
			if x < '0' || x > '7' {
				return value, encode, tail, errors.New("unable to unescape octal sequence in string")
			}
			v = v*8 + rune(x-'0')
		}
		if !isBytes && !utf8.ValidRune(v) {
			return value, encode, tail, errors.New("invalid unicode code point")
		}
		value = v
		s = s[2:]
		encode = !isBytes

		// Unknown escape sequence.
	default:
		err = errors.New("unable to unescape string")
	}

	tail = s
	return value, encode, tail, err
}

func unhex(b byte) (rune, bool) {
	c := rune(b)
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

//nolint:gochecknoglobals
var newlineNormalizer = strings.NewReplacer("\r\n", "\n", "\r", "\n")
