// +build gofuzz

package ogórek

import (
	"bytes"
)

func Fuzz(data []byte) int {
	buf := bytes.NewBuffer(data)
	dec := NewDecoder(buf)
	_, err := dec.Decode()
	if err != nil {
		return 0
	}
	return 1
}
