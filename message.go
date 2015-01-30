package carbon

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Message with metric value from carbon clients
type Message struct {
	Name  string
	Value float64
	Time  time.Time
}

// NewMessage creates new instance of Message
func NewMessage() *Message {
	return &Message{}
}

// ParseTextMessage parse text protocol message
//  host.metric.value 42 1422641531\n
func ParseTextMessage(line string) (*Message, error) {

	row := strings.Split(strings.Trim(line, "\n \t\r"), " ")
	if len(row) != 3 {
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	if row[0] == "" {
		// @TODO: add more checks
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	value, err := strconv.ParseFloat(row[1], 64)

	if err != nil {
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	tsf, err := strconv.ParseFloat(row[2], 64)

	if err != nil {
		// @TODO: add more checks
		return nil, fmt.Errorf("bad message: %#v", line)
	}

	msg := NewMessage()

	msg.Name = row[0]
	msg.Value = value
	msg.Time = time.Unix(int64(tsf), 0)

	return msg, nil
}
