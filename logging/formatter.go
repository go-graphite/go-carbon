package logging

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/Sirupsen/logrus"
)

// This is to not silently overwrite `time`, `msg` and `level` fields when
// dumping it. If this code wasn't there doing:
//
// logrus.WithField("level", 1).Info("hello")
//
// Would just silently drop the user provided level. Instead with this code
// it'll logged as:
//
// {"level": "info", "fields.level": 1, "msg": "hello", "time": "..."}
//
// It's not exported because it's still using Data in an opinionated way. It's to
// avoid code duplication between the two default formatters.
func prefixFieldClashes(data logrus.Fields) {
	_, ok := data["time"]
	if ok {
		data["fields.time"] = data["time"]
	}
	_, ok = data["msg"]
	if ok {
		data["fields.msg"] = data["msg"]
	}
	_, ok = data["level"]
	if ok {
		data["fields.level"] = data["level"]
	}
}

// TextFormatter копипаста logrus.TextFormatter с косметическими изменениями
type TextFormatter struct {
	// // Set to true to bypass checking for a TTY before outputting colors.
	// ForceColors   bool
	// DisableColors bool
	// // Set to true to disable timestamp logging (useful when the output
	// // is redirected to a logging system already adding a timestamp)
	// DisableTimestamp bool
}

// Format returns formatted message text
func (f *TextFormatter) Format(entry *logrus.Entry) ([]byte, error) {

	var keys []string
	for k := range entry.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b := &bytes.Buffer{}

	prefixFieldClashes(entry.Data)

	messagePrefix := fmt.Sprintf("%s ", strings.ToUpper(entry.Level.String())[0:1])

	fmt.Fprintf(b, "[%s] %s%s",
		entry.Time.Format("2006-01-02 15:04:05"),
		messagePrefix,
		entry.Message)

	if len(keys) > 0 {
		b.WriteString(" {")

		for index, k := range keys {
			if index > 0 {
				b.WriteString(", ")
			}
			v := entry.Data[k]
			fmt.Fprintf(b, "%s=%#v", k, v)
		}

		b.WriteString("}")
	}
	b.WriteString("\n")

	return b.Bytes(), nil
}

func needsQuoting(text string) bool {
	for _, ch := range text {
		if !((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch < '9') ||
			ch == '-' || ch == '.') {
			return false
		}
	}
	return true
}

func (f *TextFormatter) appendKeyValue(b *bytes.Buffer, key, value interface{}) {
	switch value.(type) {
	case string:
		if needsQuoting(value.(string)) {
			fmt.Fprintf(b, "%v=%s ", key, value)
		} else {
			fmt.Fprintf(b, "%v=%q ", key, value)
		}
	case error:
		if needsQuoting(value.(error).Error()) {
			fmt.Fprintf(b, "%v=%s ", key, value)
		} else {
			fmt.Fprintf(b, "%v=%q ", key, value)
		}
	default:
		fmt.Fprintf(b, "%v=%v ", key, value)
	}
}
