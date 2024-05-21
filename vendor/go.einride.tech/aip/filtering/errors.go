package filtering

import (
	"errors"
	"fmt"
	"strings"
)

type filterError interface {
	Position() Position
	Message() string
	Filter() string
}

func appendFilterError(s *strings.Builder, err error) {
	var errFilter filterError
	if !errors.As(err, &errFilter) {
		return
	}
	_ = s.WriteByte('\n')
	_, _ = s.WriteString(errFilter.Filter())
	_ = s.WriteByte('\n')
	for i := int32(1); i < errFilter.Position().Column; i++ {
		_ = s.WriteByte(' ')
	}
	_, _ = s.WriteString("^ ")
	_, _ = s.WriteString(errFilter.Message())
	_, _ = s.WriteString(" (")
	_, _ = s.WriteString(errFilter.Position().String())
	_, _ = s.WriteString(")\n")
	appendFilterError(s, errors.Unwrap(err))
}

type lexError struct {
	filter   string
	position Position
	message  string
}

var _ filterError = &lexError{}

func (l *lexError) Filter() string {
	return l.filter
}

func (l *lexError) Position() Position {
	return l.position
}

func (l *lexError) Message() string {
	return l.message
}

func (l *lexError) Error() string {
	return fmt.Sprintf("%s: %s", l.position, l.message)
}

type parseError struct {
	filter   string
	position Position
	message  string
	err      error
}

func (p *parseError) Filter() string {
	return p.filter
}

func (p *parseError) Position() Position {
	return p.position
}

func (p *parseError) Message() string {
	return p.message
}

func (p *parseError) Unwrap() error {
	return p.err
}

func (p *parseError) Error() string {
	if p == nil {
		return "<nil>"
	}
	var b strings.Builder
	appendFilterError(&b, p)
	return b.String()
}

type typeError struct {
	message string
	err     error
}

func (t *typeError) Error() string {
	if t.err != nil {
		return fmt.Sprintf("%s: %v", t.message, t.err)
	}
	return t.message
}
