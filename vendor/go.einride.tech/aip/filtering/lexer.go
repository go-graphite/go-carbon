package filtering

import (
	"errors"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

// Lexer is a filter expression lexer.
type Lexer struct {
	filter      string
	tokenStart  Position
	tokenEnd    Position
	lineOffsets []int32
}

// Init initializes the lexer with the provided filter.
func (l *Lexer) Init(filter string) {
	*l = Lexer{
		filter:      filter,
		tokenStart:  Position{Offset: 0, Line: 1, Column: 1},
		tokenEnd:    Position{Offset: 0, Line: 1, Column: 1},
		lineOffsets: l.lineOffsets[:0],
	}
}

// Lex returns the next token in the filter expression, or io.EOF when there are no more tokens to lex.
func (l *Lexer) Lex() (Token, error) {
	r, err := l.nextRune()
	if err != nil {
		return Token{}, err
	}
	switch r {
	// Single-character operator?
	case '(', ')', '-', '.', '=', ':', ',':
		return l.emit(TokenType(l.tokenValue()))
	// Two-character operator?
	case '<', '>', '!':
		if l.sniffRune('=') {
			_, _ = l.nextRune()
		}
		return l.emit(TokenType(l.tokenValue()))
	// String?
	case '\'', '"':
		quote := r
		escaped := false
		for {
			r2, err := l.nextRune()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return Token{}, l.errorf("unterminated string")
				}
				return Token{}, err
			}
			if r2 == '\\' && !escaped {
				escaped = true
				continue
			}
			if r2 == quote && !escaped {
				return l.emit(TokenTypeString)
			}
			escaped = false
		}
	// Number?
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// Hex number?
		if l.sniffRune('x') {
			_, _ = l.nextRune()
			for l.sniff(isHexDigit) {
				_, _ = l.nextRune()
			}
			return l.emit(TokenTypeHexNumber)
		}
		for l.sniff(unicode.IsDigit) {
			_, _ = l.nextRune()
		}
		return l.emit(TokenTypeNumber)
	}
	// Space?
	if unicode.IsSpace(r) {
		for l.sniff(unicode.IsSpace) {
			_, _ = l.nextRune()
		}
		return l.emit(TokenTypeWhitespace)
	}
	// Text or keyword.
	for l.sniff(isText) {
		_, _ = l.nextRune()
	}
	// Keyword?
	if tokenType := TokenType(l.tokenValue()); tokenType.IsKeyword() {
		return l.emit(tokenType)
	}
	// Text.
	return l.emit(TokenTypeText)
}

// Position returns the current position of the lexer.
func (l *Lexer) Position() Position {
	return l.tokenStart
}

// LineOffsets returns a monotonically increasing list of character offsets where newlines appear.
func (l *Lexer) LineOffsets() []int32 {
	return l.lineOffsets
}

func (l *Lexer) emit(t TokenType) (Token, error) {
	token := Token{
		Position: l.tokenStart,
		Type:     t,
		Value:    l.tokenValue(),
	}
	l.tokenStart = l.tokenEnd
	return token, nil
}

func (l *Lexer) tokenValue() string {
	return l.filter[l.tokenStart.Offset:l.tokenEnd.Offset]
}

func (l *Lexer) remainingFilter() string {
	return l.filter[l.tokenEnd.Offset:]
}

func (l *Lexer) nextRune() (rune, error) {
	r, n := utf8.DecodeRuneInString(l.remainingFilter())
	switch {
	case n == 0:
		return r, io.EOF
	// If the input rune was the replacement character (`\uFFFD`) preserve it.
	//
	// If the input rune was invalid (and converted to replacement character)
	// return an error.
	case r == utf8.RuneError && (n != 3 || l.remainingFilter()[:3] != "\xef\xbf\xbd"):
		return r, l.errorf("invalid UTF-8")
	}
	if r == '\n' {
		l.lineOffsets = append(l.lineOffsets, l.tokenEnd.Offset)
		l.tokenEnd.Line++
		l.tokenEnd.Column = 1
	} else {
		l.tokenEnd.Column++
	}
	l.tokenEnd.Offset += int32(n) // #nosec G115
	return r, nil
}

func (l *Lexer) sniff(wantFns ...func(rune) bool) bool {
	remaining := l.remainingFilter()
	for _, wantFn := range wantFns {
		r, n := utf8.DecodeRuneInString(l.remainingFilter())
		if !wantFn(r) {
			return false
		}
		remaining = remaining[n:]
	}
	return true
}

func (l *Lexer) sniffRune(want rune) bool {
	r, _ := utf8.DecodeRuneInString(l.remainingFilter())
	return r == want
}

func (l *Lexer) errorf(format string, args ...interface{}) error {
	return &lexError{
		filter:   l.filter,
		position: l.tokenStart,
		message:  fmt.Sprintf(format, args...),
	}
}

func isText(r rune) bool {
	switch r {
	case utf8.RuneError, '(', ')', '-', '.', '=', ':', '<', '>', '!', ',':
		return false
	}
	return !unicode.IsSpace(r)
}

func isHexDigit(r rune) bool {
	return unicode.Is(unicode.ASCII_Hex_Digit, r)
}
