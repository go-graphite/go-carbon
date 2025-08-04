package filtering

// Token represents a token in a filter expression.
type Token struct {
	// Position of the token.
	Position Position
	// Type of the token.
	Type TokenType
	// Value of the token, if the token is a text or a string.
	Value string
}

func (t Token) Unquote() (string, error) {
	if t.Type == TokenTypeString {
		return unescape(t.Value)
	}
	return t.Value, nil
}
