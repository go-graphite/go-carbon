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

func (t Token) Unquote() string {
	if t.Type == TokenTypeString {
		if len(t.Value) <= 2 {
			return ""
		}
		return t.Value[1 : len(t.Value)-1]
	}
	return t.Value
}
