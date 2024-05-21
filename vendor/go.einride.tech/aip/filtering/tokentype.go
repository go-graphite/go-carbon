package filtering

// TokenType represents the type of a filter expression token.
//
// See: https://google.aip.dev/assets/misc/ebnf-filtering.txt
type TokenType string

// Value token types.
const (
	TokenTypeWhitespace TokenType = "WS"
	TokenTypeText       TokenType = "TEXT"
	TokenTypeString     TokenType = "STRING"
)

// Keyword token types.
const (
	TokenTypeNot TokenType = "NOT"
	TokenTypeAnd TokenType = "AND"
	TokenTypeOr  TokenType = "OR"
)

// Numeric token types.
const (
	TokenTypeNumber    TokenType = "NUM"
	TokenTypeHexNumber TokenType = "HEX"
)

// Operator token types.
const (
	TokenTypeLeftParen     TokenType = "("
	TokenTypeRightParen    TokenType = ")"
	TokenTypeMinus         TokenType = "-"
	TokenTypeDot           TokenType = "."
	TokenTypeEquals        TokenType = "="
	TokenTypeHas           TokenType = ":"
	TokenTypeLessThan      TokenType = "<"
	TokenTypeGreaterThan   TokenType = ">"
	TokenTypeExclaim       TokenType = "!"
	TokenTypeComma         TokenType = ","
	TokenTypeLessEquals    TokenType = "<="
	TokenTypeGreaterEquals TokenType = ">="
	TokenTypeNotEquals     TokenType = "!="
)

func (t TokenType) Function() string {
	if t.IsComparator() || t.IsKeyword() {
		return string(t)
	}
	return ""
}

// IsField returns true if the token is a valid value.
func (t TokenType) IsValue() bool {
	switch t {
	case TokenTypeText, TokenTypeString:
		return true
	default:
		return false
	}
}

// IsField returns true if the token is a valid field.
func (t TokenType) IsField() bool {
	return t.IsValue() || t.IsKeyword() || t == TokenTypeNumber
}

// IsName returns true if the token is a valid name.
func (t TokenType) IsName() bool {
	return t == TokenTypeText || t.IsKeyword()
}

func (t TokenType) Test(other TokenType) bool {
	return t == other
}

// IsKeyword returns true if the token is a valid keyword.
func (t TokenType) IsKeyword() bool {
	switch t {
	case TokenTypeNot, TokenTypeAnd, TokenTypeOr:
		return true
	default:
		return false
	}
}

// IsComparator returns true if the token is a valid comparator.
func (t TokenType) IsComparator() bool {
	switch t {
	case TokenTypeLessEquals,
		TokenTypeLessThan,
		TokenTypeGreaterEquals,
		TokenTypeGreaterThan,
		TokenTypeNotEquals,
		TokenTypeEquals,
		TokenTypeHas:
		return true
	default:
		return false
	}
}
