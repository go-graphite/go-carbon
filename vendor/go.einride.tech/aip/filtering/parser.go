package filtering

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Parser for filter expressions.
type Parser struct {
	filter    string
	lexer     Lexer
	id        int64
	positions []int32
}

// Init (re-)initializes the parser to parse the provided filter.
func (p *Parser) Init(filter string) {
	filter = strings.TrimSpace(filter)
	*p = Parser{
		filter:    filter,
		positions: p.positions[:0],
		id:        -1,
	}
	p.lexer.Init(filter)
}

// Parse the filter.
func (p *Parser) Parse() (*expr.ParsedExpr, error) {
	e, err := p.ParseExpression()
	if err != nil {
		return nil, err
	}
	end := p.lexer.Position()
	if token, err := p.lexer.Lex(); err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
	} else {
		return nil, p.errorf(end, "unexpected trailing token %s", token.Type)
	}
	return &expr.ParsedExpr{
		Expr:       e,
		SourceInfo: p.SourceInfo(),
	}, nil
}

func (p *Parser) SourceInfo() *expr.SourceInfo {
	positions := make(map[int64]int32, len(p.positions))
	for id, position := range p.positions {
		positions[int64(id)] = position
	}
	return &expr.SourceInfo{
		LineOffsets: p.lexer.LineOffsets(),
		Positions:   positions,
	}
}

// ParseExpression parses an Expression.
//
// EBNF
//
//	expression
//	  : sequence {WS AND WS sequence}
//	  ;
func (p *Parser) ParseExpression() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "expression")
		}
	}()
	sequences := make([]*expr.Expr, 0, 1)
	for {
		_ = p.eatTokens(TokenTypeWhitespace)
		sequence, err := p.ParseSequence()
		if err != nil {
			return nil, err
		}
		sequences = append(sequences, sequence)
		if err := p.eatTokens(TokenTypeWhitespace, TokenTypeAnd, TokenTypeWhitespace); err != nil {
			break
		}
	}
	exp := sequences[0]
	for _, seq := range sequences[1:] {
		exp = parsedExpression(p.nextID(start), exp, seq)
	}
	return exp, nil
}

// ParseSequence parses a Sequence.
//
// EBNF
//
//	sequence
//	  : factor {WS factor}
//	  ;
func (p *Parser) ParseSequence() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "sequence")
		}
	}()
	factors := make([]*expr.Expr, 0, 2)
	for {
		factor, err := p.ParseFactor()
		if err != nil {
			return nil, err
		}
		factors = append(factors, factor)
		if p.sniffTokens(TokenTypeWhitespace, TokenTypeAnd) {
			break
		}
		if err := p.eatTokens(TokenTypeWhitespace); err != nil {
			break
		}
	}
	if len(factors) == 1 {
		return factors[0], nil
	}
	result := parsedSequence(p.nextID(start), factors[0], factors[1])
	for _, factor := range factors[2:] {
		result = parsedSequence(p.nextID(start), result, factor)
	}
	return result, nil
}

// ParseFactor parses a Factor.
//
// EBNF
//
//	factor
//	  : term {WS OR WS term}
//	  ;
func (p *Parser) ParseFactor() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "factor")
		}
	}()
	terms := make([]*expr.Expr, 0, 2)
	for {
		term, err := p.ParseTerm()
		if err != nil {
			return nil, err
		}
		terms = append(terms, term)
		if err := p.eatTokens(TokenTypeWhitespace, TokenTypeOr, TokenTypeWhitespace); err != nil {
			break
		}
	}
	if len(terms) == 1 {
		return terms[0], nil
	}
	result := parsedFactor(p.nextID(start), terms[0], terms[1])
	for _, factor := range terms[2:] {
		result = parsedFactor(p.nextID(start), result, factor)
	}
	return result, nil
}

// ParseTerm parses a Term.
//
// EBNF
//
//	term
//	  : [(NOT WS | MINUS)] simple
//	  ;
func (p *Parser) ParseTerm() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "term")
		}
	}()
	var not, minus bool
	if err := p.eatTokens(TokenTypeNot, TokenTypeWhitespace); err == nil {
		not = true
	} else if err := p.eatTokens(TokenTypeMinus); err == nil {
		minus = true
	}
	simple, err := p.ParseSimple()
	if err != nil {
		return nil, err
	}
	switch {
	case not, minus:
		if minus {
			// Simplify MINUS number to negation of the constant value.
			if constExpr, ok := simple.GetExprKind().(*expr.Expr_ConstExpr); ok {
				switch constantKind := constExpr.ConstExpr.GetConstantKind().(type) {
				case *expr.Constant_Int64Value:
					constantKind.Int64Value *= -1
					return simple, nil
				case *expr.Constant_DoubleValue:
					constantKind.DoubleValue *= -1
					return simple, nil
				}
			}
		}
		return parsedFunction(p.nextID(start), FunctionNot, simple), nil
	default:
		return simple, nil
	}
}

// ParseSimple parses a Simple.
//
// EBNF
//
//	simple
//	  : restriction
//	  | composite
//	  ;
func (p *Parser) ParseSimple() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "simple")
		}
	}()
	if p.sniffTokens(TokenTypeLeftParen) {
		return p.ParseComposite()
	}
	return p.ParseRestriction()
}

// ParseRestriction parses a Restriction.
//
// EBNF
//
//	restriction
//	  : comparable [comparator arg]
//	  ;
func (p *Parser) ParseRestriction() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "restriction")
		}
	}()
	comp, err := p.ParseComparable()
	if err != nil {
		return nil, err
	}
	if !(p.sniff(TokenType.IsComparator) || p.sniff(TokenTypeWhitespace.Test, TokenType.IsComparator)) {
		return comp, nil
	}
	_ = p.eatTokens(TokenTypeWhitespace)
	comparatorToken, err := p.parseToken(TokenType.IsComparator)
	if err != nil {
		return nil, err
	}
	_ = p.eatTokens(TokenTypeWhitespace)
	arg, err := p.ParseArg()
	if err != nil {
		return nil, err
	}
	// Special case for `:`
	if comparatorToken.Type == TokenTypeHas && arg.GetIdentExpr() != nil {
		// m:foo - true if m contains the key "foo".
		arg = parsedString(arg.GetId(), arg.GetIdentExpr().GetName())
	}
	return parsedFunction(p.nextID(start), comparatorToken.Type.Function(), comp, arg), nil
}

// ParseComparable parses a Comparable.
//
// EBNF
//
//	comparable
//	  : member
//	  | function
//	  | number (custom)
//	  ;
func (p *Parser) ParseComparable() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "comparable")
		}
	}()
	if function, ok := p.TryParseFunction(); ok {
		return function, nil
	}
	if number, ok := p.TryParseNumber(); ok {
		return number, nil
	}
	return p.ParseMember()
}

// ParseMember parses a Member.
//
// EBNF
//
//	member
//	  : value {DOT field}
//	  ;
//
//	value
//	  : TEXT
//	  | STRING
//	  ;
//
//	field
//	  : value
//	  | keyword
//	  | number
//	  ;
func (p *Parser) ParseMember() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "member")
		}
	}()
	valueToken, err := p.parseToken(TokenType.IsValue)
	if err != nil {
		return nil, err
	}
	if !p.sniffTokens(TokenTypeDot) {
		if valueToken.Type == TokenTypeString {
			return parsedString(p.nextID(valueToken.Position), valueToken.Unquote()), nil
		}
		return parsedText(p.nextID(valueToken.Position), valueToken.Value), nil
	}
	value := parsedText(p.nextID(valueToken.Position), valueToken.Unquote())
	_ = p.eatTokens(TokenTypeDot)
	firstFieldToken, err := p.parseToken(TokenType.IsField)
	if err != nil {
		return nil, err
	}
	member := parsedMember(p.nextID(start), value, firstFieldToken.Unquote())
	for {
		if err := p.eatTokens(TokenTypeDot); err != nil {
			break
		}
		fieldToken, err := p.parseToken(TokenType.IsField)
		if err != nil {
			return nil, err
		}
		member = parsedMember(p.nextID(start), member, fieldToken.Unquote())
	}
	return member, nil
}

// ParseFunction parses a Function.
//
// EBNF
//
//	function
//	  : name {DOT name} LPAREN [argList] RPAREN
//	  ;
//
//	name
//	  : TEXT
//	  | keyword
//	  ;
func (p *Parser) ParseFunction() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "function")
		}
	}()
	var name strings.Builder
	for {
		nameToken, err := p.parseToken(TokenType.IsName)
		if err != nil {
			return nil, err
		}
		_, _ = name.WriteString(nameToken.Unquote())
		if err := p.eatTokens(TokenTypeDot); err != nil {
			break
		}
		_ = name.WriteByte('.')
	}
	if err := p.eatTokens(TokenTypeLeftParen); err != nil {
		return nil, err
	}
	_ = p.eatTokens(TokenTypeWhitespace)
	args := make([]*expr.Expr, 0)
	for !p.sniffTokens(TokenTypeRightParen) {
		arg, err := p.ParseArg()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		_ = p.eatTokens(TokenTypeWhitespace)
		if err := p.eatTokens(TokenTypeComma); err != nil {
			break
		}
		_ = p.eatTokens(TokenTypeWhitespace)
	}
	_ = p.eatTokens(TokenTypeWhitespace)
	if err := p.eatTokens(TokenTypeRightParen); err != nil {
		return nil, err
	}
	return parsedFunction(p.nextID(start), name.String(), args...), nil
}

func (p *Parser) TryParseFunction() (*expr.Expr, bool) {
	start := *p
	function, err := p.ParseFunction()
	if err != nil {
		*p = start
		return nil, false
	}
	return function, true
}

// ParseComposite parses a Composite.
//
// EBNF
//
//	composite
//	  : LPAREN expression RPAREN
//	  ;
func (p *Parser) ParseComposite() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "composite")
		}
	}()
	if err := p.eatTokens(TokenTypeLeftParen); err != nil {
		return nil, err
	}
	_ = p.eatTokens(TokenTypeWhitespace)
	expression, err := p.ParseExpression()
	if err != nil {
		return nil, err
	}
	_ = p.eatTokens(TokenTypeWhitespace)
	if err := p.eatTokens(TokenTypeRightParen); err != nil {
		return nil, err
	}
	return expression, nil
}

// ParseNumber parses a number.
//
// EBNF
//
//	number
//	  : float
//	  | int
//	  ;
//
//	float
//	  : MINUS? (NUMBER DOT NUMBER* | DOT NUMBER) EXP?
//	  ;
//
//	int
//	  : MINUS? NUMBER
//	  | MINUS? HEX
//	  ;
func (p *Parser) ParseNumber() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "number")
		}
	}()
	if float, ok := p.TryParseFloat(); ok {
		return float, nil
	}
	return p.ParseInt()
}

func (p *Parser) TryParseNumber() (*expr.Expr, bool) {
	start := *p
	result, err := p.ParseNumber()
	if err != nil {
		*p = start
		return nil, false
	}
	return result, true
}

// ParseFloat parses a float.
//
// EBNF
//
//	float
//	  : MINUS? (NUMBER DOT NUMBER* | DOT NUMBER) EXP?
//	  ;
func (p *Parser) ParseFloat() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "float")
		}
	}()
	var minusToken Token
	if p.sniffTokens(TokenTypeMinus) {
		minusToken, _ = p.lexer.Lex()
	}
	var intToken Token
	var hasIntToken bool
	if p.sniffTokens(TokenTypeNumber) {
		intToken, err = p.lexer.Lex()
		if err != nil {
			return nil, err
		}
		hasIntToken = true
	}
	dotToken, err := p.parseToken(TokenTypeDot.Test)
	if err != nil {
		return nil, err
	}
	var fractionToken Token
	var hasFractionToken bool
	if p.sniffTokens(TokenTypeNumber) {
		fractionToken, err = p.lexer.Lex()
		if err != nil {
			return nil, err
		}
		hasFractionToken = true
	}
	// TODO: Support exponents.
	if !hasFractionToken && !hasIntToken {
		return nil, p.errorf(start, "expected int or fraction")
	}
	var stringValue strings.Builder
	stringValue.Grow(
		len(minusToken.Value) + len(intToken.Value) + len(dotToken.Value) + len(fractionToken.Value),
	)
	_, _ = stringValue.WriteString(minusToken.Value)
	_, _ = stringValue.WriteString(intToken.Value)
	_, _ = stringValue.WriteString(dotToken.Value)
	_, _ = stringValue.WriteString(fractionToken.Value)
	floatValue, err := strconv.ParseFloat(stringValue.String(), 64)
	if err != nil {
		return nil, err
	}
	return parsedFloat(p.nextID(start), floatValue), nil
}

func (p *Parser) TryParseFloat() (*expr.Expr, bool) {
	start := *p
	result, err := p.ParseFloat()
	if err != nil {
		*p = start
		return nil, false
	}
	return result, true
}

// ParseInt parses an int.
//
// EBNF
//
//	int
//	  : MINUS? NUMBER
//	  | MINUS? HEX
//	  ;
func (p *Parser) ParseInt() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "int")
		}
	}()
	minus := p.eatTokens(TokenTypeMinus) == nil
	token, err := p.parseToken(TokenTypeNumber.Test, TokenTypeHexNumber.Test)
	if err != nil {
		return nil, err
	}
	intValue, err := strconv.ParseInt(token.Value, 0, 64)
	if err != nil {
		return nil, err
	}
	if minus {
		intValue *= -1
	}
	return parsedInt(p.nextID(start), intValue), nil
}

// ParseArg parses an Arg.
//
// EBNF
//
//	arg
//	  : comparable
//	  | composite
//	  ;
func (p *Parser) ParseArg() (_ *expr.Expr, err error) {
	start := p.lexer.Position()
	defer func() {
		if err != nil {
			err = p.wrapf(err, start, "arg")
		}
	}()
	if p.sniffTokens(TokenTypeLeftParen) {
		return p.ParseComposite()
	}
	return p.ParseComparable()
}

func (p *Parser) parseToken(fns ...func(TokenType) bool) (Token, error) {
	start := p.lexer.Position()
	token, err := p.lexer.Lex()
	if err != nil {
		return Token{}, p.wrapf(err, start, "parse token")
	}
	for _, fn := range fns {
		if fn(token.Type) {
			return token, nil
		}
	}
	return Token{}, p.errorf(token.Position, "unexpected token %s", token.Type)
}

func (p *Parser) sniff(fns ...func(TokenType) bool) bool {
	start := *p
	defer func() {
		*p = start
	}()
	for _, fn := range fns {
		if token, err := p.lexer.Lex(); err != nil || !fn(token.Type) {
			return false
		}
	}
	return true
}

func (p *Parser) sniffTokens(wantTokenTypes ...TokenType) bool {
	start := *p
	defer func() {
		*p = start
	}()
	for _, wantTokenType := range wantTokenTypes {
		if token, err := p.lexer.Lex(); err != nil || token.Type != wantTokenType {
			return false
		}
	}
	return true
}

func (p *Parser) eatTokens(wantTokenTypes ...TokenType) error {
	start := *p
	for _, wantTokenType := range wantTokenTypes {
		if token, err := p.lexer.Lex(); err != nil || token.Type != wantTokenType {
			*p = start
			return p.errorf(start.lexer.Position(), "expected %s", wantTokenType)
		}
	}
	return nil
}

func (p *Parser) errorf(position Position, format string, args ...interface{}) error {
	return &parseError{
		filter:   p.filter,
		position: position,
		message:  fmt.Sprintf(format, args...),
	}
}

func (p *Parser) wrapf(err error, position Position, format string, args ...interface{}) error {
	return &parseError{
		filter:   p.filter,
		position: position,
		message:  fmt.Sprintf(format, args...),
		err:      err,
	}
}

func (p *Parser) nextID(position Position) int64 {
	p.id++
	p.positions = append(p.positions, position.Offset)
	return p.id
}
