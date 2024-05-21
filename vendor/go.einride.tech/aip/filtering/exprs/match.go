package exprs

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Matcher returns true if the expr matches the predicate.
type Matcher func(exp *expr.Expr) bool

// MatchString matches an expr.Constant_StringValue with an
// exact value.
func MatchString(s string) Matcher {
	var s2 string
	m := MatchAnyString(&s2)
	return func(exp *expr.Expr) bool {
		return m(exp) && s == s2
	}
}

// MatchAnyString matches an expr.Constant_StringValue with any
// value. The value of the expr is populated in argument value.
func MatchAnyString(value *string) Matcher {
	return func(exp *expr.Expr) bool {
		cons := exp.GetConstExpr()
		if cons == nil {
			return false
		}
		if _, ok := cons.GetConstantKind().(*expr.Constant_StringValue); !ok {
			return false
		}
		*value = cons.GetStringValue()
		return true
	}
}

// MatchFloat matches an expr.Constant_DoubleValue with an exact value.
func MatchFloat(value float64) Matcher {
	var f2 float64
	m := MatchAnyFloat(&f2)
	return func(exp *expr.Expr) bool {
		return m(exp) && value == f2
	}
}

// MatchAnyFloat matches an expr.Constant_DoubleValue with any value.
// The value of the expr is populated in argument value.
func MatchAnyFloat(value *float64) Matcher {
	return func(exp *expr.Expr) bool {
		cons := exp.GetConstExpr()
		if cons == nil {
			return false
		}
		if _, ok := cons.GetConstantKind().(*expr.Constant_DoubleValue); !ok {
			return false
		}
		*value = cons.GetDoubleValue()
		return true
	}
}

// MatchInt matches an expr.Constant_Int64Value with an exact value.
func MatchInt(value int64) Matcher {
	var i2 int64
	m := MatchAnyInt(&i2)
	return func(exp *expr.Expr) bool {
		return m(exp) && value == i2
	}
}

// MatchAnyInt matches an expr.Constant_Int64Value with any value.
// The value of the expr is populated in argument value.
func MatchAnyInt(value *int64) Matcher {
	return func(exp *expr.Expr) bool {
		cons := exp.GetConstExpr()
		if cons == nil {
			return false
		}
		if _, ok := cons.GetConstantKind().(*expr.Constant_Int64Value); !ok {
			return false
		}
		*value = cons.GetInt64Value()
		return true
	}
}

// MatchText matches an expr.Expr_Ident where the name
// of the ident matches an exact value.
func MatchText(text string) Matcher {
	var t2 string
	m := MatchAnyText(&t2)
	return func(exp *expr.Expr) bool {
		return m(exp) && text == t2
	}
}

// MatchAnyText matches an expr.Expr_Ident with any name.
// The name of the expr is populated in argument text.
func MatchAnyText(text *string) Matcher {
	return func(exp *expr.Expr) bool {
		ident := exp.GetIdentExpr()
		if ident == nil {
			return false
		}
		*text = ident.GetName()
		return true
	}
}

// MatchMember matches an expr.Expr_Select where the operand matches
// the argument operand, and the field matches argument field.
func MatchMember(operand Matcher, field string) Matcher {
	var f2 string
	m := MatchAnyMember(operand, &f2)
	return func(exp *expr.Expr) bool {
		return m(exp) && field == f2
	}
}

// MatchAnyMember matches an expr.Expr_Select where the operand matches
// the argument operand. The field of the expr is populated in argument field.
func MatchAnyMember(operand Matcher, field *string) Matcher {
	return func(exp *expr.Expr) bool {
		sel := exp.GetSelectExpr()
		if sel == nil {
			return false
		}
		if !operand(sel.GetOperand()) {
			return false
		}
		*field = sel.GetField()
		return true
	}
}

// MatchFunction matches an expr.Expr_Call where the name of the
// expr matches argument name, and arguments of the function matches
// the provided args (length must match).
func MatchFunction(name string, args ...Matcher) Matcher {
	var n2 string
	m := MatchAnyFunction(&n2, args...)
	return func(exp *expr.Expr) bool {
		return m(exp) && name == n2
	}
}

// MatchAnyFunction matches an expr.Expr_Call where the provided args
// matches the function arguments. The name of the function is populated
// in argument name.
func MatchAnyFunction(name *string, args ...Matcher) Matcher {
	return func(exp *expr.Expr) bool {
		call := exp.GetCallExpr()
		if call == nil {
			return false
		}
		if len(call.GetArgs()) != len(args) {
			return false
		}
		for i, a := range call.GetArgs() {
			if !args[i](a) {
				return false
			}
		}
		*name = call.GetFunction()
		return true
	}
}

// MatchAny matches any expr.Expr. The expr is populated in argument e.
func MatchAny(e **expr.Expr) Matcher {
	return func(exp *expr.Expr) bool {
		*e = exp
		return true
	}
}
