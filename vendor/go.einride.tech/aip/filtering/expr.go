package filtering

import (
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func Not(arg *expr.Expr) *expr.Expr {
	return Function(FunctionNot, arg)
}

func Member(operand *expr.Expr, field string) *expr.Expr {
	return &expr.Expr{
		ExprKind: &expr.Expr_SelectExpr{
			SelectExpr: &expr.Expr_Select{
				Operand: operand,
				Field:   field,
			},
		},
	}
}

func Function(name string, args ...*expr.Expr) *expr.Expr {
	return &expr.Expr{
		ExprKind: &expr.Expr_CallExpr{
			CallExpr: &expr.Expr_Call{
				Function: name,
				Args:     args,
			},
		},
	}
}

func Float(value float64) *expr.Expr {
	return &expr.Expr{
		ExprKind: &expr.Expr_ConstExpr{
			ConstExpr: &expr.Constant{
				ConstantKind: &expr.Constant_DoubleValue{
					DoubleValue: value,
				},
			},
		},
	}
}

func Duration(value time.Duration) *expr.Expr {
	return Function(FunctionDuration, String(value.String()))
}

func Timestamp(value time.Time) *expr.Expr {
	return Function(FunctionTimestamp, String(value.Format(time.RFC3339)))
}

func Int(value int64) *expr.Expr {
	return &expr.Expr{
		ExprKind: &expr.Expr_ConstExpr{
			ConstExpr: &expr.Constant{
				ConstantKind: &expr.Constant_Int64Value{
					Int64Value: value,
				},
			},
		},
	}
}

func Equals(lhs, rhs *expr.Expr) *expr.Expr {
	return Function(FunctionEquals, lhs, rhs)
}

func NotEquals(lhs, rhs *expr.Expr) *expr.Expr {
	return Function(FunctionNotEquals, lhs, rhs)
}

func Has(lhs, rhs *expr.Expr) *expr.Expr {
	return Function(FunctionHas, lhs, rhs)
}

func Or(args ...*expr.Expr) *expr.Expr {
	if len(args) <= 2 {
		return Function(FunctionOr, args...)
	}
	result := Function(FunctionOr, args[:2]...)
	for _, arg := range args[2:] {
		result = Function(FunctionOr, result, arg)
	}
	return result
}

func And(args ...*expr.Expr) *expr.Expr {
	if len(args) <= 2 {
		return Function(FunctionAnd, args...)
	}
	result := Function(FunctionAnd, args[:2]...)
	for _, arg := range args[2:] {
		result = Function(FunctionAnd, result, arg)
	}
	return result
}

func LessThan(lhs, rhs *expr.Expr) *expr.Expr {
	return Function(FunctionLessThan, lhs, rhs)
}

func LessEquals(lhs, rhs *expr.Expr) *expr.Expr {
	return Function(FunctionLessEquals, lhs, rhs)
}

func GreaterEquals(lhs, rhs *expr.Expr) *expr.Expr {
	return Function(FunctionGreaterEquals, lhs, rhs)
}

func GreaterThan(lhs, rhs *expr.Expr) *expr.Expr {
	return Function(FunctionGreaterThan, lhs, rhs)
}

func Text(text string) *expr.Expr {
	return &expr.Expr{
		ExprKind: &expr.Expr_IdentExpr{
			IdentExpr: &expr.Expr_Ident{
				Name: text,
			},
		},
	}
}

func String(s string) *expr.Expr {
	return &expr.Expr{
		ExprKind: &expr.Expr_ConstExpr{
			ConstExpr: &expr.Constant{
				ConstantKind: &expr.Constant_StringValue{
					StringValue: s,
				},
			},
		},
	}
}

func Expression(sequences ...*expr.Expr) *expr.Expr {
	return And(sequences...)
}

func Sequence(factors ...*expr.Expr) *expr.Expr {
	if len(factors) <= 2 {
		return Function(FunctionFuzzyAnd, factors...)
	}
	result := Function(FunctionFuzzyAnd, factors[:2]...)
	for _, arg := range factors[2:] {
		result = Function(FunctionFuzzyAnd, result, arg)
	}
	return result
}

func Factor(terms ...*expr.Expr) *expr.Expr {
	return Or(terms...)
}
