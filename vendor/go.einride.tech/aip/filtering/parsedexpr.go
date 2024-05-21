package filtering

import expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

func parsedFloat(id int64, value float64) *expr.Expr {
	result := Float(value)
	result.Id = id
	return result
}

func parsedInt(id, value int64) *expr.Expr {
	result := Int(value)
	result.Id = id
	return result
}

func parsedText(id int64, s string) *expr.Expr {
	result := Text(s)
	result.Id = id
	return result
}

func parsedString(id int64, s string) *expr.Expr {
	result := String(s)
	result.Id = id
	return result
}

func parsedExpression(id int64, sequences ...*expr.Expr) *expr.Expr {
	result := Expression(sequences...)
	result.Id = id
	return result
}

func parsedSequence(id int64, factor1, factor2 *expr.Expr) *expr.Expr {
	return parsedFunction(id, FunctionFuzzyAnd, factor1, factor2)
}

func parsedFactor(id int64, term1, term2 *expr.Expr) *expr.Expr {
	result := Or(term1, term2)
	result.Id = id
	return result
}

func parsedMember(id int64, operand *expr.Expr, field string) *expr.Expr {
	result := Member(operand, field)
	result.Id = id
	return result
}

func parsedFunction(id int64, name string, args ...*expr.Expr) *expr.Expr {
	result := Function(name, args...)
	result.Id = id
	return result
}
