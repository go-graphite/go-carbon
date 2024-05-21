package filtering

import expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

// WalkFunc is called for every expression while calling Walk.
// Return false to stop Walk.
type WalkFunc func(currExpr, parentExpr *expr.Expr) bool

// Walk an expression in depth-first order.
func Walk(fn WalkFunc, currExpr *expr.Expr) {
	walk(fn, currExpr, nil)
}

func walk(fn WalkFunc, currExpr, parentExpr *expr.Expr) {
	if fn == nil || currExpr == nil {
		return
	}
	if ok := fn(currExpr, parentExpr); !ok {
		return
	}
	switch v := currExpr.GetExprKind().(type) {
	case *expr.Expr_ConstExpr, *expr.Expr_IdentExpr:
		// Nothing to do here.
	case *expr.Expr_SelectExpr:
		walk(fn, v.SelectExpr.GetOperand(), currExpr)
	case *expr.Expr_CallExpr:
		walk(fn, v.CallExpr.GetTarget(), currExpr)
		for _, arg := range v.CallExpr.GetArgs() {
			walk(fn, arg, currExpr)
		}
	case *expr.Expr_ListExpr:
		for _, el := range v.ListExpr.GetElements() {
			walk(fn, el, currExpr)
		}
	case *expr.Expr_StructExpr:
		for _, entry := range v.StructExpr.GetEntries() {
			walk(fn, entry.GetValue(), currExpr)
		}
	case *expr.Expr_ComprehensionExpr:
		walk(fn, v.ComprehensionExpr.GetIterRange(), currExpr)
		walk(fn, v.ComprehensionExpr.GetAccuInit(), currExpr)
		walk(fn, v.ComprehensionExpr.GetLoopCondition(), currExpr)
		walk(fn, v.ComprehensionExpr.GetLoopStep(), currExpr)
		walk(fn, v.ComprehensionExpr.GetResult(), currExpr)
	}
}
