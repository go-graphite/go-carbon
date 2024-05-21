package filtering

import expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

// Macro represents a function that can perform macro replacements on a filter expression.
type Macro func(*Cursor)

// ApplyMacros applies the provided macros to the filter and type-checks the result against the provided declarations.
func ApplyMacros(filter Filter, declarations *Declarations, macros ...Macro) (Filter, error) {
	applyMacros(filter.CheckedExpr.GetExpr(), filter.CheckedExpr.GetSourceInfo(), macros...)
	var checker Checker
	checker.Init(filter.CheckedExpr.GetExpr(), filter.CheckedExpr.GetSourceInfo(), declarations)
	checkedExpr, err := checker.Check()
	if err != nil {
		return Filter{}, err
	}
	filter.CheckedExpr = checkedExpr
	return filter, nil
}

func applyMacros(exp *expr.Expr, sourceInfo *expr.SourceInfo, macros ...Macro) {
	nextID := maxID(exp) + 1
	Walk(func(currExpr, parentExpr *expr.Expr) bool {
		cursor := &Cursor{
			sourceInfo: sourceInfo,
			currExpr:   currExpr,
			parentExpr: parentExpr,
			nextID:     nextID,
		}
		for _, macro := range macros {
			macro(cursor)
			nextID = cursor.nextID
			if cursor.replaced {
				// Don't traverse children of replaced expr.
				return false
			}
		}
		return true
	}, exp)
}

// A Cursor describes an expression encountered while applying a Macro.
//
// The method Replace can be used to rewrite the filter.
type Cursor struct {
	parentExpr *expr.Expr
	currExpr   *expr.Expr
	sourceInfo *expr.SourceInfo
	replaced   bool
	nextID     int64
}

// Parent returns the parent of the current expression.
func (c *Cursor) Parent() (*expr.Expr, bool) {
	return c.parentExpr, c.parentExpr != nil
}

// Expr returns the current expression.
func (c *Cursor) Expr() *expr.Expr {
	return c.currExpr
}

// Replace the current expression with a new expression.
func (c *Cursor) Replace(newExpr *expr.Expr) {
	Walk(func(childExpr, _ *expr.Expr) bool {
		childExpr.Id = c.nextID
		c.nextID++
		return true
	}, newExpr)
	if c.sourceInfo.MacroCalls == nil {
		c.sourceInfo.MacroCalls = map[int64]*expr.Expr{}
	}
	c.sourceInfo.MacroCalls[newExpr.GetId()] = &expr.Expr{Id: c.currExpr.GetId(), ExprKind: c.currExpr.GetExprKind()}
	c.currExpr.Id = newExpr.GetId()
	c.currExpr.ExprKind = newExpr.GetExprKind()
	c.replaced = true
}

func maxID(exp *expr.Expr) int64 {
	var max int64
	Walk(func(_, _ *expr.Expr) bool {
		if exp.GetId() > max {
			max = exp.GetId()
		}
		return true
	}, exp)
	return max
}
