package filtering

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Filter represents a parsed and type-checked filter.
type Filter struct {
	CheckedExpr *expr.CheckedExpr
}
