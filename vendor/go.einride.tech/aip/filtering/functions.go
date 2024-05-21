package filtering

import expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

// Standard function names.
const (
	FunctionFuzzyAnd      = "FUZZY"
	FunctionAnd           = "AND"
	FunctionOr            = "OR"
	FunctionNot           = "NOT"
	FunctionEquals        = "="
	FunctionNotEquals     = "!="
	FunctionLessThan      = "<"
	FunctionLessEquals    = "<="
	FunctionGreaterEquals = ">="
	FunctionGreaterThan   = ">"
	FunctionHas           = ":"
	FunctionDuration      = "duration"
	FunctionTimestamp     = "timestamp"
)

// StandardFunctionDeclarations returns declarations for all standard functions and their standard overloads.
func StandardFunctionDeclarations() []*expr.Decl {
	return []*expr.Decl{
		StandardFunctionTimestamp(),
		StandardFunctionDuration(),
		StandardFunctionHas(),
		StandardFunctionAnd(),
		StandardFunctionOr(),
		StandardFunctionNot(),
		StandardFunctionLessThan(),
		StandardFunctionLessEquals(),
		StandardFunctionGreaterThan(),
		StandardFunctionGreaterEquals(),
		StandardFunctionEquals(),
		StandardFunctionNotEquals(),
	}
}

// Timestamp overloads.
const (
	FunctionOverloadTimestampString = FunctionTimestamp + "_string"
)

// StandardFunctionTimestamp returns a declaration for the standard `timestamp` function and all its standard overloads.
func StandardFunctionTimestamp() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionTimestamp,
		NewFunctionOverload(FunctionOverloadTimestampString, TypeTimestamp, TypeString),
	)
}

// Duration overloads.
const (
	FunctionOverloadDurationString = FunctionDuration + "_string"
)

// StandardFunctionDuration returns a declaration for the standard `duration` function and all its standard overloads.
func StandardFunctionDuration() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionDuration,
		NewFunctionOverload(FunctionOverloadDurationString, TypeDuration, TypeString),
	)
}

// Has overloads.
const (
	FunctionOverloadHasString          = FunctionHas + "_string"
	FunctionOverloadHasMapStringString = FunctionHas + "_map_string_string"
	FunctionOverloadHasListString      = FunctionHas + "_list_string"
)

// StandardFunctionHas returns a declaration for the standard `:` function and all its standard overloads.
func StandardFunctionHas() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionHas,
		NewFunctionOverload(FunctionOverloadHasString, TypeBool, TypeString, TypeString),
		// TODO: Remove this after implementing support for type parameters.
		NewFunctionOverload(FunctionOverloadHasMapStringString, TypeBool, TypeMap(TypeString, TypeString), TypeString),
		NewFunctionOverload(FunctionOverloadHasListString, TypeBool, TypeList(TypeString), TypeString),
	)
}

// And overloads.
const (
	FunctionOverloadAndBool = FunctionAnd + "_bool"
)

// StandardFunctionAnd returns a declaration for the standard `AND` function and all its standard overloads.
func StandardFunctionAnd() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionAnd,
		NewFunctionOverload(FunctionOverloadAndBool, TypeBool, TypeBool, TypeBool),
	)
}

// Or overloads.
const (
	FunctionOverloadOrBool = FunctionOr + "_bool"
)

// StandardFunctionOr returns a declaration for the standard `OR` function and all its standard overloads.
func StandardFunctionOr() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionOr,
		NewFunctionOverload(FunctionOverloadOrBool, TypeBool, TypeBool, TypeBool),
	)
}

// Not overloads.
const (
	FunctionOverloadNotBool = FunctionNot + "_bool"
)

// StandardFunctionNot returns a declaration for the standard `NOT` function and all its standard overloads.
func StandardFunctionNot() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionNot,
		NewFunctionOverload(FunctionOverloadNotBool, TypeBool, TypeBool),
	)
}

// LessThan overloads.
const (
	FunctionOverloadLessThanInt             = FunctionLessThan + "_int"
	FunctionOverloadLessThanFloat           = FunctionLessThan + "_float"
	FunctionOverloadLessThanString          = FunctionLessThan + "_string"
	FunctionOverloadLessThanTimestamp       = FunctionLessThan + "_timestamp"
	FunctionOverloadLessThanTimestampString = FunctionLessThan + "_timestamp_string"
	FunctionOverloadLessThanDuration        = FunctionLessThan + "_duration"
)

// StandardFunctionLessThan returns a declaration for the standard '<' function and all its standard overloads.
func StandardFunctionLessThan() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionLessThan,
		NewFunctionOverload(FunctionOverloadLessThanInt, TypeBool, TypeInt, TypeInt),
		NewFunctionOverload(FunctionOverloadLessThanFloat, TypeBool, TypeFloat, TypeFloat),
		NewFunctionOverload(FunctionOverloadLessThanString, TypeBool, TypeString, TypeString),
		NewFunctionOverload(FunctionOverloadLessThanTimestamp, TypeBool, TypeTimestamp, TypeTimestamp),
		NewFunctionOverload(FunctionOverloadLessThanTimestampString, TypeBool, TypeTimestamp, TypeString),
		NewFunctionOverload(FunctionOverloadLessThanDuration, TypeBool, TypeDuration, TypeDuration),
	)
}

// GreaterThan overloads.
const (
	FunctionOverloadGreaterThanInt             = FunctionGreaterThan + "_int"
	FunctionOverloadGreaterThanFloat           = FunctionGreaterThan + "_float"
	FunctionOverloadGreaterThanString          = FunctionGreaterThan + "_string"
	FunctionOverloadGreaterThanTimestamp       = FunctionGreaterThan + "_timestamp"
	FunctionOverloadGreaterThanTimestampString = FunctionGreaterThan + "_timestamp_string"
	FunctionOverloadGreaterThanDuration        = FunctionGreaterThan + "_duration"
)

// StandardFunctionGreaterThan returns a declaration for the standard '>' function and all its standard overloads.
func StandardFunctionGreaterThan() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionGreaterThan,
		NewFunctionOverload(FunctionOverloadGreaterThanInt, TypeBool, TypeInt, TypeInt),
		NewFunctionOverload(FunctionOverloadGreaterThanFloat, TypeBool, TypeFloat, TypeFloat),
		NewFunctionOverload(FunctionOverloadGreaterThanString, TypeBool, TypeString, TypeString),
		NewFunctionOverload(FunctionOverloadGreaterThanTimestamp, TypeBool, TypeTimestamp, TypeTimestamp),
		NewFunctionOverload(FunctionOverloadGreaterThanTimestampString, TypeBool, TypeTimestamp, TypeString),
		NewFunctionOverload(FunctionOverloadGreaterThanDuration, TypeBool, TypeDuration, TypeDuration),
	)
}

// LessEquals overloads.
const (
	FunctionOverloadLessEqualsInt             = FunctionLessEquals + "_int"
	FunctionOverloadLessEqualsFloat           = FunctionLessEquals + "_float"
	FunctionOverloadLessEqualsString          = FunctionLessEquals + "_string"
	FunctionOverloadLessEqualsTimestamp       = FunctionLessEquals + "_timestamp"
	FunctionOverloadLessEqualsTimestampString = FunctionLessEquals + "_timestamp_string"
	FunctionOverloadLessEqualsDuration        = FunctionLessEquals + "_duration"
)

// StandardFunctionLessEquals returns a declaration for the standard '<=' function and all its standard overloads.
func StandardFunctionLessEquals() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionLessEquals,
		NewFunctionOverload(FunctionOverloadLessEqualsInt, TypeBool, TypeInt, TypeInt),
		NewFunctionOverload(FunctionOverloadLessEqualsFloat, TypeBool, TypeFloat, TypeFloat),
		NewFunctionOverload(FunctionOverloadLessEqualsString, TypeBool, TypeString, TypeString),
		NewFunctionOverload(FunctionOverloadLessEqualsTimestamp, TypeBool, TypeTimestamp, TypeTimestamp),
		NewFunctionOverload(FunctionOverloadLessEqualsTimestampString, TypeBool, TypeTimestamp, TypeString),
		NewFunctionOverload(FunctionOverloadLessEqualsDuration, TypeBool, TypeDuration, TypeDuration),
	)
}

// GreaterEquals overloads.
const (
	FunctionOverloadGreaterEqualsInt             = FunctionGreaterEquals + "_int"
	FunctionOverloadGreaterEqualsFloat           = FunctionGreaterEquals + "_float"
	FunctionOverloadGreaterEqualsString          = FunctionGreaterEquals + "_string"
	FunctionOverloadGreaterEqualsTimestamp       = FunctionGreaterEquals + "_timestamp"
	FunctionOverloadGreaterEqualsTimestampString = FunctionGreaterEquals + "_timestamp_string"
	FunctionOverloadGreaterEqualsDuration        = FunctionGreaterEquals + "_duration"
)

// StandardFunctionGreaterEquals returns a declaration for the standard '>=' function and all its standard overloads.
func StandardFunctionGreaterEquals() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionGreaterEquals,
		NewFunctionOverload(FunctionOverloadGreaterEqualsInt, TypeBool, TypeInt, TypeInt),
		NewFunctionOverload(FunctionOverloadGreaterEqualsFloat, TypeBool, TypeFloat, TypeFloat),
		NewFunctionOverload(FunctionOverloadGreaterEqualsString, TypeBool, TypeString, TypeString),
		NewFunctionOverload(FunctionOverloadGreaterEqualsTimestamp, TypeBool, TypeTimestamp, TypeTimestamp),
		NewFunctionOverload(FunctionOverloadGreaterEqualsTimestampString, TypeBool, TypeTimestamp, TypeString),
		NewFunctionOverload(FunctionOverloadGreaterEqualsDuration, TypeBool, TypeDuration, TypeDuration),
	)
}

// Equals overloads.
const (
	FunctionOverloadEqualsBool            = FunctionEquals + "_bool"
	FunctionOverloadEqualsInt             = FunctionEquals + "_int"
	FunctionOverloadEqualsFloat           = FunctionEquals + "_float"
	FunctionOverloadEqualsString          = FunctionEquals + "_string"
	FunctionOverloadEqualsTimestamp       = FunctionEquals + "_timestamp"
	FunctionOverloadEqualsTimestampString = FunctionEquals + "_timestamp_string"
	FunctionOverloadEqualsDuration        = FunctionEquals + "_duration"
)

// StandardFunctionEquals returns a declaration for the standard '=' function and all its standard overloads.
func StandardFunctionEquals() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionEquals,
		NewFunctionOverload(FunctionOverloadEqualsBool, TypeBool, TypeBool, TypeBool),
		NewFunctionOverload(FunctionOverloadEqualsInt, TypeBool, TypeInt, TypeInt),
		NewFunctionOverload(FunctionOverloadEqualsFloat, TypeBool, TypeFloat, TypeFloat),
		NewFunctionOverload(FunctionOverloadEqualsString, TypeBool, TypeString, TypeString),
		NewFunctionOverload(FunctionOverloadEqualsTimestamp, TypeBool, TypeTimestamp, TypeTimestamp),
		NewFunctionOverload(FunctionOverloadEqualsTimestampString, TypeBool, TypeTimestamp, TypeString),
		NewFunctionOverload(FunctionOverloadEqualsDuration, TypeBool, TypeDuration, TypeDuration),
	)
}

// NotEquals overloads.
const (
	FunctionOverloadNotEqualsBool            = FunctionNotEquals + "_bool"
	FunctionOverloadNotEqualsInt             = FunctionNotEquals + "_int"
	FunctionOverloadNotEqualsFloat           = FunctionNotEquals + "_float"
	FunctionOverloadNotEqualsString          = FunctionNotEquals + "_string"
	FunctionOverloadNotEqualsTimestamp       = FunctionNotEquals + "_timestamp"
	FunctionOverloadNotEqualsTimestampString = FunctionNotEquals + "_timestamp_string"
	FunctionOverloadNotEqualsDuration        = FunctionNotEquals + "_duration"
)

// StandardFunctionNotEquals returns a declaration for the standard '!=' function and all its standard overloads.
func StandardFunctionNotEquals() *expr.Decl {
	return NewFunctionDeclaration(
		FunctionNotEquals,
		NewFunctionOverload(FunctionOverloadNotEqualsBool, TypeBool, TypeBool, TypeBool),
		NewFunctionOverload(FunctionOverloadNotEqualsInt, TypeBool, TypeInt, TypeInt),
		NewFunctionOverload(FunctionOverloadNotEqualsFloat, TypeBool, TypeFloat, TypeFloat),
		NewFunctionOverload(FunctionOverloadNotEqualsString, TypeBool, TypeString, TypeString),
		NewFunctionOverload(FunctionOverloadNotEqualsTimestamp, TypeBool, TypeTimestamp, TypeTimestamp),
		NewFunctionOverload(FunctionOverloadNotEqualsTimestampString, TypeBool, TypeTimestamp, TypeString),
		NewFunctionOverload(FunctionOverloadNotEqualsDuration, TypeBool, TypeDuration, TypeDuration),
	)
}
