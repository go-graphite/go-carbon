package filtering

import (
	"fmt"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
)

type Checker struct {
	declarations *Declarations
	expr         *expr.Expr
	sourceInfo   *expr.SourceInfo
	typeMap      map[int64]*expr.Type
}

func (c *Checker) Init(exp *expr.Expr, sourceInfo *expr.SourceInfo, declarations *Declarations) {
	*c = Checker{
		expr:         exp,
		declarations: declarations,
		sourceInfo:   sourceInfo,
		typeMap:      make(map[int64]*expr.Type, len(sourceInfo.GetPositions())),
	}
}

func (c *Checker) Check() (*expr.CheckedExpr, error) {
	if err := c.checkExpr(c.expr); err != nil {
		return nil, err
	}
	resultType, ok := c.getType(c.expr)
	if !ok {
		return nil, c.errorf(c.expr, "unknown result type")
	}
	if !proto.Equal(resultType, TypeBool) {
		return nil, c.errorf(c.expr, "non-bool result type")
	}
	return &expr.CheckedExpr{
		TypeMap:    c.typeMap,
		SourceInfo: c.sourceInfo,
		Expr:       c.expr,
	}, nil
}

func (c *Checker) checkExpr(e *expr.Expr) error {
	if e == nil {
		return nil
	}
	switch e.GetExprKind().(type) {
	case *expr.Expr_ConstExpr:
		switch e.GetConstExpr().GetConstantKind().(type) {
		case *expr.Constant_BoolValue:
			return c.checkBoolLiteral(e)
		case *expr.Constant_DoubleValue:
			return c.checkDoubleLiteral(e)
		case *expr.Constant_Int64Value:
			return c.checkInt64Literal(e)
		case *expr.Constant_StringValue:
			return c.checkStringLiteral(e)
		default:
			return c.errorf(e, "unsupported constant kind")
		}
	case *expr.Expr_IdentExpr:
		return c.checkIdentExpr(e)
	case *expr.Expr_SelectExpr:
		return c.checkSelectExpr(e)
	case *expr.Expr_CallExpr:
		return c.checkCallExpr(e)
	default:
		return c.errorf(e, "unsupported expr kind")
	}
}

func (c *Checker) checkIdentExpr(e *expr.Expr) error {
	identExpr := e.GetIdentExpr()
	ident, ok := c.declarations.LookupIdent(identExpr.GetName())
	if !ok {
		return c.errorf(e, "undeclared identifier '%s'", identExpr.GetName())
	}
	if err := c.setType(e, ident.GetIdent().GetType()); err != nil {
		return c.wrapf(err, e, "identifier '%s'", identExpr.GetName())
	}
	return nil
}

func (c *Checker) checkSelectExpr(e *expr.Expr) (err error) {
	defer func() {
		if err != nil {
			err = c.wrapf(err, e, "check select expr")
		}
	}()
	if qualifiedName, ok := toQualifiedName(e); ok {
		if ident, ok := c.declarations.LookupIdent(qualifiedName); ok {
			return c.setType(e, ident.GetIdent().GetType())
		}
	}
	selectExpr := e.GetSelectExpr()
	if selectExpr.GetOperand() == nil {
		return c.errorf(e, "missing operand")
	}
	if err := c.checkExpr(selectExpr.GetOperand()); err != nil {
		return err
	}
	operandType, ok := c.getType(selectExpr.GetOperand())
	if !ok {
		return c.errorf(e, "failed to get operand type")
	}
	switch operandType.GetTypeKind().(type) {
	case *expr.Type_MapType_:
		return c.setType(e, operandType.GetMapType().GetValueType())
	default:
		return c.errorf(e, "unsupported operand type")
	}
}

func (c *Checker) checkCallExpr(e *expr.Expr) (err error) {
	defer func() {
		if err != nil {
			err = c.wrapf(err, e, "check call expr")
		}
	}()
	callExpr := e.GetCallExpr()
	for _, arg := range callExpr.GetArgs() {
		if err := c.checkExpr(arg); err != nil {
			return err
		}
	}
	functionDeclaration, ok := c.declarations.LookupFunction(callExpr.GetFunction())
	if !ok {
		return c.errorf(e, "undeclared function '%s'", callExpr.GetFunction())
	}
	functionOverload, err := c.resolveCallExprFunctionOverload(e, functionDeclaration)
	if err != nil {
		return err
	}
	if err := c.checkCallExprBuiltinFunctionOverloads(e, functionOverload); err != nil {
		return err
	}
	return c.setType(e, functionOverload.GetResultType())
}

func (c *Checker) resolveCallExprFunctionOverload(
	e *expr.Expr,
	functionDeclaration *expr.Decl,
) (*expr.Decl_FunctionDecl_Overload, error) {
	callExpr := e.GetCallExpr()
	for _, overload := range functionDeclaration.GetFunction().GetOverloads() {
		if len(callExpr.GetArgs()) != len(overload.GetParams()) {
			continue
		}
		if len(overload.GetTypeParams()) == 0 {
			allTypesMatch := true
			for i, param := range overload.GetParams() {
				argType, ok := c.getType(callExpr.GetArgs()[i])
				if !ok {
					return nil, c.errorf(callExpr.GetArgs()[i], "unknown type")
				}
				if !proto.Equal(argType, param) {
					allTypesMatch = false
					break
				}
			}
			if allTypesMatch {
				return overload, nil
			}
		}
		// TODO: Add support for type parameters.
	}
	var argTypes []string
	for _, arg := range callExpr.GetArgs() {
		t, ok := c.getType(arg)
		if !ok {
			argTypes = append(argTypes, "UNKNOWN")
		} else {
			argTypes = append(argTypes, t.String())
		}
	}
	return nil, c.errorf(e, "no matching overload found for calling '%s' with %s", callExpr.GetFunction(), argTypes)
}

func (c *Checker) checkCallExprBuiltinFunctionOverloads(
	e *expr.Expr,
	functionOverload *expr.Decl_FunctionDecl_Overload,
) error {
	callExpr := e.GetCallExpr()
	switch functionOverload.GetOverloadId() {
	case FunctionOverloadTimestampString:
		if constExpr := callExpr.GetArgs()[0].GetConstExpr(); constExpr != nil {
			if _, err := time.Parse(time.RFC3339, constExpr.GetStringValue()); err != nil {
				return c.errorf(callExpr.GetArgs()[0], "invalid timestamp. Should be in RFC3339 format")
			}
		}
	case FunctionOverloadDurationString:
		if constExpr := callExpr.GetArgs()[0].GetConstExpr(); constExpr != nil {
			if _, err := time.ParseDuration(constExpr.GetStringValue()); err != nil {
				return c.errorf(callExpr.GetArgs()[0], "invalid duration")
			}
		}
	case FunctionOverloadLessThanTimestampString,
		FunctionOverloadGreaterThanTimestampString,
		FunctionOverloadLessEqualsTimestampString,
		FunctionOverloadGreaterEqualsTimestampString,
		FunctionOverloadEqualsTimestampString,
		FunctionOverloadNotEqualsTimestampString:
		if constExpr := callExpr.GetArgs()[1].GetConstExpr(); constExpr != nil {
			if _, err := time.Parse(time.RFC3339, constExpr.GetStringValue()); err != nil {
				return c.errorf(callExpr.GetArgs()[0], "invalid timestamp. Should be in RFC3339 format")
			}
		}
	}
	return nil
}

func (c *Checker) checkInt64Literal(e *expr.Expr) error {
	return c.setType(e, TypeInt)
}

func (c *Checker) checkStringLiteral(e *expr.Expr) error {
	return c.setType(e, TypeString)
}

func (c *Checker) checkDoubleLiteral(e *expr.Expr) error {
	return c.setType(e, TypeFloat)
}

func (c *Checker) checkBoolLiteral(e *expr.Expr) error {
	return c.setType(e, TypeBool)
}

func (c *Checker) errorf(_ *expr.Expr, format string, args ...interface{}) error {
	// TODO: Include the provided expr.
	return &typeError{
		message: fmt.Sprintf(format, args...),
	}
}

func (c *Checker) wrapf(err error, _ *expr.Expr, format string, args ...interface{}) error {
	// TODO: Include the provided expr.
	return &typeError{
		message: fmt.Sprintf(format, args...),
		err:     err,
	}
}

func (c *Checker) setType(e *expr.Expr, t *expr.Type) error {
	if existingT, ok := c.typeMap[e.GetId()]; ok && !proto.Equal(t, existingT) {
		return c.errorf(e, "type conflict between %s and %s", t, existingT)
	}
	c.typeMap[e.GetId()] = t
	return nil
}

func (c *Checker) getType(e *expr.Expr) (*expr.Type, bool) {
	t, ok := c.typeMap[e.GetId()]
	if !ok {
		return nil, false
	}
	return t, true
}

func toQualifiedName(e *expr.Expr) (string, bool) {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		return kind.IdentExpr.GetName(), true
	case *expr.Expr_SelectExpr:
		if kind.SelectExpr.GetTestOnly() {
			return "", false
		}
		parent, ok := toQualifiedName(kind.SelectExpr.GetOperand())
		if !ok {
			return "", false
		}
		return parent + "." + kind.SelectExpr.GetField(), true
	default:
		return "", false
	}
}
