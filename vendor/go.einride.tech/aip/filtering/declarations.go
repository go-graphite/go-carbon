package filtering

import (
	"fmt"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// NewStringConstant creates a new string constant.
func NewStringConstant(value string) *expr.Constant {
	return &expr.Constant{
		ConstantKind: &expr.Constant_StringValue{
			StringValue: value,
		},
	}
}

// NewFunctionDeclaration creates a new function declaration.
func NewFunctionDeclaration(name string, overloads ...*expr.Decl_FunctionDecl_Overload) *expr.Decl {
	return &expr.Decl{
		Name: name,
		DeclKind: &expr.Decl_Function{
			Function: &expr.Decl_FunctionDecl{
				Overloads: overloads,
			},
		},
	}
}

// NewFunctionOverload creates a new function overload.
func NewFunctionOverload(id string, result *expr.Type, params ...*expr.Type) *expr.Decl_FunctionDecl_Overload {
	return &expr.Decl_FunctionDecl_Overload{
		OverloadId: id,
		ResultType: result,
		Params:     params,
	}
}

// NewIdentDeclaration creates a new ident declaration.
func NewIdentDeclaration(name string, identType *expr.Type) *expr.Decl {
	return &expr.Decl{
		Name: name,
		DeclKind: &expr.Decl_Ident{
			Ident: &expr.Decl_IdentDecl{
				Type: identType,
			},
		},
	}
}

// NewConstantDeclaration creates a new constant ident declaration.
func NewConstantDeclaration(name string, constantType *expr.Type, constantValue *expr.Constant) *expr.Decl {
	return &expr.Decl{
		Name: name,
		DeclKind: &expr.Decl_Ident{
			Ident: &expr.Decl_IdentDecl{
				Type:  constantType,
				Value: constantValue,
			},
		},
	}
}

// Declarations contain declarations for type-checking filter expressions.
type Declarations struct {
	idents    map[string]*expr.Decl
	functions map[string]*expr.Decl
	enums     map[string]protoreflect.EnumType
}

// DeclarationOption configures Declarations.
type DeclarationOption func(*Declarations) error

// DeclareStandardFunction is a DeclarationOption that declares all standard functions and their overloads.
func DeclareStandardFunctions() DeclarationOption {
	return func(declarations *Declarations) error {
		for _, declaration := range StandardFunctionDeclarations() {
			if err := declarations.declare(declaration); err != nil {
				return err
			}
		}
		return nil
	}
}

// DeclareFunction is a DeclarationOption that declares a single function and its overloads.
func DeclareFunction(name string, overloads ...*expr.Decl_FunctionDecl_Overload) DeclarationOption {
	return func(declarations *Declarations) error {
		return declarations.declareFunction(name, overloads...)
	}
}

// DeclareIdent is a DeclarationOption that declares a single ident.
func DeclareIdent(name string, t *expr.Type) DeclarationOption {
	return func(declarations *Declarations) error {
		return declarations.declareIdent(name, t)
	}
}

func DeclareEnumIdent(name string, enumType protoreflect.EnumType) DeclarationOption {
	return func(declarations *Declarations) error {
		return declarations.declareEnumIdent(name, enumType)
	}
}

// NewDeclarations creates a new set of Declarations for filter expression type-checking.
func NewDeclarations(opts ...DeclarationOption) (*Declarations, error) {
	d := &Declarations{
		idents:    make(map[string]*expr.Decl),
		functions: make(map[string]*expr.Decl),
		enums:     make(map[string]protoreflect.EnumType),
	}
	for _, opt := range opts {
		if err := opt(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *Declarations) LookupIdent(name string) (*expr.Decl, bool) {
	result, ok := d.idents[name]
	return result, ok
}

func (d *Declarations) LookupFunction(name string) (*expr.Decl, bool) {
	result, ok := d.functions[name]
	return result, ok
}

func (d *Declarations) LookupEnumIdent(name string) (protoreflect.EnumType, bool) {
	result, ok := d.enums[name]
	return result, ok
}

func (d *Declarations) declareIdent(name string, t *expr.Type) error {
	if _, ok := d.idents[name]; ok {
		return fmt.Errorf("redeclaration of %s", name)
	}
	d.idents[name] = NewIdentDeclaration(name, t)
	return nil
}

func (d *Declarations) declareConstant(name string, constantType *expr.Type, constantValue *expr.Constant) error {
	constantDecl := NewConstantDeclaration(name, constantType, constantValue)
	if existingIdent, ok := d.idents[name]; ok {
		if !proto.Equal(constantDecl, existingIdent) {
			return fmt.Errorf("redeclaration of %s", name)
		}
		return nil
	}
	d.idents[name] = constantDecl
	return nil
}

func (d *Declarations) declareEnumIdent(name string, enumType protoreflect.EnumType) error {
	if _, ok := d.enums[name]; ok {
		return fmt.Errorf("redeclaration of %s", name)
	}
	d.enums[name] = enumType
	enumIdentType := TypeEnum(enumType)
	if err := d.declareIdent(name, enumIdentType); err != nil {
		return err
	}
	for _, fn := range []string{
		FunctionEquals,
		FunctionNotEquals,
	} {
		if err := d.declareFunction(
			fn,
			NewFunctionOverload(fn+"_"+enumIdentType.GetMessageType(), TypeBool, enumIdentType, enumIdentType),
		); err != nil {
			return err
		}
	}
	values := enumType.Descriptor().Values()
	for i := 0; i < values.Len(); i++ {
		valueName := string(values.Get(i).Name())
		if err := d.declareConstant(valueName, enumIdentType, NewStringConstant(valueName)); err != nil {
			return err
		}
	}
	return nil
}

func (d *Declarations) declareFunction(name string, overloads ...*expr.Decl_FunctionDecl_Overload) error {
	decl, ok := d.functions[name]
	if !ok {
		decl = NewFunctionDeclaration(name)
		d.functions[name] = decl
	}
	function := decl.GetFunction()
NewOverloadLoop:
	for _, newOverload := range overloads {
		for _, existingOverload := range function.GetOverloads() {
			if newOverload.GetOverloadId() == existingOverload.GetOverloadId() {
				if !proto.Equal(newOverload, existingOverload) {
					return fmt.Errorf("redeclaration of overload %s", existingOverload.GetOverloadId())
				}
				continue NewOverloadLoop
			}
		}
		function.Overloads = append(function.Overloads, newOverload)
	}
	return nil
}

func (d *Declarations) declare(decl *expr.Decl) error {
	switch decl.GetDeclKind().(type) {
	case *expr.Decl_Function:
		return d.declareFunction(decl.GetName(), decl.GetFunction().GetOverloads()...)
	case *expr.Decl_Ident:
		if decl.GetIdent().GetValue() != nil {
			return d.declareConstant(decl.GetName(), decl.GetIdent().GetType(), decl.GetIdent().GetValue())
		}
		return d.declareIdent(decl.GetName(), decl.GetIdent().GetType())
	default:
		return fmt.Errorf("unsupported declaration kind")
	}
}
