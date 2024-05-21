package filtering

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Primitive types.
//
//nolint:gochecknoglobals
var (
	TypeInt    = &expr.Type{TypeKind: &expr.Type_Primitive{Primitive: expr.Type_INT64}}
	TypeFloat  = &expr.Type{TypeKind: &expr.Type_Primitive{Primitive: expr.Type_DOUBLE}}
	TypeString = &expr.Type{TypeKind: &expr.Type_Primitive{Primitive: expr.Type_STRING}}
	TypeBool   = &expr.Type{TypeKind: &expr.Type_Primitive{Primitive: expr.Type_BOOL}}
)

// TypeMap returns the type for a map with the provided key and value types.
func TypeMap(keyType, valueType *expr.Type) *expr.Type {
	return &expr.Type{
		TypeKind: &expr.Type_MapType_{
			MapType: &expr.Type_MapType{
				KeyType:   keyType,
				ValueType: valueType,
			},
		},
	}
}

// TypeList returns the type for a list with the provided element type.
func TypeList(elementType *expr.Type) *expr.Type {
	return &expr.Type{
		TypeKind: &expr.Type_ListType_{
			ListType: &expr.Type_ListType{
				ElemType: elementType,
			},
		},
	}
}

// TypeEnum returns the type of a protobuf enum.
func TypeEnum(enumType protoreflect.EnumType) *expr.Type {
	return &expr.Type{
		TypeKind: &expr.Type_MessageType{
			MessageType: string(enumType.Descriptor().FullName()),
		},
	}
}

// Well-known types.
//
//nolint:gochecknoglobals
var (
	TypeDuration  = &expr.Type{TypeKind: &expr.Type_WellKnown{WellKnown: expr.Type_DURATION}}
	TypeTimestamp = &expr.Type{TypeKind: &expr.Type_WellKnown{WellKnown: expr.Type_TIMESTAMP}}
)
