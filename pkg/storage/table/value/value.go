package value

import (
	"encoding/binary"
	"math"
)

type ValueType string

const (
	TypeBoolean ValueType = "boolean"
	TypeInt32   ValueType = "int32"
	TypeFloat32 ValueType = "float32"
	TypeVarChar ValueType = "varchar"
	TypeInvalid ValueType = "invalid"
)

type Value struct {
	Type ValueType
	Data []byte
}

func NewValueTypeFromUserType(typeName string) ValueType {
	switch typeName {
	case "bool":
		return TypeBoolean
	case "int":
		return TypeInt32
	case "float":
		return TypeFloat32
	case "char":
		return TypeVarChar
	default:
		return TypeInvalid
	}
}

func (v *Value) GetValue() []byte {
	return v.Data
}

func NewValue(type_id ValueType, data []byte) *Value {
	return &Value{
		Type: type_id,
		Data: data,
	}
}

func (typeId *ValueType) TypeSize() uint16 {
	switch *typeId {
	case TypeBoolean:
		return 1
	case TypeInt32:
		return 4
	case TypeFloat32:
		return 4
	default:
		panic("unrechable. varchar should use Column.StorageSize")
	}
}

func (v *ValueType) AsString() string {
	return string(*v)
}

func (v *Value) AsBoolean() bool {
	return v.Data[0] != 0
}

func (v *Value) AsInt32() int32 {
	return int32(binary.LittleEndian.Uint32(v.Data))
}

func (v *Value) AsFloat32() float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(v.Data))
}

func (v *Value) AsVarchar() string {
	return string(v.Data)
}
