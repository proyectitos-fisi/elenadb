package value

import (
	"encoding/binary"
	"fisi/elenadb/pkg/utils"
	"math"
	"strconv"
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

func GetDefaultValueForType(typeId ValueType) any {
	switch typeId {
	case TypeBoolean:
		return false
	case TypeInt32:
		return int32(0)
	case TypeFloat32:
		return float32(0)
	case TypeVarChar:
		return ""
	default:
		panic("unrechable. varchar should use Column.StorageSize")
	}
}

func NewInt32Value(data int32) *Value {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(data))
	return NewValue(TypeInt32, buf)
}

func NewFloat32Value(data float32) *Value {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, math.Float32bits(data))
	return NewValue(TypeFloat32, buf)
}

func NewBooleanValue(data bool) *Value {
	if data {
		return NewValue(TypeBoolean, []byte{1})
	}
	return NewValue(TypeBoolean, []byte{0})
}


// varchars are encoded as: [len(u8)][data(len)]
func NewVarCharValue(data string, maxBytes int) *Value {
	if maxBytes > 255 {
		panic("varchar data is waaay too big, got size: " + strconv.Itoa(maxBytes))
	}
	strlen := utils.Min(maxBytes, len(data))
	buf := make([]byte, strlen+1)

	copy(buf[1:], []byte(data))
	buf[0] = byte(uint8(strlen))
	return NewValue(TypeVarChar, buf)
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
	return string(v.Data[1 : v.Data[0]+1])
}

func (v *Value) SizeOnDisk() uint16 {
	return uint16(len(v.Data))
}
