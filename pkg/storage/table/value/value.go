package value

type ValueType string

const (
	TypeBoolean ValueType = "boolean"
	TypeInt32   ValueType = "int32"
	TypeFloat32 ValueType = "float32"
	TypeVarChar ValueType = "varchar"
	TypeInvalid ValueType = "invalid"
)

type Value struct {
	typeId ValueType
	data   []byte
}

func (v *Value) GetValue() []byte {
	return v.data
}

func NewValue(type_id ValueType, data []byte) *Value {
	return &Value{
		typeId: type_id,
		data:   data,
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
