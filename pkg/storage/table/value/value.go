package value

const (
	TypeIdBoolean = iota
	TypeIdInt32
	TypeIdFloat32
	TypeIdVarChar
	TypeIdInvalid
)

type TypeId int

func (typeId *TypeId) TypeSize() int {
	if typeId.IsInlinedType() {
		return 4
	}

	switch *typeId {
	case TypeIdBoolean:
		return 1
	case TypeIdInt32:
		return 4
	case TypeIdFloat32:
		return 4
	default:
		panic("unrechable")
	}
}

func (typeId *TypeId) IsInlinedType() bool {
	switch *typeId {
	case TypeIdVarChar:
		return false
	default:
		return true
	}
}

type Value struct {
	type_id int
	data    []byte
}

func (v *Value) GetValue() []byte {
	return v.data
}

func NewValue(type_id int, data []byte) {

}

// Math operations
