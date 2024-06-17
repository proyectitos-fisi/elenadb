package tuple

const (
	TypeBoolean = iota
	TypeInt32
	TypeFloat32
	TypeVarChar
	TypeInvalid
)

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
