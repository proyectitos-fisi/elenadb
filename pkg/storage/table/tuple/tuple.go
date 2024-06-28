package tuple

import (
	"bytes"
	"encoding/binary"
	"fisi/elenadb/internal/debugutils"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/storage/table/value"
	"fisi/elenadb/pkg/utils"
	"fmt"
	"math"
	"strings"
)

// TOGRASP: is this tuple also used in table heaps? If so, it should have a valid RowId, right?
type Tuple struct {
	RowId  common.RID // only valid if pointing to the table heap
	Values []value.Value
	Size   uint16
}

// func New(row_id common.RID) *Tuple

func New(values []value.Value, RowId common.RID) *Tuple {
	return &Tuple{
		Values: values,
		RowId:  RowId,
		Size:   calculateValuesSize(values),
	}
}

func (t *Tuple) AsRawData() []byte {
	bytesitos := make([]byte, t.Size)
	offset := 0
	for _, val := range t.Values {
		copy(bytesitos[offset:], val.Data)
		offset += len(val.Data)
	}
	return bytesitos
}

func NewFromValues(values []value.Value) *Tuple {
	return &Tuple{
		Values: values,
		RowId:  *common.InvalidRID(),
		Size:   calculateValuesSize(values),
	}
}

func NewFromRawData(schema *schema.Schema, reader *bytes.Reader) *Tuple {
	values := make([]value.Value, schema.GetColumnCount())

	for idx, col := range schema.GetColumns() {
		var val value.Value
		switch col.ColumnType {
		case value.TypeBoolean:
			b := make([]byte, 1)
			debugutils.NotNil(reader.Read(b))
			val = *value.NewBooleanValue(b[0] == 1)
		case value.TypeInt32:
			b := make([]byte, 4)
			debugutils.NotNil(reader.Read(b))
			val = *value.NewInt32Value(int32(binary.LittleEndian.Uint32(b)))
		case value.TypeFloat32:
			b := make([]byte, 4)
			debugutils.NotNil(reader.Read(b))
			val = *value.NewFloat32Value(math.Float32frombits(binary.LittleEndian.Uint32(b)))
		case value.TypeVarChar:
			b := make([]byte, 1)
			debugutils.NotNil(reader.Read(b))
			size := b[0]
			if size != byte(col.StorageSize) {
				panic("tuple.NewFromRawData: Size mismatch in varchar")
			}
			data := make([]byte, size)
			debugutils.NotNil(reader.Read(data))
			val = *value.NewVarCharValue(string(data), int(size))
		default:
			panic("Unknown column type")

		}
		values[idx] = val
	}
	return NewFromValues(values)
}

func (t *Tuple) GetValue(idx int) value.Value {
	return t.Values[idx]
}

func Empty() *Tuple {
	return &Tuple{
		RowId: *common.InvalidRID(),
		Size:  0,
	}
}

func calculateValuesSize(values []value.Value) uint16 {
	s := uint16(0)
	for _, v := range values {
		s += uint16(len(v.Data))
	}
	return s
}

func (t *Tuple) PrintAsRow(rowSchema *schema.Schema) {
	fmt.Print("| ")

	for idx, val := range t.Values {
		col := rowSchema.GetColumn(idx)
		var formattedValue string

		switch val.Type {
		case value.TypeInt32:
			formattedValue = fmt.Sprintf("%d", val.AsInt32())
		case value.TypeFloat32:
			formattedValue = fmt.Sprintf("%f", val.AsFloat32())
		case value.TypeVarChar:
			formattedValue = val.AsVarchar()
		case value.TypeBoolean:
			formattedValue = fmt.Sprintf("%t", val.AsBoolean())
		default:
			panic("Unknown value type")
		}

		spacing := utils.Max(
			len(formattedValue),
			len(col.ColumnName),
			schema.GetMinimumSpacingForType(val.Type),
		)

		fmt.Print(formattedValue)
		fmt.Print(strings.Repeat(" ", spacing-len(formattedValue)))
	}
	fmt.Print(" |\n")
}

// func (t *Tuple) DeserializeFrom(reader *bytes.Reader) error {
// 	var size uint32
// 	err := binary.Read(reader, binary.LittleEndian, &size)
// 	if err != nil {
// 		return err
// 	}

// 	t.Data = make([]byte, size)
// 	n, err := reader.Read(t.Data)
// 	if err != nil {
// 		return err
// 	}
// 	if n != int(size) {
// 		panic("Failed to read all data")
// 	}

// 	return nil
// }

// func (t *Tuple) SerializeTo(writer *bytes.Buffer) error {
// 	err := binary.Write(writer, binary.LittleEndian, uint32(len(t.Data)))
// 	if err != nil {
// 		return err
// 	}

// 	_, err = writer.Write(t.Data)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (t* Tuple) Serialize

func (t *Tuple) IsNull() bool {
	return false
}
