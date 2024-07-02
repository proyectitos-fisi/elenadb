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
// FLAG_ESTRUCTURA: tuple
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

// FLAG_ALGORITMO: data serialization
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
			debugutils.NotErr(reader.Read(b))
			val = *value.NewBooleanValue(b[0] == 1)
		case value.TypeInt32:
			b := make([]byte, 4)
			debugutils.NotErr(reader.Read(b))
			val = *value.NewInt32Value(int32(binary.LittleEndian.Uint32(b)))
		case value.TypeFloat32:
			b := make([]byte, 4)
			debugutils.NotErr(reader.Read(b))
			val = *value.NewFloat32Value(math.Float32frombits(binary.LittleEndian.Uint32(b)))
		case value.TypeVarChar:
			b := make([]byte, 1)
			debugutils.NotErr(reader.Read(b))
			size := b[0]
			if size > byte(col.StorageSize) {
				panic(fmt.Sprintf("Size mismatch in varchar: %d != %d", size, col.StorageSize))
			}
			data := make([]byte, size)
			debugutils.NotErr(reader.Read(data))
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
		RowId:  *common.InvalidRID(),
		Values: []value.Value{},
		Size:   0,
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
	var formattedCols [][]string = make([][]string, len(t.Values))
	maxRows := 0

	for idx, val := range t.Values {
		colName := schema.ExtractColumnName(rowSchema.GetColumn(idx).ColumnName)

		var formattedValue string

		// TODO: scape characters?
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
			panic(fmt.Sprintf("Unknown type: '%s'", string(val.Type)))
		}

		width := utils.Max(
			len(colName),
			schema.GetMinimumSpacingForType(val.Type),
		)

		nrows := int(math.Ceil(float64(len([]rune(formattedValue))) / float64(width)))
		formattedCols[idx] = make([]string, 0, nrows)

		if nrows > maxRows {
			maxRows = nrows
		}

		for r := 0; r < nrows; r++ {
			if r+1 != nrows {
				formattedCols[idx] = append(formattedCols[idx], formattedValue[r*width:utils.Min((r+1)*width, len(formattedValue))])
			} else {
				formattedCols[idx] = append(formattedCols[idx], formattedValue[r*width:])
			}
		}
	}

	if maxRows > 1 {
		maxRows += 1
	}

	for row := 0; row < maxRows; row++ {
		for idx, _ := range t.Values {
			col := rowSchema.GetColumn(idx)
			var formattedValue string

			if row < len(formattedCols[idx]) {
				formattedValue = formattedCols[idx][row]
			} else {
				formattedValue = ""
			}
			utf8len := len([]rune(formattedValue))

			spacing := schema.GetTableColSpacingFromColumn(col)

			fmt.Print("| ")
			fmt.Print(formattedValue)
			if spacing-utf8len >= 0 {
				fmt.Print(strings.Repeat(" ", spacing-utf8len+1))
			}
		}
		fmt.Print("|\n")
	}
}

func (t *Tuple) IsNull() bool {
	return false
}
