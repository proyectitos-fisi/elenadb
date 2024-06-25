package tuple

import (
	"bytes"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/storage/table/value"
	"fmt"
)

// TOGRASP: is this tuple also used in table heaps? If so, it should have a valid RowId, right?
type Tuple struct {
	RowId  common.RID // only valid if pointing to the table heap
	values []value.Value
}

// func New(row_id common.RID) *Tuple

func New(values []value.Value, RowId common.RID) *Tuple {
	return &Tuple{
		values: values,
		RowId:  RowId,
	}
}

func NewFromValues(values []value.Value) *Tuple {
	return &Tuple{
		values: values,
		RowId:  *common.InvalidRID(),
	}
}

func NewFromRawData(schema *schema.Schema, reader *bytes.Reader) *Tuple {
	panic("Not implemented")
}

func Empty() *Tuple {
	return &Tuple{
		RowId: *common.InvalidRID(),
	}
}

func (t *Tuple) PrintAsRow(schema *schema.Schema) {
	// TODO: format nicely
	fmt.Print("| ")
	for _, val := range t.values {
		fmt.Print(val)
	}
	fmt.Print("\t|\n")
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
