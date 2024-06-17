package tuple

import (
	"bytes"
	"encoding/binary"
	"fisi/elenadb/pkg/common"
)

// TOGRASP: is this tuple also used in table heaps? If so, it should have a valid RowId, right?
type Tuple struct {
	RowId common.RID // only valid if pointing to the table heap
	Data  []byte
}

// func New(row_id common.RID) *Tuple

func New(values []Value /*, schema Schema*/) {

}

func Empty() *Tuple {
	return &Tuple{
		RowId: common.InvalidRID,
	}
}

func (t *Tuple) DeserializeFrom(reader *bytes.Reader) error {
	var size uint32
	err := binary.Read(reader, binary.LittleEndian, &size)
	if err != nil {
		return err
	}

	t.Data = make([]byte, size)
	n, err := reader.Read(t.Data)
	if err != nil {
		return err
	}
	if n != int(size) {
		panic("Failed to read all data")
	}

	return nil
}

func (t *Tuple) SerializeTo(writer *bytes.Buffer) error {
	err := binary.Write(writer, binary.LittleEndian, uint32(len(t.Data)))
	if err != nil {
		return err
	}

	_, err = writer.Write(t.Data)
	if err != nil {
		return err
	}

	return nil
}

// func (t* Tuple) Serialize

func (t *Tuple) IsNull() bool {
	return false
}
