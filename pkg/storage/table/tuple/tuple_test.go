package tuple_test

import (
	"bytes"
	"encoding/binary"
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/storage/table/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTupleSerializeDeserialize(t *testing.T) {
	// disk := bytes.NewBuffer(nil)

	// Serialize
	tp := tuple.Empty()

	assert.NotNil(t, tp)

	// tp.Data = []byte("Some random tuple data")
	// tp.SerializeTo(disk)

	// // Deserialize
	// tp2 := tuple.Empty()
	// tp2.DeserializeFrom(bytes.NewReader(disk.Bytes()))

	// if string(tp.Data) != string(tp2.Data) {
	// 	t.Fatalf("Data mismatch")
	// }
}

func TestTupleAsRawData(t *testing.T) {
	tp := tuple.New(
		[]value.Value{
			*value.NewBooleanValue(true),
			*value.NewInt32Value(69),
			*value.NewVarCharValue("elena", 5),
			*value.NewVarCharValue("elena_overflowed", 10),
		},
		*common.InvalidRID(),
	)

	rawData := tp.AsRawData()

	assert.Equal(t, 1+4+1+5+1+10, len(rawData))

	// check for the boolean
	assert.Equal(t, byte(1), rawData[0])

	// check for the integer
	decodedI32 := int32(binary.LittleEndian.Uint32(rawData[1:5]))
	assert.Equal(t, int32(69), decodedI32)

	// check for the string
	decodedString := string(rawData[6:11])
	assert.Equal(t, "elena", decodedString)

	// check for the varchar length
	assert.Equal(t, byte(5), rawData[5])

	// check for the second varchar length
	assert.Equal(t, byte(10), rawData[11])

	// check for the second varchar
	decodedString2 := string(rawData[1+4+1+5+1:])
	assert.Equal(t, "elena_over", decodedString2)
}

func TestTupleRawDataParsingUsingSchemas(t *testing.T) {
	tp := tuple.New(
		[]value.Value{
			*value.NewBooleanValue(true),
			*value.NewInt32Value(69),
			*value.NewVarCharValue("elena", 5),
			*value.NewVarCharValue("elena_overflowed", 10),
		},
		*common.InvalidRID(),
	)

	tpSchema := schema.NewSchema([]column.Column{
		column.NewColumn(value.TypeBoolean, "the_bool"),
		column.NewColumn(value.TypeInt32, "the_int"),
		column.NewSizedColumn(value.TypeVarChar, "the_varchar", 5),
		column.NewSizedColumn(value.TypeVarChar, "the_varchar2", 10),
	})
	rawData := tp.AsRawData()

	tp2 := tuple.NewFromRawData(tpSchema, bytes.NewReader(rawData))

	assert.Equal(t, tp.Size, tp2.Size)
	assert.Equal(t, tp.RowId, tp2.RowId) // both invalid
	assert.Equal(t, tp.AsRawData(), tp2.AsRawData())
	assert.Equal(t, tp.IsNull(), tp2.IsNull())
	assert.Equal(t, tp.Values, tp2.Values)
	assert.Equal(t, tp.Values, tp2.Values)
}
