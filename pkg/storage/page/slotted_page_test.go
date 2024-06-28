package page_test

import (
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/storage/page"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/storage/table/value"
	"fisi/elenadb/pkg/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlottedPages(t *testing.T) {
	slots := []page.SlotData{}

	// Create a new page
	sp := page.NewEmptySlottedPage()
	sp.SetSlotsArray(slots)

	// Append a tuple
	tpl := tuple.NewFromValues([]value.Value{
		*value.NewBooleanValue(true),
		*value.NewInt32Value(69),
		*value.NewVarCharValue("elena", 5),
	})
	sch := schema.NewSchema([]column.Column{
		column.NewColumn(value.TypeBoolean, "some_bool"),
		column.NewColumn(value.TypeInt32, "some_int"),
		column.NewSizedColumn(value.TypeVarChar, "some_varchar", 5),
	})

	assert.Equal(t, sp.Header.NumTuples, uint16(0))
	assert.Equal(t, sp.Header.NumDeleted, uint16(0))
	assert.Equal(t, sp.Header.FreeSpace, uint16(common.ElenaPageSize-page.SLOTTED_PAGE_HEADER_SIZE))
	sp.AppendTuple(tpl)
	assert.Equal(t, sp.Header.NumTuples, uint16(1))
	assert.Equal(t, sp.Header.NumDeleted, uint16(0))
	assert.Equal(t, sp.Header.FreeSpace, uint16(common.ElenaPageSize-page.SLOTTED_PAGE_HEADER_SIZE-tpl.Size-4))

	// Read the tuple
	readTpl := sp.ReadTuple(sch, 0)

	// Check the tuple
	if readTpl == nil {
		t.Fatalf("Tuple not found")
	}

	assert.Equal(t, tpl, readTpl)

	// Append
	tpl2 := tuple.NewFromValues([]value.Value{
		*value.NewBooleanValue(false),
		*value.NewFloat32Value(420.69),
		*value.NewVarCharValue("elena2", 6),
	})
	sch2 := schema.NewSchema([]column.Column{
		column.NewColumn(value.TypeBoolean, "some_bool"),
		column.NewColumn(value.TypeFloat32, "some_float"),
		column.NewSizedColumn(value.TypeVarChar, "some_varchar", 6),
	})
	assert.Nil(t, sp.AppendTuple(tpl2))

	// Check if tuple 0 is still valid!
	assert.Equal(t, tpl, sp.ReadTuple(sch, 0))
	assert.Equal(t, tpl2, sp.ReadTuple(sch2, 1))
	assert.Equal(t, sp.Header.NumTuples, uint16(2))
	assert.Nil(t, sp.ReadTuple(sch2, 2))

	// Try deleting both
	sp.DeleteTuple(0)
	assert.Nil(t, sp.ReadTuple(sch, 0))
	assert.Equal(t, tpl2, sp.ReadTuple(sch2, 1))
	sp.DeleteTuple(1)
	assert.Nil(t, sp.ReadTuple(sch, 0))
	assert.Nil(t, sp.ReadTuple(sch2, 1))
}

func TestSlottedPageFlooding(t *testing.T) {
	// We create an arbitrary tuple
	tpl := tuple.NewFromValues([]value.Value{
		*value.NewBooleanValue(true),
		*value.NewInt32Value(69),
		*value.NewVarCharValue("elena", 5),
		*value.NewFloat32Value(69.69),
	})
	sp := page.NewEmptySlottedPage()

	appendedTupleSize := tpl.Size + page.SLOT_SIZE

	// Simulate pushing 10000 tuples
	for i := uint16(0); i < 1000; i++ {
		err := sp.AppendTuple(tpl)
		expectedFreeSize, safe := utils.SafeSubtractUint16(
			common.ElenaPageSize,
			page.SLOTTED_PAGE_HEADER_SIZE+(i+1)*appendedTupleSize,
		)
		if !safe {
			assert.True(t, page.IsNoSpaceLeft(err))
			break
		}
		assert.Equal(t, sp.Header.FreeSpace, expectedFreeSize)
		assert.Equal(t, sp.Header.NumTuples, uint16(len(sp.GetSlotsArray())))

		if err != nil {
			t.Fatalf("Failed to append tuple: " + err.Error())
		}
	}

	freeSpaceWhenFulled := sp.Header.FreeSpace

	// Now simulate deleting all of them
	deletedCount := uint16(0)
	for i := uint16(0); i < 1000; i++ {
		d := sp.DeleteTuple(common.SlotNumber_t(i))
		if d {
			deletedCount++
		}
		assert.Equal(t, sp.Header.NumDeleted, deletedCount)
	}
	// Deleting all tuples shouldn't free any space
	assert.Equal(t, sp.Header.FreeSpace, freeSpaceWhenFulled)
}
