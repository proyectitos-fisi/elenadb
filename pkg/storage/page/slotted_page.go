package page

import (
	"bytes"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/storage/table/tuple"
	"unsafe"
)

// A table is made up of Data Pages, which is a combination of HEADER, SLOTS and INSERTED TUPLES
// -------------------------------------------------------------------
// |  HEADER (8 bytes)  |  SLOTS  |  ..........  |  INSERTED TUPLES  |
// -------------------------------------------------------------------
//                                               ^
// ________________ LastUsedOffset ______________|
//

// Page header format:
// -------------------------------------------------------------------------
// | NumTuples(2) | NumDeletedTuples(2) | FreeSpace(2) | LastUsedOffset(2) |
// -------------------------------------------------------------------------
type SlottedPageHeader struct {
	NumTuples      uint16              // 2 bytes
	NumDeleted     uint16              // 2 bytes
	FreeSpace      uint16              // 2 bytes
	LastUsedOffset common.SlotOffset_t // 2 bytes
}

// Just a wrapper type for Pages
type SlottedPage struct {
	Header   *SlottedPageHeader
	PageData []byte
}

// |-------- Slot 1 ---------|-------- Slot 2 ---------|
// ---------------------------------------------------------
// | Offset (2) | Length (2) | Offset (2) | Length (2) | ...
// ---------------------------------------------------------
type SlotData struct {
	Offset common.SlotOffset_t
	Length uint16
}

func NewEmptySlottedPage() *SlottedPage {
	h := &SlottedPageHeader{
		NumTuples:      0,
		NumDeleted:     0,
		FreeSpace:      common.ElenaPageSize - SLOTTED_PAGE_HEADER_SIZE,
		LastUsedOffset: common.SlotOffset_t(common.ElenaPageSize) - SLOTTED_PAGE_HEADER_SIZE,
	}
	rawEmptyPage := make([]byte, common.ElenaPageSize)
	copy(rawEmptyPage[0:], (*(*[8]byte)(unsafe.Pointer(h)))[:])

	return &SlottedPage{
		Header:   h,
		PageData: rawEmptyPage,
	}
}

func AsSlottedPage(p *Page) *SlottedPage {
	return &SlottedPage{
		Header:   NewSlottedPageHeaderFromRawPage(p),
		PageData: p.Data,
	}
}

// We define the following operations for a slotted page:
// - Append a tuple:
//   - increase the tuple count
//   - move the last offset
//   - modify free space
// - Delete a tuple:
//   - decrease the tuple count
// - Read a tuple from slot:

const SLOTTED_PAGE_HEADER_SIZE = 8
const SLOT_SIZE = 4

// Compile time checks
var _ [0]struct{} = [unsafe.Sizeof(SlottedPageHeader{}) - SLOTTED_PAGE_HEADER_SIZE]struct{}{}
var _ [0]struct{} = [unsafe.Sizeof(SlotData{}) - SLOT_SIZE]struct{}{}

func (sp *SlottedPage) AsRawPageData() []byte {
	if len(sp.PageData) != common.ElenaPageSize {
		panic("SlottedPage.AsRawPageData: Invalid page size")
	}
	return sp.PageData
}

func NewSlottedPageHeaderFromRawPage(p *Page) *SlottedPageHeader {
	return (*SlottedPageHeader)(unsafe.Pointer(&p.Data[0]))
}

func (sp *SlottedPage) SetNumTuples(numTuples uint16) {
	sp.Header.NumTuples = numTuples
	copy(sp.PageData[0:], (*(*[2]byte)(unsafe.Pointer(&numTuples)))[:])
}

func (sp *SlottedPage) SetNumDeleted(numDeleted uint16) {
	sp.Header.NumDeleted = numDeleted
	copy(sp.PageData[2:], (*(*[2]byte)(unsafe.Pointer(&numDeleted)))[:])
}

func (sp *SlottedPage) SetFreeSpace(freeSpace uint16) {
	sp.Header.FreeSpace = freeSpace
	copy(sp.PageData[4:], (*(*[2]byte)(unsafe.Pointer(&freeSpace)))[:])
}

func (sp *SlottedPage) SetLastUsedOffset(lastUsedOffset common.SlotOffset_t) {
	sp.Header.LastUsedOffset = lastUsedOffset
	copy(sp.PageData[6:], (*(*[2]byte)(unsafe.Pointer(&lastUsedOffset)))[:])
}

func (sp *SlottedPage) AppendTuple(t *tuple.Tuple) error {
	if sp.Header.FreeSpace < t.Size {
		return NoSpaceLeft{}
	}
	// sp.header.NumTuples += 1
	sp.SetNumTuples(sp.Header.NumTuples + 1)
	// sp.header.FreeSpace -= t.Size
	sp.SetFreeSpace(sp.Header.FreeSpace - t.Size)
	// sp.header.LastUsedOffset -= t.Size
	sp.SetLastUsedOffset(sp.Header.LastUsedOffset - common.SlotOffset_t(t.Size))

	// TODO: update slots array
	slots := sp.GetSlotsArray()
	slots = append(slots, SlotData{
		Offset: sp.Header.LastUsedOffset,
		Length: t.Size,
	})
	sp.SetSlotsArray(slots)

	copy(sp.PageData[SLOTTED_PAGE_HEADER_SIZE+int(sp.Header.LastUsedOffset):], t.AsRawData())
	return nil
}

func (sp *SlottedPage) GetSlotsArray() []SlotData {
	numSlots := sp.Header.NumTuples + sp.Header.NumDeleted
	rawSlots := make([]byte, SLOT_SIZE*numSlots)
	copy(rawSlots, sp.PageData[SLOTTED_PAGE_HEADER_SIZE:SLOTTED_PAGE_HEADER_SIZE+int(numSlots)*SLOT_SIZE])

	slots := make([]SlotData, 0, sp.Header.NumTuples)
	for i := 0; i < int(numSlots); i++ {
		offset := i * SLOT_SIZE
		slot := SlotData{
			Offset: common.SlotOffset_t((*(*uint16)(unsafe.Pointer(&rawSlots[offset])))),
			Length: (*(*uint16)(unsafe.Pointer(&rawSlots[offset+2]))),
		}
		if slot.Length != 0 {
			slots = append(slots, slot)
		}
	}
	return slots
}

func (sp *SlottedPage) SetSlotsArray(slots []SlotData) {
	rawSlots := make([]byte, 0, SLOT_SIZE*len(slots))
	minOffset := common.SlotOffset_t(common.ElenaPageSize) - SLOTTED_PAGE_HEADER_SIZE

	numDeleted := 0

	for _, s := range slots {
		if s.Length == 0 {
			numDeleted += 1
		}
		rawSlots = append(rawSlots, (*(*[SLOT_SIZE]byte)(unsafe.Pointer(&s)))[:]...)

		if s.Offset != 0 && s.Offset < minOffset {
			minOffset = s.Offset
		}
	}
	sp.SetNumTuples(uint16(len(slots)))
	sp.SetLastUsedOffset(minOffset)
	sp.SetNumDeleted(uint16(numDeleted))
	if uint16(len(slots)*SLOT_SIZE) > uint16(minOffset) {
		panic("SlottedPage.setSlotsArray: Invalid slots array")
	}
	sp.SetFreeSpace(uint16(minOffset) - uint16(len(slots)*SLOT_SIZE))
	copy(sp.PageData[SLOTTED_PAGE_HEADER_SIZE:], rawSlots)
}

// Deleting a tuple zero out the slot
func (sp *SlottedPage) DeleteTuple(slot common.SlotNumber_t) {
	slots := sp.GetSlotsArray()
	for i := range slots {
		if common.SlotNumber_t(i) == slot {
			slots[i].Length = 0
			sp.SetSlotsArray(slots)
			return
		}
	}
}

// Given a slot number, read that tuple
func (sp *SlottedPage) ReadTuple(schema *schema.Schema, slot common.SlotNumber_t) *tuple.Tuple {
	slots := sp.GetSlotsArray()
	for i, s := range slots {
		if slot == common.SlotNumber_t(i) {
			return tuple.NewFromRawData(
				schema,
				bytes.NewReader(sp.PageData[SLOTTED_PAGE_HEADER_SIZE+s.Offset:SLOTTED_PAGE_HEADER_SIZE+s.Offset+common.SlotOffset_t(s.Length)]),
			)
		}
	}
	return nil
}

// NoSpaceLeft error
type NoSpaceLeft struct{}

func (nsl NoSpaceLeft) Error() string {
	return "No space left in the page"
}
