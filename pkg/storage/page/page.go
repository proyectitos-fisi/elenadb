package page

import (
	"fisi/elenadb/pkg/common"
	"sync"
	"sync/atomic"
	"unsafe"
)

// -------------------------------------------------------------------
// |  HEADER (8 bytes)  |  SLOTS  |  ..........  |  INSERTED TUPLES  |
// -------------------------------------------------------------------

// |------------------------------- HEADER --------------------------------|
// -------------------------------------------------------------------------
// | NumTuples(2) | NumDeletedTuples(2) | FreeSpace(2) | LastUsedOffset(2) |
// -------------------------------------------------------------------------
type PageHeader struct {
	NumTuples      uint16 // 2 bytes
	NumDeleted     uint16 // 2 bytes
	FreeSpace      uint16 // 2 bytes
	LastUsedOffset uint16 // 2 bytes

}

// Data format
// ----------------------------------------------------------------
// | Tuple_1 offset+size (4) | Tuple_2 offset+size (4) | ... |
// ----------------------------------------------------------------
type PageTableData struct {
	PageId     common.PageID_t // 4 bytes
	NumTuples  uint32          // 4 bytes
	NumDeleted uint32          // 4 bytes
}

// static assert for PageTable size
const HEADER_SIZE = 12

var _ [0]struct{} = [unsafe.Sizeof(PageTableData{}) - HEADER_SIZE]struct{}{}

type Page struct {
	PageId   common.PageID_t
	PinCount atomic.Int32 // number of workers que usan la page. WHEN DO U PIN? when a worker thread is using it during a query
	IsDirty  bool
	Data     []byte
	Latch    sync.RWMutex
}

func NewPage(pageId common.PageID_t, pinCount int32) *Page {
	p := &Page{
		PageId:   pageId,
		PinCount: atomic.Int32{},
		IsDirty:  false,
		Data:     make([]byte, common.ElenaPageSize),
		Latch:    sync.RWMutex{},
	}
	p.PinCount.Store(pinCount)
	return p
}

func NewPageWithData(pageId common.PageID_t, data []byte, pinCount int32) *Page {
	p := &Page{
		PageId:   pageId,
		PinCount: atomic.Int32{},
		IsDirty:  false,
		Data:     data,
		Latch:    sync.RWMutex{},
	}
	p.PinCount.Store(pinCount)
	return p
}

func (p *Page) ResetMemory() {
	p.Latch.Lock()
	defer p.Latch.Unlock()

	p.Data = make([]byte, common.ElenaPageSize)
	p.IsDirty = false
	p.PinCount.Store(0)
}

func (p *Page) AsPageTableData() *PageTableData {
	p.Latch.RLock()
	defer p.Latch.RUnlock()

	// cast the memory to PageTableData
	return (*PageTableData)(unsafe.Pointer(&p.Data[0]))
}
