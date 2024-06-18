package page

import (
	"fisi/elenadb/pkg/common"
	"sync"
	"sync/atomic"
)

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
