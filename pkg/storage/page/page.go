package page

import (
	"fisi/elenadb/pkg/common"
	"sync"
)

type Page struct {
	pageId   common.PageID_t
	PinCount int
	isDirty  bool
	data     []byte
	latch    sync.RWMutex
}

func (p *Page) ResetMemory() {
	p.latch.Lock()
	defer p.latch.Unlock()

	p.data = make([]byte, common.ElenaPageSize)
}
