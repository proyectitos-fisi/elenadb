package buffer

import (
	"fisi/elenadb/pkg/common"
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"fisi/elenadb/pkg/storage/page"
	"sync"
	"sync/atomic"
)

type BufferPoolManager struct {
	poolSize      uint32
	pages         map[common.PageID_t]*page.Page
	diskScheduler *storage_disk.DiskScheduler
	pageTable     map[common.PageID_t]common.FrameID_t
	replacer      LRUKReplacer
	latch         sync.RWMutex
	nextPageID    atomic.Value
}

func NewBufferPoolManager(poolSize uint32, diskScheduler *storage_disk.DiskScheduler) *BufferPoolManager {
	return &BufferPoolManager{
		poolSize:      poolSize,
		pages:         make(map[common.PageID_t]*page.Page),
		diskScheduler: diskScheduler,
		pageTable:     make(map[common.PageID_t]common.FrameID_t),
		replacer:      *NewLRUK(poolSize, common.LRUKReplacerK),
		nextPageID:    atomic.Value{},
	}
}

/* For FetchPage, you should return nullptr if no page is available in the free list and all other pages are currently pinned. */
func (bp *BufferPoolManager) FetchPage() *page.Page {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	for _, pg := range bp.pages {
		// Check if the page is not pinned
		if pg.PinCount == 0 {
			return pg
		}
	}

	// If no unpinned page found, return nil
	return nil
}

func (bp *BufferPoolManager) AllocatePage() common.PageID_t {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	currentID := bp.nextPageID.Load().(common.PageID_t)
	newID := currentID + 1
	bp.nextPageID.Store(newID)

	return newID
}
