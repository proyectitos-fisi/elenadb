package buffer

import (
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"fisi/elenadb/pkg/storage/page"
	"sync"
)

type BufferPoolManager struct {
	poolSize      uint32
	pages         page.Page
	diskScheduler storage_disk.DiskScheduler
	pageTable     map[uint32]page.Page
	replacer      LRUKReplacer
	latch         sync.RWMutex
}

func (bp *BufferPoolManager) FetchPage() *page.Page {
	return nil
}
