package buffer

import (
	"fisi/elenadb/pkg/common"
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"fisi/elenadb/pkg/storage/page"
	"sync"
	"sync/atomic"
)

// pages         []*page.Page
type BufferPoolManager struct {
	poolSize      uint32
	diskScheduler *storage_disk.DiskScheduler
	pageTable     map[common.FrameID_t]*page.Page // relaciones GRACIAS!!!!!!!!!!!!!!!!!!!!!!!
	replacer      LRUKReplacer
	latch         sync.RWMutex
	nextPageID    *atomic.Int32
	freeList      []common.FrameID_t
}

func NewBufferPoolManager(poolSize uint32, diskManager *storage_disk.DiskManager, k int) *BufferPoolManager {
	scheduler := storage_disk.NewScheduler(diskManager)
	freeList := make([]common.FrameID_t, poolSize)
	pageTable := make(map[common.FrameID_t]*page.Page)
	var nextPageID atomic.Int32
	nextPageID.Store(-1)

	for i := uint32(0); i < poolSize; i++ {
		freeList[i] = common.FrameID_t(i)
		pageTable[common.FrameID_t(i)] = nil
	}

	// TODO: @damaris how many threads should we use?
	scheduler.StartWorkerThread()

	return &BufferPoolManager{
		poolSize:      poolSize,
		pageTable:     pageTable,
		diskScheduler: scheduler,
		replacer:      *NewLRUK(poolSize, k),
		nextPageID:    &nextPageID,
		freeList:      freeList,
	}
}

// func (bp *BufferPoolManager)

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. 💋 Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
 * disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
 * you need to write it back to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
 *
 * @param page_id id of page to be fetched
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
// auto FetchPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> Page *;
func (bp *BufferPoolManager) FetchPage(pageId common.PageID_t) *page.Page {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	// First search for page_id in the buffer pool
	for _, page := range bp.pageTable {
		if page == nil {
			continue
		}

		if page.PageId == pageId {
			// if found, returneas la page pues
			return page
		}
	}

	frameId := common.InvalidFrameID

	// before fetching from disk, we check whether if there's a free frame
	if len(bp.freeList) == 0 {
		frameId = bp.replacer.Evict() // obtain the next evictable frame
		if frameId == common.InvalidFrameID {
			return nil
		}
		// eviction can happen
		if !bp.DeletePage(bp.pageTable[frameId].PageId) {
			panic("DeletePage shouldn't have returned false since we just evicted that page")
		}
	}

	var data []byte
	callback := make(chan bool)

	// try fetching from disk
	bp.diskScheduler.Schedule(&storage_disk.DiskRequest{
		IsWrite:  false,
		PageID:   pageId,
		Data:     data,
		Callback: callback,
	})

	// TODO: @damaris should we wait for the callback?
	read := <-callback
	if !read {
		panic("unexpected I/O error")
	}

	newPage := page.NewPageWithData(pageId, data, 1)

	// Una vez que hallamos creado la página, marcamos ese frame
	// como not evictable
	// bp.replacer.TriggerAccess(frameId)
	// bp.replacer.SetEvictable(frameId, false)
	// TODO: @damaris @damaris @damaris @damaris @damaris

	return newPage
}

/**
 * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
 * @return the id of the allocated page
 */
func (bp *BufferPoolManager) AllocatePage() common.PageID_t {
	// add one to the next page id
	return common.PageID_t(bp.nextPageID.Add(1))
}

/**
* TODO(P1): Add implementation
*
* @brief Create a new page in the buffer pool.
* ✅ Set page_id to the new page's id,
* ✅ or nullptr if all frames
* are currently in use and not evictable (in another word, pinned).
*
* ✅ You should pick the replacement frame from either the free list or the replacer
* ✅ (always find from the free list first)
* ✅ and then call the AllocatePage() method to get a new page id.
* ✅ If the replacement frame has a dirty page, you should write it back to the disk first.
*
* ✅ You also need to reset the memory and metadata for the new page.
*
* ✅ Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
* so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
* ✅ Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
*
* @param[out] page_id id of created page
* @return nullptr if no new pages could be created, otherwise pointer to new page
 */

func (bp *BufferPoolManager) NewPage() *page.Page {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	var frameId common.FrameID_t

	if len(bp.freeList) == 0 {
		// no free frames
		frameId = bp.replacer.Evict() // obtain the next evictable frame
		if frameId == common.InvalidFrameID {
			return nil
		}
		// eviction can happen
		// check if page is dirrrty (POP ANTHEM BY CRHISTINA AGUILERA!!) so we write it to disk
		if !bp.DeletePage(bp.pageTable[frameId].PageId) {
			panic("DeletePage shouldn't have returned false since we just evicted that page")
		}
	} else {
		// there's a free frame to use!!11!!1!
		frameId = bp.freeList[0]
		bp.freeList = bp.freeList[1:]
	}

	newPage := page.Page{
		PageId:   bp.AllocatePage(),
		PinCount: atomic.Int32{},
		IsDirty:  false,
		Data:     make([]byte, common.ElenaPageSize),
		Latch:    sync.RWMutex{},
	}
	// Una vez que hallamos creado la página, marcamos ese frame como not evictable

	// Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
	newPage.PinCount.Store(1)
	bp.replacer.TriggerAccess(frameId)
	bp.replacer.SetEvictable(frameId, false) // THERE'S NOTHING HERE ya entendí, paolo

	bp.pageTable[frameId] = &newPage

	return &newPage
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata.
 *
 * WARNING: LOCK SHOULD BE ACQUIRED BEFORE CALLING THIS FUNCTION
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
func (bp *BufferPoolManager) DeletePage(pageId common.PageID_t) bool {
	frameIdToDelete := common.InvalidFrameID

	for frameId, page := range bp.pageTable {
		if page == nil {
			continue
		}

		if page.PageId == pageId {
			frameIdToDelete = frameId
			if page.PinCount.Load() > 0 {
				return false
			}
			break
		}
	}

	if frameIdToDelete == common.InvalidFrameID {
		return true
	}

	page := bp.pageTable[frameIdToDelete]
	if page.IsDirty {
		// write to disk
		bp.FlushPage(pageId)
	}

	// stop tracking the frame in the replacer
	bp.replacer.Remove(frameIdToDelete)
	// add the frame back to the free list
	bp.freeList = append(bp.freeList, frameIdToDelete)
	// reset the page's memory and metadata
	page.ResetMemory()
	return true
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
/* For UnpinPage, the is_dirty parameter keeps track of whether a page was modified while it was pinned. */
func (bp *BufferPoolManager) UnpinPage(pageId common.PageID_t, isDirty bool) bool {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	for frameId, page := range bp.pageTable {
		if page == nil {
			continue
		}

		if page.PageId == pageId {
			if page.PinCount.Load() <= 0 {
				return false
			}
			if page.PinCount.Add(-1) == 0 {
				bp.replacer.SetEvictable(frameId, true)
			}
			page.IsDirty = isDirty
			return true
		}
	}

	return false
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush the target page to disk.
 *
 * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
 * Unset the dirty flag of the page after flushing.
 *
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
func (bp *BufferPoolManager) FlushPage(pageId common.PageID_t) bool {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	for _, page := range bp.pageTable {
		if page == nil {
			continue
		}

		if page.PageId == pageId {
			cb := make(chan bool)
			bp.diskScheduler.Schedule(&storage_disk.DiskRequest{
				IsWrite:  true,
				PageID:   pageId,
				Data:     page.Data,
				Callback: cb,
			})
			res := <-cb
			if !res {
				panic("unexpected I/O error")
			}
			page.IsDirty = false
			return true
		}
	}

	return false
}
