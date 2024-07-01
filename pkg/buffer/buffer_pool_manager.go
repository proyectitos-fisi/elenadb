package buffer

import (
	"fisi/elenadb/pkg/catalog"
	"fisi/elenadb/pkg/common"
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"fisi/elenadb/pkg/storage/page"
	"fisi/elenadb/pkg/utils"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type BufferPoolManager struct {
	poolSize      uint32
	diskScheduler *storage_disk.DiskScheduler
	pageTable     map[common.FrameID_t]*page.Page // relaciones GRACIAS!!!!!!!!!!!!!!!!!!!!!!!
	replacer      LRUKReplacer
	latch         sync.RWMutex
	nextPageID    *atomic.Int32
	dbName        string
	freeList      []common.FrameID_t
	log           *common.Logger
}

func NewBufferPoolManager(dbName string, poolSize uint32, k int, ctlg *catalog.Catalog) *BufferPoolManager {
	diskManager, err := storage_disk.NewDiskManager(dbName)
	if err != nil {
		panic(err)
	}

	scheduler := storage_disk.NewScheduler(diskManager, ctlg)
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
		dbName:        dbName,
		latch:         sync.RWMutex{},
		log:           common.NewLogger('💾'),
	}
}

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

	return bp.fetchPageUnlocked(pageId)
}

func (bp *BufferPoolManager) FetchLastPage(fileId common.FileID_t) *page.Page {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	filename := bp.diskScheduler.Catalog.FilenameFromFileId(fileId)
	size, err := storage_disk.GetFileSize(filepath.Join(bp.dbName, *filename))
	if err != nil {
		panic(err)
	}

	if size == 0 {
		return nil
	}

	apidOffset := utils.Max(size/common.ElenaPageSize-1, 0)
	pageId := common.NewPageIdFromParts(fileId, common.APageID_t(apidOffset))
	return bp.fetchPageUnlocked(pageId)
}

func (bp *BufferPoolManager) fetchPageUnlocked(pageId common.PageID_t) *page.Page {
	// First search for page_id in the buffer pool
	for frameId, page := range bp.pageTable {
		if page == nil {
			continue
		}

		if page.PageId == pageId {
			// if found, returneas la page pues, but you pin it
			page.PinCount.Add(1)
			bp.log.Debug("fetch page %s from frame '%d' (pins=%d)", pageId.ToString(), frameId, page.PinCount.Load())
			return page
		}
	}

	frameId := common.InvalidFrameID

	// before fetching from disk, we check whether if there's a free frame
	if len(bp.freeList) == 0 {
		bp.log.Debug("no frames available, trying to evict")

		frameId = bp.replacer.Evict() // obtain the next evictable frame
		if frameId == common.InvalidFrameID {
			bp.log.Error("unable to allocate page %s: no free frames available", pageId.ToString())
			log.Println("MAYBE ERR: No free frames available")
			return nil
		}
		bp.log.Debug("evicted frame '%d'", frameId)
		// eviction can happen
		if !bp.DeletePage(bp.pageTable[frameId].PageId) {
			panic("DeletePage shouldn't have returned false since we just evicted that page")
		}
	} else {
		// there's a free frame to use!!11!!1!
		frameId = bp.freeList[0]
	}

	data := make([]byte, common.ElenaPageSize)
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
		return nil
	}

	newPage := page.NewPageWithData(pageId, data, 1)
	bp.log.Debug("cache page %s to frame '%d'", pageId.ToString(), frameId)
	bp.pageTable[frameId] = newPage
	bp.removeFromFreeList(frameId)

	// Una vez que hallamos creado la página, marcamos ese frame
	// como not evictable
	bp.replacer.TriggerAccess(frameId)
	bp.replacer.SetEvictable(frameId, false)

	return newPage
}

/**
 * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
 * @return the id of the allocated page
 */
func (bp *BufferPoolManager) AllocatePage(fileId common.FileID_t) common.PageID_t {
	// In order to know what's the next apid (actual page id) to allocate for this fileId
	// we need to visit our frames, since there may be pages that are in memory but not
	filename := bp.diskScheduler.Catalog.FilenameFromFileId(fileId)

	maxActualPageId := common.APageID_t(0)
	foundOnBuffer := false

	for _, page := range bp.pageTable {
		if page == nil {
			continue
		}

		if fileId == page.PageId.GetFileId() {
			foundOnBuffer = true
			if page.PageId.GetActualPageId() > maxActualPageId {
				maxActualPageId = page.PageId.GetActualPageId()
			}
		}
	}

	size, err := storage_disk.GetFileSize(filepath.Join(bp.dbName, *filename))
	if err != nil {
		panic(err)
	}

	var nextActualPageId common.APageID_t
	if !foundOnBuffer {
		// IF this file doesn't have pages on memory we choose the next actual page id
		// based on its size
		nextActualPageId = common.APageID_t(size / common.ElenaPageSize)
	} else {
		// IF this file has pages on memory we choose the next actual page id
		nextActualPageId = utils.Max(maxActualPageId+1, common.APageID_t(size/common.ElenaPageSize))
	}

	pageId := common.NewPageIdFromParts(fileId, nextActualPageId)

	// add one to the next page id
	return pageId
}

/**
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
func (bp *BufferPoolManager) newPageUnlocked(fileId common.FileID_t) *page.Page {
	var frameId common.FrameID_t

	if len(bp.freeList) == 0 {
		// no free frames
		frameId = bp.replacer.Evict() // obtain the next evictable frame
		if frameId == common.InvalidFrameID {
			return nil
		}
		// eviction can happen
		// check if page is dirrrty (POP ANTHEM BY CRHISTINA AGUILERA!!) so we write it to disk
		// NOTE: debugger halts here
		if !bp.DeletePage(bp.pageTable[frameId].PageId) {
			panic("DeletePage shouldn't have returned false since we just evicted that page")
		}
	} else {
		// there's a free frame to use!!11!!1!
		frameId = bp.freeList[0]
	}

	newPage := page.NewPage(bp.AllocatePage(fileId), 1)
	bp.pageTable[frameId] = newPage
	bp.removeFromFreeList(frameId)
	// Una vez que hallamos creado la página, marcamos ese frame como not evictable

	// Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
	bp.replacer.TriggerAccess(frameId)
	bp.replacer.SetEvictable(frameId, false) // ya entendí, paolo

	return newPage
}

func (bp *BufferPoolManager) NewPage(fileId common.FileID_t) *page.Page {
	bp.latch.Lock()
	defer bp.latch.Unlock()
	return bp.newPageUnlocked(fileId)
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
		bp.flushPageNoLock(pageId)
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
			page.IsDirty = isDirty
			if page.PinCount.Load() <= 0 {
				return false
			}
			if page.PinCount.Add(-1) == 0 {
				bp.replacer.SetEvictable(frameId, true)
			}
			// bp.log.Debug("unpin page %s in frame '%d' (is_dirty=%t, pins=%d)", pageId.ToString(), frameId, isDirty, page.PinCount.Load())
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
	return bp.flushPageNoLock(pageId)
}

// Same as FlushPage but without the lock
func (bp *BufferPoolManager) flushPageNoLock(pageId common.PageID_t) bool {
	for _, page := range bp.pageTable {
		if page == nil {
			continue
		}

		if page.PageId == pageId {
			if !page.IsDirty {
				return true
			}
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

// Schedules a write for each dirty page in the buffer pool.
// FUTURE NOTE: You may need to first adquire each page's latch before writing to disk
func (bp *BufferPoolManager) FlushEntirePool() {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	for _, page := range bp.pageTable {
		if page == nil {
			continue
		}

		if page.IsDirty {
			cb := make(chan bool)
			bp.diskScheduler.Schedule(&storage_disk.DiskRequest{
				IsWrite:  true,
				PageID:   page.PageId,
				Data:     page.Data,
				Callback: cb,
			})
			res := <-cb
			if !res {
				panic("unexpected I/O error")
			}
			page.IsDirty = false
		}
	}
}

func (bp *BufferPoolManager) removeFromFreeList(frameId common.FrameID_t) {
	newFreeList := make([]common.FrameID_t, 0, len(bp.freeList))

	for _, id := range bp.freeList {
		if id == frameId {
			continue
		}
		newFreeList = append(newFreeList, id)
	}

	bp.freeList = newFreeList
}

// Iterates over our frames to see if the page exists. If not, tries to create fetch it from disk
// as usual. Once fetched, writes the data to the page.
// TODO: adquire write lock
func (bp *BufferPoolManager) WriteDataToPage(pageId common.PageID_t, data []byte) bool {
	bp.latch.Lock()
	defer bp.latch.Unlock()

	page := bp.fetchPageUnlocked(pageId)

	if page == nil {
		return false
	}

	copy(page.Data, data)
	page.IsDirty = true

	return true
}
