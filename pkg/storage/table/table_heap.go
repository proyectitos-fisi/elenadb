package table

import (
	"fisi/elenadb/pkg/buffer"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/storage/table/tuple"
	"sync"
)

type TableIterator struct {
	heap     *TableHeap
	startRID common.RID
	endRID   common.RID
}

func NewTableIterator(
	heap *TableHeap,
	startRID common.RID,
	endRID common.RID,
) *TableIterator {
	it := &TableIterator{
		heap:     heap,
		startRID: startRID,
		endRID:   endRID,
	}

	if startRID.PageID == common.InvalidPageID {
		it.startRID = common.RID{PageID: heap.firstPageId, SlotNum: 69}
	}
	page := heap.bpm.FetchPage(startRID.PageID)
	if page == nil {
		return nil
	}
	return it
}

type TableHeap struct {
	bpm         *buffer.BufferPoolManager
	latch       sync.RWMutex
	firstPageId common.PageID_t
	lastPageId  common.PageID_t
}

func (th *TableHeap) InsertTuple(meta tuple.TupleMeta, tuple tuple.Tuple, tableId int32) *common.RID {
	return common.InvalidRID()
}
func (th *TableHeap) UpdateTupleMeta(meta tuple.TupleMeta, rid common.RID) {

}
func (th *TableHeap) GetTuple(rid common.RID) (tuple.TupleMeta, tuple.Tuple) {
	return *tuple.EmptyTupleMeta(), *tuple.Empty()
}
func (th *TableHeap) GetTupleMeta(rid common.RID) tuple.Tuple {
	return *tuple.Empty()
}
func (th *TableHeap) MakeIterator() *TableIterator {
	th.latch.Lock()
	defer th.latch.Unlock()

	page := th.bpm.FetchPage(th.lastPageId) // TODO: @damaris this should be a ReadGuard
	numTuples := page.AsPageTableData().NumTuples
	// drop guard
	return NewTableIterator(
		th,
		common.RID{PageID: th.firstPageId, SlotNum: 0},
		common.RID{PageID: th.lastPageId, SlotNum: numTuples},
	)
}
func (th *TableHeap) MakeEagerIterator() {

}
func (th *TableHeap) GetFirstPageId() {

}
func (th *TableHeap) UpdateTupleInPlace() {

}
func (th *TableHeap) CreateEmptyHeap() {

}
func (th *TableHeap) AcquireTablePageReadLock() {

}
func (th *TableHeap) AcquireTablePageWriteLock() {

}
func (th *TableHeap) UpdateTupleInPlaceWithLockAcquired() {

}
func (th *TableHeap) GetTupleWithLockAcquired() {

}
func (th *TableHeap) GetTupleMetaWithLockAcquired() {

}
