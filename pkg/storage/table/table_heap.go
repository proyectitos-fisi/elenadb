package table

import (
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/storage/table/tuple"
)

type TableHeap struct {
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
func (th *TableHeap) MakeIterator() {

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
