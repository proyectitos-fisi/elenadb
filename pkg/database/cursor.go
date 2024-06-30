package database

import "fisi/elenadb/pkg/common"

type PagesCursor struct {
	// The current page that the cursor is pointing to
	PageId  common.PageID_t
	SlotNum common.SlotNumber_t
}

func NewPagesCursor(pageId common.PageID_t, slotNum common.SlotNumber_t) *PagesCursor {
	return &PagesCursor{
		PageId:  pageId,
		SlotNum: slotNum,
	}
}

func NewPagesCursorFromParts(fileId common.FileID_t, actualPageId common.PageID_t, slotNum common.SlotNumber_t) *PagesCursor {
	pageId := common.PageID_t(fileId)<<16 | actualPageId
	return NewPagesCursor(pageId, slotNum)
}

func (c *PagesCursor) NextPage() {
	fileId := c.PageId.GetFileId()
	actualPageId := c.PageId.GetActualPageId() // (uint16)
	actualPageId++

	pageId := common.PageID_t(uint32(fileId)<<16 | uint32(actualPageId))
	c.PageId = pageId
	c.SlotNum = 0
}

func (c *PagesCursor) NextSlot() {
	c.SlotNum++
}
