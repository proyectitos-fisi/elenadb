//===----------------------------------------------------------------------===//
//
//                         🚄 ElenaDB ®
//
// rid.go
//
// Identification: pkg/common/rid.go
//
// Copyright (c) 2024
//
//===----------------------------------------------------------------------===//

package common

import (
	"fmt"
	"hash/fnv"
)

// RID represents a Record Identifier in the database
type RID struct {
	PageID  PageID_t
	SlotNum uint32
}

// NewRID creates a new RID with the given page identifier and slot number
func NewRID(pageID PageID_t, slotNum uint32) *RID {
	return &RID{
		PageID:  pageID,
		SlotNum: slotNum,
	}
}

// NewRIDFromInt64 creates a new RID from a single int64 value
func NewRIDFromInt64(rid int64) *RID {
	return &RID{
		PageID:  PageID_t(rid >> 32),
		SlotNum: uint32(rid),
	}
}

// Get returns the RID as an int64
func (rid *RID) Get() int64 {
	return int64(rid.PageID)<<32 | int64(rid.SlotNum)
}

// GetPageID returns the page identifier
func (rid *RID) GetPageID() PageID_t {
	return rid.PageID
}

// GetSlotNum returns the slot number
func (rid *RID) GetSlotNum() uint32 {
	return rid.SlotNum
}

// Set updates the page identifier and slot number
func (rid *RID) Set(pageID PageID_t, slotNum uint32) {
	rid.PageID = pageID
	rid.SlotNum = slotNum
}

// ToString returns a string representation of the RID
func (rid *RID) ToString() string {
	return fmt.Sprintf("page_id: %d slot_num: %d", rid.PageID, rid.SlotNum)
}

// Equal checks if two RIDs are equal
func (rid *RID) Equal(other *RID) bool {
	return rid.PageID == other.PageID && rid.SlotNum == other.SlotNum
}

// Hash generates a hash value for the RID
func (rid *RID) Hash() uint32 {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%d:%d", rid.PageID, rid.SlotNum)))
	return h.Sum32()
}
