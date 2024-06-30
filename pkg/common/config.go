//===----------------------------------------------------------------------===//
//
//                         🚄 ElenaDB ®
//
// config.go
//
// Identification: pkg/common/config.go
//
// Copyright (c) 2024
//
//===----------------------------------------------------------------------===//

package common

import (
	"fmt"
	"sync/atomic"
	"time"
)

const (
	Name        = "elenadb"
	DisplayName = "🚄 Elena"
	Description = "🚄 The Elena Database"
	Version     = "0.0.69-alpha"
)

// Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds.
var CycleDetectionInterval = time.Millisecond * 1000

// True if logging should be enabled, false otherwise.
var EnableLogging atomic.Value

// If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT.
var LogTimeout = time.Duration(1000) // en teoría esto es ajustable a lo que deseemos (?)

const (
	InvalidPageID  = PageID_t(4294967295)
	InvalidFrameID = FrameID_t(-1)
	InvalidTxnID   = TxnID_t(-1)
	InvalidLSN     = LSN_t(-1)
	HeaderPageID   = 0
	ElenaPageSize  = 4096
	BufferPoolSize = 10
	LogBufferSize  = (BufferPoolSize + 1) * ElenaPageSize
	BucketSize     = 50
	LRUKReplacerK  = 10
	// Pavlo says more system use K = 2 <https://youtu.be/BS5h8QZHCPk?si=Gsie7D2qB1aPJx1F&t=3389>
)

type FrameID_t int32
type PageID_t uint32 // PageId = FileID + APageID
type FileID_t uint16
type APageID_t uint16 // "Actual" Page ID
type TxnID_t int64
type LSN_t int32
type SlotOffset_t uint16
type SlotNumber_t uint16 // Slot number is slot index
type OID_t uint16

const TXNStartID TxnID_t = 1 << 62

const VarcharDefaultLength = 128

func (pid *PageID_t) GetFileId() FileID_t {
	return FileID_t(*pid >> 16)
}

func (pid *PageID_t) GetActualPageId() APageID_t {
	return APageID_t(*pid & 0xFFFF)
}

func (pid *PageID_t) ToString() string {
	return fmt.Sprintf("(%d,%d)", pid.GetFileId(), pid.GetActualPageId())
}

// ParsePageID: splits the pageID into fileID and apID.
// @param pageID: the ID of the page
// @return fileID: the ID of the file
// @return apID: the ID of the actual page within the file
func ParsePageID(pageID PageID_t) (FileID_t, APageID_t) {
	fileID := pageID >> 16      // high 16 bits
	apID := pageID & 0x0000FFFF // low 16 bits
	return FileID_t(fileID), APageID_t(apID)
}

func NewPageIdFromParts(fileID FileID_t, actualPageID APageID_t) PageID_t {
	return PageID_t(fileID)<<16 | PageID_t(actualPageID)
}
