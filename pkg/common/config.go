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
	InvalidPageID  = PageID(-1)
	InvalidFrameID = FrameID(-1)
	InvalidTxnID   = TxnID(-1)
	InvalidLSN     = LSN(-1)
	HeaderPageID   = 0
	ElenaPageSize  = 4096
	BufferPoolSize = 10
	LogBufferSize  = (BufferPoolSize + 1) * ElenaPageSize
	BucketSize     = 50
	LRUKReplacerK  = 10
	// Pavlo says more system use K = 2 <https://youtu.be/BS5h8QZHCPk?si=Gsie7D2qB1aPJx1F&t=3389>
)

type FrameID int32
type PageID int32
type TxnID int64
type LSN int32
type SlotOffset int
type OID uint16

const TXNStartID TxnID = 1 << 62

const VarcharDefaultLength = 128
