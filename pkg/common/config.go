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
	InvalidPageID  = PageID_t(-1)
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
type PageID_t int32
type TxnID_t int64
type LSN_t int32
type SlotOffset_t int
type OID_t uint16

const TXNStartID TxnID_t = 1 << 62

const VarcharDefaultLength = 128
