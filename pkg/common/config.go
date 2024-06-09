package common

import (
    "sync/atomic"
    "time"
)

// Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds.
var CycleDetectionInterval = time.Millisecond * 1000

// True if logging should be enabled, false otherwise.
var EnableLogging atomic.Value

// If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT.
var LogTimeout = time.Duration(1000) // en teoría esto es ajustable a lo que deseemos (?)

const (
    InvalidPageID  = -1
    InvalidTxnID   = -1
    InvalidLSN     = -1
    HeaderPageID   = 0
    ElenaPageSize = 4096
    BufferPoolSize = 10
    LogBufferSize  = (BufferPoolSize + 1) * ElenaPageSize
    BucketSize     = 50
    LRUKReplacerK  = 10
)

type FrameID int32
type PageID int32
type TxnID int64
type LSN int32
type SlotOffset int
type OID uint16

const TXNStartID TxnID = 1 << 62

const VarcharDefaultLength = 128
