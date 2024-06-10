//===----------------------------------------------------------------------===//
//
//                         🚄 ElenaDB ®
//
// disk_scheduler.go
//
// Identification: pkg/storage/disk/disk_scheduler.go
//
// Copyright (c) 2024
//
//===----------------------------------------------------------------------===//

package storage_disk

import (
	"fisi/elenadb/pkg/common"
	"sync"
)

// Represents a Write or Read request for the DiskManager to execute.

type DiskRequest struct {
	// Flag indicating whether the request is a write or a read.
	IsWrite bool

	/*
	 *  Pointer to the start of the memory location where a page is either:
	 *   1. being read into from disk (on a read).
	 *   2. being written out to disk (on a write).
	 */
	Data []byte

	// ID of the page being read from / written to disk.
	PageID common.PageID

	// Channel used to signal to the request issuer when the request has been completed.
	Callback chan bool
}

type DiskScheduler struct {
	// Pointer to the disk manager.
	DiskManager *DiskManager
	// A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called, `nil` is put into the queue to signal to the background thread to stop execution. */
	RequestQueue common.Channel[*DiskRequest] // esto debe testearse fijo
	// Mutex to synchronize access to shared resources. */
	Mutex sync.Mutex
	// Wait group for tracking the worker thread. */
	WaitGroup sync.WaitGroup
}
