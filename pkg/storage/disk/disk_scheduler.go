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
	"fmt"
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
	PageID common.PageID_t

	// Channel used to signal to the request issuer when the request has been completed.
	Callback chan bool
}

type DiskScheduler struct {
	// Pointer to the disk manager.
	diskManager *DiskManager
	// A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called, `nil` is put into the queue to signal to the background thread to stop execution. */
	RequestQueue common.Channel[*DiskRequest] // esto debe testearse fijo
	// Mutex to synchronize access to shared resources. */
	Mutex sync.Mutex
	// Wait group for tracking the worker thread. */
	WaitGroup sync.WaitGroup
}

func NewScheduler(dm *DiskManager) *DiskScheduler {
	return &DiskScheduler{
		diskManager:  dm,
		RequestQueue: *common.NewChannel[*DiskRequest](),
	}
}

func (ds *DiskScheduler) Schedule(request *DiskRequest) {
	ds.RequestQueue.Put(request)
}

func (ds *DiskScheduler) StartWorkerThread() {
	go func() {
		for {
			request := ds.RequestQueue.Get()
			if request == nil {
				return
			}
			if request.IsWrite {
				err := ds.diskManager.WritePage(request.PageID, request.Data)
				if err != nil {
					fmt.Println("unexpedted I/O error:", err.Error())
					request.Callback <- false
				} else {
					request.Callback <- true
				}
			} else {
				data, err := ds.diskManager.ReadPage(request.PageID)
				if err != nil {
					fmt.Println("unexpected I/O error:", err.Error())
					request.Callback <- false
				} else {
					request.Data = data
					request.Callback <- true
				}
			}
		}
	}()
}
