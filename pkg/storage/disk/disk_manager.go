//===----------------------------------------------------------------------===//
//
//                         🚄 ElenaDB ®
//
// disk_manager.go
//
// Identification: pkg/storage/disk/disk_manager.go
//
// Copyright (c) 2024
//
//===----------------------------------------------------------------------===//

package storage_disk

import (
	"fisi/elenadb/pkg/common"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DiskManager takes care of the allocation and deallocation of pages within a database.
// It performs the reading and writing of pages to and from disk, providing a logical file layer within the context of a database management system.
type DiskManager struct {
	fileName   string
	logName    string
	numFlushes int32
	numWrites  int32
	flushLog   bool
	flushLogF  chan struct{}
	latch      sync.RWMutex
	logLatch   sync.RWMutex
	dbFile     *os.File
	logFile    *os.File
}

// NewDiskManager creates a new disk manager that writes to the specified database file.
// @param dbFile: the file name of the database file to write to
// TODO: should open a directory instead
func NewDiskManager(dbDir string) (*DiskManager, error) {
	db, err := os.OpenFile(dbDir, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	logFileName := fmt.Sprintf("%s.log", dbDir)
	logFile, err := os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	dm := &DiskManager{
		fileName:  dbDir,
		logName:   logFileName,
		dbFile:    db,
		logFile:   logFile,
		flushLogF: make(chan struct{}),
	}

	go dm.FlushLogRoutine()

	return dm, nil
}

// ShutDown: shuts down the disk manager and closes all the file resources.
func (dm *DiskManager) ShutDown() {
	dm.latch.Lock()
	dm.logLatch.Lock()
	defer dm.latch.Unlock()
	defer dm.logLatch.Unlock()
	if dm.dbFile != nil {
		dm.dbFile.Close()
		dm.dbFile = nil
	}
	if dm.logFile != nil {
		dm.logFile.Close()
		dm.logFile = nil
	}
}

// WritePage: writes a page to the database file.
// @param pageID: id of the page
// @param pageData: raw page data
func (dm *DiskManager) WritePage(pageID common.PageID_t, pageData []byte) error {
	dm.latch.Lock()
	defer dm.latch.Unlock()
	offset := int64(pageID) * common.ElenaPageSize
	_, err := dm.dbFile.WriteAt(pageData, offset)
	if err == nil {
		atomic.AddInt32(&dm.numWrites, 1)
	}
	return err
}

// ReadPage: reads a page from the database file.
// @param pageID: id of the page
// @param pageData: output buffer
func (dm *DiskManager) ReadPage(pageID common.PageID_t) ([]byte, error) {
	dm.latch.RLock()
	defer dm.latch.RUnlock()
	offset := int64(pageID) * common.ElenaPageSize
	pageData := make([]byte, common.ElenaPageSize)
	_, err := dm.dbFile.ReadAt(pageData, offset)
	return pageData, err
}

// WriteLog: flushes the entire log buffer into disk.
// @param logData: raw log data
func (dm *DiskManager) WriteLog(logData []byte) error {
	dm.logLatch.Lock()
	defer dm.logLatch.Unlock()
	_, err := dm.logFile.Write(logData)
	if err == nil {
		atomic.AddInt32(&dm.numFlushes, 1)
	}
	return err
}

// ReadLog reads a log entry from the log file.
// @param logData output buffer
// @param size size of the log entry
// @param offset offset of the log entry in the file
// @return true if the read was successful, false otherwise
func (dm *DiskManager) ReadLog(logData []byte, size, offset int) (bool, error) {
	dm.logLatch.RLock()
	defer dm.logLatch.RUnlock()
	_, err := dm.logFile.ReadAt(logData, int64(offset))
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetNumFlushes returns the number of disk flushes.
// @return the number of disk flushes
func (dm *DiskManager) GetNumFlushes() int32 {
	return dm.numFlushes
}

// GetFlushState returns true if the in-memory content has not been flushed yet.
// @return true iff the in-memory content has not been flushed yet
func (dm *DiskManager) GetFlushState() bool {
	return dm.flushLog
}

// GetNumWrites returns the number of disk writes.
// @return the number of disk writes
func (dm *DiskManager) GetNumWrites() int32 {
	return dm.numWrites
}

// ✅ SetFlushLogFuture: sets the future which is used to check for non-blocking flushes.
// @param ch: the channel to set
// func (dm *DiskManager) SetFlushLogFuture(ch chan struct{}) {
// 	dm.flushLogF = ch
// }

// HasFlushLogFuture: checks if the non-blocking flush future was set.
// @return true if the non-blocking flush future was set, false otherwise
func (dm *DiskManager) HasFlushLogFuture() bool {
	return dm.flushLogF != nil
}

// ✅ GetFileSize: gets the size of the specified file.
// @param fileName: name of the file
// @return the size of the file and error if any
func (dm *DiskManager) GetFileSize(fileName string) (int64, error) {
	info, err := os.Stat(fileName)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// FlushLogRoutine: handles periodic flushing of logs if enabled.
func (dm *DiskManager) FlushLogRoutine() {
	for {
		select {
		case <-time.After(common.LogTimeout * time.Millisecond):
			dm.logLatch.Lock()
			if dm.logFile != nil {
				dm.logFile.Sync()
			}
			dm.logLatch.Unlock()
		case <-dm.flushLogF:
			dm.logLatch.Lock()
			if dm.logFile != nil {
				dm.logFile.Sync()
			}
			dm.logLatch.Unlock()
		}
	}
}
