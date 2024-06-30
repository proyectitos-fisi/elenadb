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
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// DiskManager takes care of the allocation and deallocation of pages within a database.
// It performs the reading and writing of pages to and from disk, providing a logical file layer within the context of a database management system.
type DiskManager struct {
	logName    string
	numFlushes int32
	numWrites  int32
	flushLogF  chan struct{}
	latch      sync.RWMutex
	dbDir      string
	logFile    *os.File
	logLatch   sync.RWMutex
	log        *common.Logger
}

// NewDiskManager creates a new disk manager that writes to the specified database file.
// @param dbFile: the file name of the database file to write to
// TODO: should open a directory instead
func NewDiskManager(dbDir string) (*DiskManager, error) {
	dm := &DiskManager{
		dbDir:     dbDir,
		flushLogF: make(chan struct{}),
		latch:     sync.RWMutex{},
		logLatch:  sync.RWMutex{},
		log:       common.NewLogger('📀'),
	}

	// go dm.FlushLogRoutine()
	return dm, nil
}

// ShutDown: shuts down the disk manager and closes all the file resources.
func (dm *DiskManager) ShutDown() {
	dm.latch.Lock()
	dm.logLatch.Lock()
	defer dm.latch.Unlock()
	defer dm.logLatch.Unlock()
	// TODO: gracefully close the file resources
}

// WritePage: writes a page to the database file.
// @param pageID: id of the page
// @param pageData: raw page data
func (dm *DiskManager) WritePage(pageID common.PageID_t, pageData []byte, filename string) error {
	dm.latch.Lock()
	defer dm.latch.Unlock()
	offset := int64(pageID.GetActualPageId()) * common.ElenaPageSize

	// open to write, don't create if not exists. don't flush on open
	file, err := os.OpenFile(filepath.Join(dm.dbDir, filename), os.O_WRONLY, 0755)
	defer file.Close()
	_, err = file.WriteAt(pageData, offset)
	if err == nil {
		dm.log.Info(
			"writing %d bytes to '%s' (file_id=%d, apage_id=%d)",
			common.ElenaPageSize, filename, pageID.GetFileId(), pageID.GetActualPageId(),
		)
		atomic.AddInt32(&dm.numWrites, 1)
	}
	return err
}

// ReadPage: reads a page from the database file.
// @param pageID: id of the page
// @param pageData: output buffer
func (dm *DiskManager) ReadPage(pageID common.PageID_t, filename string) ([]byte, error) {
	dm.latch.RLock()
	defer dm.latch.RUnlock()

	offset := int64(pageID.GetActualPageId()) * common.ElenaPageSize
	pageData := make([]byte, common.ElenaPageSize)
	// open to read. don't create if not exists
	file, err := os.OpenFile(filepath.Join(dm.dbDir, filename), os.O_RDONLY, 0755)
	defer file.Close()
	_, err = file.ReadAt(pageData, offset)
	if err == nil {
		dm.log.Info(
			"reading %d bytes from '%s' (file_id=%d, apage_id=%d)",
			common.ElenaPageSize, filename, pageID.GetFileId(), pageID.GetActualPageId(),
		)
	}
	return pageData, err
}

// WriteLog: flushes the entire log buffer into disk.
// @param logData: raw log data
func (dm *DiskManager) WriteLog(format string, a ...any) {
	dm.logLatch.Lock()
	defer dm.logLatch.Unlock()
	_, err := dm.logFile.Write([]byte(fmt.Sprintf(format, a...)))
	if err != nil {
		panic(err)
	}
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

// ✅ GetFileSize: gets the size of the specified file.
// @param fileName: name of the file
// @return the size of the file and error if any
func GetFileSize(fileName string) (int64, error) {
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
