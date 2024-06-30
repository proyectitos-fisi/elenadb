package common_test

import (
	"fisi/elenadb/pkg/catalog"
	"fisi/elenadb/pkg/common"
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChannelWorks(t *testing.T) {
	dmanager, err := storage_disk.NewDiskManager("dummy_db")
	assert.Nil(t, err)

	scheduler := storage_disk.NewScheduler(dmanager, catalog.EmptyCatalog())
	scheduler.StartWorkerThread()

	data := make([]byte, common.ElenaPageSize)

	wg := sync.WaitGroup{}
	wg.Add(100)

	for i := 0; i < 100; i++ {
		j := i
		go func() {
			notifier := make(chan bool)

			scheduler.Schedule(
				&storage_disk.DiskRequest{
					PageID:   common.PageID_t(j),
					IsWrite:  true,
					Data:     data,
					Callback: notifier,
				},
			)

			result := <-notifier
			assert.True(t, result)
			wg.Done()
		}()
	}

	wg.Wait()
}
