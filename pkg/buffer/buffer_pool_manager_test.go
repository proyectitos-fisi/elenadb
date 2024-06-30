package buffer_test

import (
	"fisi/elenadb/pkg/buffer"
	"fisi/elenadb/pkg/catalog"
	"fisi/elenadb/pkg/common"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferPoolManagerTestBinaryDataTest(t *testing.T) {
	db_dir := "db.elena/"
	common.GloablDbDir = db_dir
	buffer_pool_size := 10
	k := 5

	lower_bound := int(math.MinInt8)
	upper_bound := int(math.MaxInt8)

	random_byte := func() int {
		return lower_bound + rand.Intn(upper_bound-lower_bound)
	}

	os.MkdirAll(db_dir, os.ModePerm)
	os.Create(db_dir + "elena_meta.table")
	defer os.RemoveAll(db_dir)

	// defer os.RemoveAll(db_dir)
	assert.Nil(t, nil)
	bpm := buffer.NewBufferPoolManager(db_dir, uint32(buffer_pool_size), k, catalog.EmptyCatalog())
	catalogFileId := common.FileID_t(0)

	page0 := bpm.NewPage(catalogFileId)
	page_id_temp := page0.PageId

	// Scenario: The buffer pool is empty. We should be able to create a new page.
	assert.NotNil(t, page0)
	assert.Equal(t, page_id_temp, common.PageID_t(0))

	random_binary_data := make([]byte, common.ElenaPageSize)
	// Generate random binary data
	for i := 0; i < len(random_binary_data); i++ {
		random_binary_data[i] = byte(random_byte())
	}

	// Insert terminal characters both in the middle and at end
	random_binary_data[common.ElenaPageSize/2] = 0x0
	random_binary_data[common.ElenaPageSize-1] = 0x0

	// Scenario: Once we have a page, we should be able to read and write content.
	copy(page0.Data, random_binary_data)
	assert.Equal(t, random_binary_data, page0.Data)

	// Scenario: We should be able to create new pages until we fill up the buffer pool.
	for i := 1; i < buffer_pool_size; i++ {
		assert.NotNil(t, bpm.NewPage(catalogFileId))
	}

	// Scenario: Once the buffer pool is full, we should not be able to create any new pages.
	for i := buffer_pool_size; i < buffer_pool_size*2; i++ {
		assert.Nil(t, bpm.NewPage(catalogFileId))
	}

	// Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages
	for i := 0; i < 5; i++ {
		pageId := common.NewPageIdFromParts(catalogFileId, common.APageID_t(i))
		unpinned := bpm.UnpinPage(pageId, true)
		assert.True(t, unpinned)
		bpm.FlushPage(pageId)
	}
	for i := 0; i < 5; i++ {
		p := bpm.NewPage(catalogFileId)
		assert.NotNil(t, p)
		// Unpin the page here to allow future fetching
		bpm.UnpinPage(p.PageId, false)
	}

	// Scenario: We should be able to fetch the data we wrote a while ago.
	page0 = bpm.FetchPage(0)
	assert.NotNil(t, page0)
	assert.Equal(t, random_binary_data, page0.Data)

	assert.True(t, bpm.UnpinPage(0, true))

	// Shutdown the disk manager and remove the temporary file we created.
	// disk_manager.ShutDown()
}

func TestBufferPoolManagerTestSampleTest(t *testing.T) {
	db_dir := "db.elena/"
	common.GloablDbDir = db_dir
	buffer_pool_size := 10
	k := 5

	os.MkdirAll(db_dir, os.ModePerm)
	os.Create(db_dir + "elena_meta.table")
	defer os.RemoveAll(db_dir)

	bpm := buffer.NewBufferPoolManager(db_dir, uint32(buffer_pool_size), k, catalog.EmptyCatalog())
	catalogFileId := common.FileID_t(0)

	page0 := bpm.NewPage(catalogFileId)
	assert.NotNil(t, page0)
	page_id_temp := page0.PageId

	// Scenario: The buffer pool is empty. We should be able to create a new page.
	assert.NotNil(t, page0)
	assert.Equal(t, common.PageID_t(0), page_id_temp)

	// Scenario: Once we have a page, we should be able to read and write content.
	copy(page0.Data, []byte("Hello"))
	assert.Equal(t, "Hello", string(page0.Data[:5]))

	// Scenario: We should be able to create new pages until we fill up the buffer pool.
	for i := 1; i < buffer_pool_size; i++ {
		p := bpm.NewPage(catalogFileId)
		assert.NotNil(t, p)
		assert.Equal(t, p.PageId, common.PageID_t(i))
	}

	// Scenario: Once the buffer pool is full, we should not be able to create any new pages.
	for i := buffer_pool_size; i < buffer_pool_size*2; i++ {
		assert.Nil(t, bpm.NewPage(catalogFileId))
	}

	// Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
	// there would still be one buffer page left for reading page 0.
	for i := 0; i < 5; i++ {
		assert.True(t, bpm.UnpinPage(common.PageID_t(i), true))
	}
	for i := 0; i < 4; i++ {
		assert.NotNil(t, bpm.NewPage(catalogFileId))
	}

	// Scenario: We should be able to fetch the data we wrote a while ago.
	page0 = bpm.FetchPage(0)
	assert.NotNil(t, page0)
	assert.Equal(t, "Hello", string(page0.Data[:5]))

	// Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
	// now be pinned. Fetching page 0 again should fail.
	assert.True(t, bpm.UnpinPage(0, true))
	pp := bpm.NewPage(catalogFileId)
	assert.NotNil(t, pp)
	assert.Nil(t, bpm.FetchPage(0))

	// Shutdown the disk manager and remove the temporary file we created.
	// disk_manager.ShutDown()
}
