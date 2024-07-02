package storage

import (
	"fisi/elenadb/pkg/buffer"
	"fisi/elenadb/pkg/catalog"
	"fisi/elenadb/pkg/common"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRangeSearch(t *testing.T) {
	// Inicializa el DiskManager
	db_dir := "db.elena/"
	common.GloablDbDir = db_dir
	// common.DebugEnabled.Store(true)
	buffer_pool_size := 10
	k := 5

	os.MkdirAll(db_dir, os.ModePerm)
	os.Create(db_dir + "elena_meta.table")
	// defer os.RemoveAll(db_dir)

	bpm := buffer.NewBufferPoolManager(db_dir, uint32(buffer_pool_size), k, catalog.EmptyCatalog())
	catalogFileId := common.FileID_t(0)

	// Inicializa el B+ Tree con el Buffer Pool Manager
	bptree := NewBPTree(bpm, catalogFileId)

	const large = 60000
	key := 1
	for ; key < large; key++ {
		bptree.Insert(key, uint64(key))
		// bptree.PrintTree()
	}

	// bptree.PrintTree()
	keys, values := bptree.RangeSearch(2576, 2576, 0)

	fmt.Printf("Keys: %v", keys)
	fmt.Printf("Values: %v", values)
}

// IntegrationWithBufferpool es una prueba de integración del B+ Tree con el Buffer Pool Manager
func TestIntegrationWithBufferpool(t *testing.T) {
	// Inicializa el DiskManager
	db_dir := "db.elena/"
	common.GloablDbDir = db_dir
	buffer_pool_size := 3
	k := 5

	os.MkdirAll(db_dir, os.ModePerm)
	os.Create(db_dir + "elena_meta.table")
	// defer os.RemoveAll(db_dir)

	bpm := buffer.NewBufferPoolManager(db_dir, uint32(buffer_pool_size), k, catalog.EmptyCatalog())
	catalogFileId := common.FileID_t(0)

	// Inicializa el B+ Tree con el Buffer Pool Manager
	bptree := NewBPTree(bpm, catalogFileId)

	const large = 60000
	key := 1
	for ; key < large; key++ {
		bptree.Insert(key, uint64(key))
		// bptree.PrintTree()
	}
	// bufferPoolManager.FlushEntirePool()

	// Verifica que las claves y valores se hayan insertado correctamente
	keyIterator := 1
	for ; keyIterator < large; keyIterator++ {
		value, found := bptree.Search(keyIterator)
		if !found {
			t.Errorf("Clave %d no encontrada en el B+ Tree", keyIterator)
		}
		fmt.Printf("%v", value)
		if value != uint64(keyIterator) {
			t.Errorf("Valor incorrecto para la clave %d. Se esperaba %d pero se obtuvo %d", keyIterator, keyIterator, value)
		}
	}
	// bptree.PrintTree()
}

func TestEmpiricalAnalysis(t *testing.T) {
	db_dir := "db.elena/"
	common.GloablDbDir = db_dir
	buffer_pool_size := 1000
	k := 5

	os.MkdirAll(db_dir, os.ModePerm)
	os.Create(db_dir + "elena_meta.table")
	defer os.RemoveAll(db_dir)

	bpm := buffer.NewBufferPoolManager(db_dir, uint32(buffer_pool_size), k, catalog.EmptyCatalog())
	catalogFileId := common.FileID_t(0)

	dataSizes := []int{10, 100, 1000, 10000, 60000}

	fmt.Println("Data Size,Insert Time (ms),Search Time (ms),Range Search Time (ms)")

	for _, size := range dataSizes {
		bptree := NewBPTree(bpm, catalogFileId)

		// Measure insert time
		startInsert := time.Now()
		for i := 1; i <= size; i++ {
			bptree.Insert(i, uint64(i))
		}
		insertTime := time.Since(startInsert)

		// Measure search time
		startSearch := time.Now()
		for i := 1; i <= size; i++ {
			_, found := bptree.Search(i)
			if !found {
				t.Errorf("Key %d not found in B+ Tree", i)
			}
		}
		searchTime := time.Since(startSearch)

		// Measure range search time
		startRangeSearch := time.Now()
		lowerBound := 1
		upperBound := size
		keys, _ := bptree.RangeSearch(lowerBound, upperBound, bptree.RootPageID)
		rangeSearchTime := time.Since(startRangeSearch)

		if len(keys) != upperBound-lowerBound+1 {
			t.Errorf("Range search returned unexpected number of results. Expected %d, got %d", upperBound-lowerBound+1, len(keys))
		}

		fmt.Printf("%d,%.2f,%.2f,%.2f\n",
			size,
			float64(insertTime.Milliseconds()),
			float64(searchTime.Milliseconds()),
			float64(rangeSearchTime.Milliseconds()))
	}
}
