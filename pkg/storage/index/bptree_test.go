package storage

import (
	"fisi/elenadb/pkg/buffer"
	"fisi/elenadb/pkg/catalog"
	"fisi/elenadb/pkg/common"
	"fmt"
	"os"
	"testing"
)

func TestRangeSearch(t *testing.T) {
	// Inicializa el DiskManager
	db_dir := "db.elena/"
	common.GloablDbDir = db_dir
	common.DebugEnabled.Store(true)
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

	bptree.PrintTree()
	// keys, values := bptree.RangeSearch(2576, 2576, 0)

	// fmt.Printf("Keys: %v", keys)
	// fmt.Printf("Values: %v", values)
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
