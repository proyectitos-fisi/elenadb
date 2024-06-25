package storage

import (
	"fisi/elenadb/pkg/buffer"
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"fmt"
	"testing"
)

// IntegrationWithBufferpool es una prueba de integración del B+ Tree con el Buffer Pool Manager
func TestIntegrationWithBufferpool(t *testing.T) {
	// Inicializa el DiskManager
	dbDir := "db.elena"
	diskManager, err := storage_disk.NewDiskManager(dbDir)
	if err != nil {
		t.Fatalf("No se pudo inicializar el DiskManager: %v", err)
	}
	// defer diskManager.Shutdown()

	// Inicializa el Buffer Pool Manager
	poolSize := uint32(10) // tamaño del buffer pool
	k := 5                 // parámetro K para LRU-K
	bufferPoolManager := buffer.NewBufferPoolManager(poolSize, diskManager, k)

	// Inicializa el B+ Tree con el Buffer Pool Manager
	bptree := NewBPTree(bufferPoolManager)

	const large = 1000
	key := 1
	for ; key < large; key++ {
		bptree.Insert(key, key)
		bptree.PrintTree()
	}
	bufferPoolManager.FlushEntirePool()

	// Verifica que las claves y valores se hayan insertado correctamente
	keyIterator := 1
	for ; keyIterator < large; keyIterator++ {
		value, found := bptree.Search(keyIterator)
		if !found {
			t.Errorf("Clave %d no encontrada en el B+ Tree", keyIterator)
		}
		fmt.Printf("%v", value)
		if value != keyIterator {
			t.Errorf("Valor incorrecto para la clave %d. Se esperaba %d pero se obtuvo %d", keyIterator, keyIterator, value)
		}
	}
	bptree.PrintTree()
}
