package storage

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBPTreeInsert(t *testing.T) {
	tree := NewBPlusTree()

	tree.Insert(10, "Value 20")
	tree.Insert(20, "Value 20")
	tree.Insert(5, "Value 5")
	tree.Insert(15, "Value 15")

	value, found := tree.Find(10)
	if found {
		fmt.Println("Found value for key 10:", value) // Output: Found value for key 10: Value 10
	}

	tree.Delete(5)
	_, found = tree.Find(5)
	if !found {
		fmt.Println("Key 5 not found") // Output: Key 5 not found
	}
}

// TestBPlusTreeLargeData tests the B+ Tree with a large amount of data
func TestBPlusTreeLargeData(t *testing.T) {
	const numKeys = 4000 // Number of keys to insert

	// Create a new B+ Tree
	tree := NewBPlusTree()

	// Insert keys with random values
	for i := 0; i < numKeys; i++ {
		key := rand.Intn(numKeys * 10) // Generate random key
		value := fmt.Sprintf("\nValue %d", key)
		tree.Insert(key, value)
	}

	// Validate inserted keys
	for i := 0; i < numKeys; i++ {
		key := rand.Intn(numKeys * 10)
		value, found := tree.Find(key)
		if found {
			fmt.Printf("\nFound value for key %d: %s", key, value) // Output: Found value for key 10: Value 10
		}
	}

	// Delete keys
	for i := 0; i < numKeys/2; i++ {
		key := rand.Intn(numKeys * 10)
		tree.Delete(key)
		_, found := tree.Find(key)
		if found {
			t.Errorf("\nFailed to delete key %d", key)
		}
	}

	// Validate remaining keys
	for i := 0; i < numKeys; i++ {
		key := rand.Intn(numKeys * 10)
		_, found := tree.Find(key)
		if !found {
			fmt.Printf("\nKey %d not found", key) // Output: Key 5 not found
		}
	}
}
