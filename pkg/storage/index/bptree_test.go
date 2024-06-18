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
	tree.Insert(11, "Value 15")
	tree.Insert(12, "Value 15")
	tree.Insert(14, "Value 15")
	tree.Insert(2, "Value 15")
	tree.Insert(5, "Value 15")
	tree.Insert(25, "Value 15")
	tree.Insert(45, "Value 15")
	tree.Insert(252, "Value 15")

	// tree.Insert(122, "Value 15")
	// tree.Insert(225, "Value 15")
	// tree.Insert(125, "Value 15")
	// tree.Insert(445, "Value 15")
	// tree.Insert(232, "Value 15")

	tree.PrintTree()

	value, found := tree.Find(10)
	if found {
		fmt.Println("Found value for key 10:", value) // Output: Found value for key 10: Value 10
	}

	tree.Delete(5)
	tree.PrintTree()
	tree.Delete(14)
	_, found = tree.Find(5)
	if !found {
		fmt.Println("Key 5 not found") // Output: Key 5 not found
	}
	tree.PrintTree()
}

func TestBPlusTree_Insert(t *testing.T) {
	tree := NewBPlusTree()

	// Insert unique keys
	tree.Insert(10, "Value 10")
	tree.Insert(20, "Value 20")
	tree.Insert(5, "Value 5")

	// Insert duplicate key with different value
	tree.Insert(20, "Updated Value 20")

	// Verify values for keys
	value, found := tree.Find(10)
	if !found || value != "Value 10" {
		t.Errorf("Expected value 'Value 10' for key 10, got '%s'", value)
	}

	value, found = tree.Find(20)
	if !found || value != "Value 20" { // Check updated value
		t.Errorf("Expected value 'Value 20' for key 20, got '%s'", value)
	}

	// Verify handling of duplicates
	tree.Insert(20, "Another Value 20") // This should not update the value again

	value, found = tree.Find(20)
	if !found || value != "Value 20" { // Should still have the updated value
		t.Errorf("Expected value 'Value 20' for key 20, got '%s'", value)
	}
}

// TestBPlusTreeLargeData tests the B+ Tree with a large amount of data
func TestBPlusTreeLargeData(t *testing.T) {
	const numKeys = 12410 // Number of keys to insert

	// Create a new B+ Tree
	tree := NewBPlusTree()

	// Insert keys with random values
	for i := 0; i < numKeys; i++ {
		key := rand.Intn(numKeys * 10) // Generate random key
		value := fmt.Sprintf("\nValue %d", key)
		tree.Insert(key, value)
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
	// for i := 0; i < numKeys; i++ {
	// 	key := rand.Intn(numKeys * 10)
	// 	_, found := tree.Find(key)
	// 	if !found {
	// 		fmt.Printf("\nKey %d not found", key)
	// 	}
	// }
}
