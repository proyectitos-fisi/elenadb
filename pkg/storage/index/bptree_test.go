package storage

import (
	"fmt"
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
