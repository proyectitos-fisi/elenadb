package storage

import (
	"os"
	"testing"
)

func TestInsertAndSearch(t *testing.T) {
	tree := NewBPTree(3)

	keys := []int{10, 20, 5, 6, 12, 30, 7, 17}
	pageIDs := []int{1, 2, 3, 4, 5, 6, 7, 8}

	for i, key := range keys {
		tree.Insert(key, pageIDs[i])
	}

	tree.PrintTree()

	for _, key := range keys {
		node, idx := tree.Search(key)
		if node == nil || node.Keys[idx] != key {
			t.Errorf("Expected to find key %d, but did not", key)
		}
	}
}

func TestDelete(t *testing.T) {
	tree := NewBPTree(3)

	keys := []int{10, 20, 5, 6, 12, 30, 7, 17}
	pageIDs := []int{1, 2, 3, 4, 5, 6, 7, 8}

	for i, key := range keys {
		tree.Insert(key, pageIDs[i])
		tree.PrintTree()
	}

	tree.Delete(6)

	tree.PrintTree()

	node, _ := tree.Search(6)
	if node != nil {
		t.Errorf("Expected key 6 to be deleted, but found in node with keys: %v", node.Keys)
	}

	for _, key := range keys {
		if key != 6 {
			node, idx := tree.Search(key)
			if node == nil || node.Keys[idx] != key {
				t.Errorf("Expected to find key %d, but did not", key)
			}
		}
	}
}

// Not working rn
func TestPersistence(t *testing.T) {
	filename := "bptree_test.gob"
	tree := NewBPTree(3)

	keys := []int{10, 20, 5, 6, 12, 30, 7, 17}
	pageIDs := []int{1, 2, 3, 4, 5, 6, 7, 8}

	for i, key := range keys {
		tree.Insert(key, pageIDs[i])
	}

	err := tree.SaveToFile(filename)
	if err != nil {
		t.Fatalf("Failed to save tree to file: %v", err)
	}

	loadedTree, err := LoadBPTreeFromFile(filename)
	if err != nil {
		t.Fatalf("Failed to load tree from file: %v", err)
	}

	for _, key := range keys {
		node, idx := loadedTree.Search(key)
		if node == nil || node.Keys[idx] != key {
			t.Errorf("Expected to find key %d in loaded tree, but did not", key)
		}
	}

	os.Remove(filename)
}
