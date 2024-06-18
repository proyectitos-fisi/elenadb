package storage

import (
	"errors"
	"fmt"
)

const (
	maxKeys = 10 // Maximum number of keys per page (adjustable)
	minKeys = 5  // Minimum number of keys per page (adjustable)
)

// Page represents a B+ Tree node/page
type Page struct {
	Keys     []int    // Keys stored in the page
	Children []*Page  // Child pages (nil for leaf pages)
	Values   []string // Values stored in leaf pages
	Parent   *Page    // Parent page (nil for root page)
	Leaf     bool     // Indicates if the page is a leaf
}

// BPlusTree represents the B+ Tree
type BPlusTree struct {
	Root *Page // Root of the B+ Tree
}

// NewBPlusTree initializes a new B+ Tree
func NewBPlusTree() *BPlusTree {
	return &BPlusTree{
		Root: &Page{
			Keys:     make([]int, 0, maxKeys),
			Children: make([]*Page, 0, maxKeys+1),
			Values:   make([]string, 0, maxKeys),
			Parent:   nil,
			Leaf:     true,
		},
	}
}

// Insert inserts a key-value pair into the B+ Tree
func (t *BPlusTree) Insert(key int, value string) error {
	leaf := t.findLeafNode(t.Root, key)

	// Check if key already exists in the leaf node
	insertIndex := t.findIndex(leaf.Keys, key)
	if insertIndex < len(leaf.Keys) && leaf.Keys[insertIndex] == key {
		// Key already exists, return an error or handle as needed
		return errors.New("duplicate key found")
	}

	// Key does not exist, proceed with insertion
	leaf.Keys = append(leaf.Keys, 0)                         // Create space for new key
	copy(leaf.Keys[insertIndex+1:], leaf.Keys[insertIndex:]) // Shift keys to the right
	leaf.Keys[insertIndex] = key

	leaf.Values = append(leaf.Values, "")
	copy(leaf.Values[insertIndex+1:], leaf.Values[insertIndex:])
	leaf.Values[insertIndex] = value

	// Check if the leaf node overflows
	if len(leaf.Keys) > maxKeys {
		t.splitLeafNode(leaf)
	}

	return nil
}

// Find searches for a key in the B+ Tree and returns its value
func (t *BPlusTree) Find(key int) (string, bool) {
	leaf := t.findLeafNode(t.Root, key)
	index := t.findIndex(leaf.Keys, key)

	if index < len(leaf.Keys) && leaf.Keys[index] == key {
		return leaf.Values[index], true
	}

	return "", false
}

// Delete deletes a key from the B+ Tree
func (t *BPlusTree) Delete(key int) {
	leaf := t.findLeafNode(t.Root, key)
	index := t.findIndex(leaf.Keys, key)

	if index != -1 && index < len(leaf.Keys) && leaf.Keys[index] == key {
		// Remove key and value
		copy(leaf.Keys[index:], leaf.Keys[index+1:])
		leaf.Keys = leaf.Keys[:len(leaf.Keys)-1]

		copy(leaf.Values[index:], leaf.Values[index+1:])
		leaf.Values = leaf.Values[:len(leaf.Values)-1]

		// Check if the leaf node underflows
		if len(leaf.Keys) < minKeys && leaf != t.Root {
			fmt.Println("Balancing leaf node...")
			t.balanceLeafNode(leaf)
		}
	}
}

// findLeafNode finds the leaf node where the key should be inserted or deleted
func (t *BPlusTree) findLeafNode(current *Page, key int) *Page {
	if current.Leaf {
		return current
	}
	index := t.findIndex(current.Keys, key)
	return t.findLeafNode(current.Children[index], key)
}

// splitLeafNode splits a full leaf node into two
func (t *BPlusTree) splitLeafNode(leaf *Page) {
	middleIndex := len(leaf.Keys) / 2

	// Create a new leaf node
	newLeaf := &Page{
		Keys:   make([]int, maxKeys),
		Values: make([]string, maxKeys),
		Parent: leaf.Parent,
		Leaf:   true,
	}

	// Move half of the keys and values to the new leaf node
	copy(newLeaf.Keys, leaf.Keys[middleIndex:])
	copy(newLeaf.Values, leaf.Values[middleIndex:])
	newLeaf.Keys = newLeaf.Keys[:len(leaf.Keys)-middleIndex]
	newLeaf.Values = newLeaf.Values[:len(leaf.Values)-middleIndex]

	leaf.Keys = leaf.Keys[:middleIndex]
	leaf.Values = leaf.Values[:middleIndex]

	// Update parent pointers
	if leaf.Parent == nil {
		newParent := &Page{
			Keys:     []int{newLeaf.Keys[0]},
			Children: []*Page{leaf, newLeaf},
			Parent:   nil,
			Leaf:     false,
		}
		leaf.Parent = newParent
		newLeaf.Parent = newParent
		t.Root = newParent
	} else {
		index := t.findIndex(leaf.Parent.Keys, newLeaf.Keys[0])
		leaf.Parent.Keys = append(leaf.Parent.Keys, 0) // Ensure enough capacity for new key
		copy(leaf.Parent.Keys[index+1:], leaf.Parent.Keys[index:])
		leaf.Parent.Keys[index] = newLeaf.Keys[0]

		leaf.Parent.Children = append(leaf.Parent.Children, nil) // Ensure enough capacity for new child
		copy(leaf.Parent.Children[index+1:], leaf.Parent.Children[index:])
		leaf.Parent.Children[index+1] = newLeaf
		newLeaf.Parent = leaf.Parent
	}
}

// balanceLeafNode balances an underflowed leaf node
func (t *BPlusTree) balanceLeafNode(leaf *Page) {
	if leaf.Parent == nil {
		// If the leaf is the root, no balancing needed
		return
	}

	index := t.findIndex(leaf.Parent.Keys, leaf.Keys[0])

	// Debugging: print the current state of the node and its parent
	fmt.Printf("Balancing node with keys: %v\n", leaf.Keys)
	if leaf.Parent != nil {
		fmt.Printf("Parent keys: %v\n", leaf.Parent.Keys)
	}

	// Ensure the index is within valid bounds
	if index < 0 || index >= len(leaf.Parent.Children) {
		return
	}

	// Try to borrow from the left sibling
	if index > 0 && leaf.Parent.Children[index-1] != nil && len(leaf.Parent.Children[index-1].Keys) > minKeys {
		fmt.Printf("Borrowing from the left node: %v\n", leaf.Parent.Children[index-1])
		t.borrowFromLeft(leaf.Parent, index)
		return
	}

	// Try to borrow from the right sibling
	if index < len(leaf.Parent.Children)-1 && leaf.Parent.Children[index+1] != nil && len(leaf.Parent.Children[index+1].Keys) > minKeys {
		fmt.Printf("Borrowing from the righ node: %v\n", leaf.Parent.Children[index+1])
		t.borrowFromRight(leaf.Parent, index)
		return
	}

	// If borrowing is not possible, merge with sibling
	if index > 0 {
		t.merge(leaf.Parent, index-1)
	} else {
		t.merge(leaf.Parent, index)
	}

	fmt.Printf("Parent node: %v", leaf.Parent)
	// Recursively balance the parent node
	if leaf.Parent != nil {
		if len(leaf.Parent.Keys) < minKeys {
			t.balanceLeafNode(leaf.Parent)
		}
	}
}

// merge merges a node with its sibling
func (t *BPlusTree) merge(parent *Page, index int) {
	if parent == nil || index < 0 || index >= len(parent.Children)-1 {
		return
	}

	fmt.Printf("Merging: %v and %v \n", parent.Children[index], parent.Children[index+1])
	left := parent.Children[index]
	right := parent.Children[index+1]

	if left == nil || right == nil {
		return
	}

	// Merge right node into left node
	// left.Keys = append(left.Keys, parent.Keys[index]) // Add parent's key to the left node
	// fmt.Printf("Left node: %v", left.Keys)
	left.Keys = append(left.Keys, right.Keys...) // Add keys from the right node to the left node
	fmt.Printf("Left node: %v\n", left.Keys)
	if left.Leaf {
		left.Values = append(left.Values, right.Values...) // Add values from the right node to the left node (for leaf nodes)
	} else {
		left.Children = append(left.Children, right.Children...) // Add children from the right node to the left node (for internal nodes)
		for _, child := range right.Children {
			if child != nil {
				child.Parent = left // Update parent pointer of each child in the left node
			}
		}
	}

	// Remove the key and child from the parent
	parent.Keys = append(parent.Keys[:index], parent.Keys[index+1:]...) // Remove the key from the parent
	fmt.Printf("Parent node: %v\n", parent.Keys)
	parent.Children = append(parent.Children[:index], parent.Children[index+1:]...) // Remove the right child from the parent
	fmt.Printf("Parent node: %v\n", parent.Children)

	// If the parent is the root and has no keys, make the left child the new root
	if parent == t.Root && len(parent.Keys) == 0 {
		fmt.Printf("Parent node: %v\n", parent.Keys)
		t.Root = left
		left.Parent = nil
		fmt.Printf("Parent node: %v\n", t.Root)
	}
}

// borrowFromLeft borrows a key from the left sibling
func (t *BPlusTree) borrowFromLeft(parent *Page, index int) {
	if parent == nil || index <= 0 || index >= len(parent.Children) {
		return
	}

	child := parent.Children[index]
	leftSibling := parent.Children[index-1]

	// Ensure the sibling pointers are not nil
	if child == nil || leftSibling == nil {
		return
	}

	// Move the last key from the left sibling to the child
	child.Keys = append([]int{parent.Keys[index-1]}, child.Keys...)
	parent.Keys[index-1] = leftSibling.Keys[len(leftSibling.Keys)-1]
	leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]

	// Move the corresponding value or child pointer
	if child.Leaf {
		child.Values = append([]string{leftSibling.Values[len(leftSibling.Values)-1]}, child.Values...)
		leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]
	} else {
		child.Children = append([]*Page{leftSibling.Children[len(leftSibling.Children)-1]}, child.Children...)
		leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
		child.Children[0].Parent = child
	}
}

// borrowFromRight borrows a key from the right sibling
func (t *BPlusTree) borrowFromRight(parent *Page, index int) {
	if parent == nil || index < 0 || index >= len(parent.Children)-1 {
		return
	}

	child := parent.Children[index]
	rightSibling := parent.Children[index+1]

	// Ensure the sibling pointers are not nil
	if child == nil || rightSibling == nil {
		return
	}

	// Move the first key from the right sibling to the child
	child.Keys = append(child.Keys, parent.Keys[index])
	parent.Keys[index] = rightSibling.Keys[0]
	rightSibling.Keys = rightSibling.Keys[1:]

	// Move the corresponding value or child pointer
	if child.Leaf {
		child.Values = append(child.Values, rightSibling.Values[0])
		rightSibling.Values = rightSibling.Values[1:]
	} else {
		child.Children = append(child.Children, rightSibling.Children[0])
		rightSibling.Children = rightSibling.Children[1:]
		child.Children[len(child.Children)-1].Parent = child
	}
}

// findIndex finds the index where a key should be inserted or located
func (t *BPlusTree) findIndex(keys []int, key int) int {
	for i, k := range keys {
		if key <= k {
			return i
		}
	}
	return len(keys)
}

// PrintTree prints the B+ Tree structure starting from the root
func (t *BPlusTree) PrintTree() {
	fmt.Println("Printing B+ Tree:")
	t.printNode(t.Root, 0)
}

// printNode recursively prints a node and its children
func (t *BPlusTree) printNode(node *Page, level int) {
	if node == nil {
		return
	}

	indent := ""
	for i := 0; i < level; i++ {
		indent += "\t"
	}

	fmt.Printf("%sLevel %d (Leaf: %v): Keys: %v, Values: %v\n", indent, level, node.Leaf, node.Keys, node.Values)

	if !node.Leaf {
		for i, child := range node.Children {
			fmt.Printf("%sChild %d of parent %v:\n", indent, i, node.Keys)
			t.printNode(child, level+1)
		}
	}
}
