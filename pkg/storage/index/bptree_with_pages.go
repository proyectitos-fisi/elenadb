package storage

const (
	maxKeys = 3 // Maximum number of keys per page (adjustable)
	minKeys = 2 // Minimum number of keys per page (adjustable)
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
func (t *BPlusTree) Insert(key int, value string) {
	leaf := t.findLeafNode(t.Root, key)

	// Insert key and value into the leaf node
	insertIndex := t.findIndex(leaf.Keys, key)
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
}

// Find searches for a key in the B+ Tree and returns its value
func (t *BPlusTree) Find(key int) (string, bool) {
	leaf := t.findLeafNode(t.Root, key)
	index := t.findIndex(leaf.Keys, key)
	if index != -1 && leaf.Keys[index] == key {
		return leaf.Values[index], true
	}
	return "", false
}

// Delete deletes a key from the B+ Tree
func (t *BPlusTree) Delete(key int) {
	leaf := t.findLeafNode(t.Root, key)
	index := t.findIndex(leaf.Keys, key)
	if index != -1 && leaf.Keys[index] == key {
		// Remove key and value
		copy(leaf.Keys[index:], leaf.Keys[index+1:])
		leaf.Keys = leaf.Keys[:len(leaf.Keys)-1]

		copy(leaf.Values[index:], leaf.Values[index+1:])
		leaf.Values = leaf.Values[:len(leaf.Values)-1]

		// Check if the leaf node underflows
		if len(leaf.Keys) < minKeys && leaf != t.Root {
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
		copy(leaf.Parent.Keys[index+1:], leaf.Parent.Keys[index:])
		leaf.Parent.Keys[index] = newLeaf.Keys[0]

		copy(leaf.Parent.Children[index+1:], leaf.Parent.Children[index:])
		leaf.Parent.Children[index+1] = newLeaf
		newLeaf.Parent = leaf.Parent
	}
}

// balanceLeafNode balances an underflowed leaf node
func (t *BPlusTree) balanceLeafNode(leaf *Page) {
	if leaf.Parent == nil {
		return
	}
	index := t.findIndex(leaf.Parent.Keys, leaf.Keys[0])
	var sibling *Page

	// Try to borrow from left sibling
	if index > 0 && len(leaf.Parent.Children[index-1].Keys) > minKeys {
		sibling = leaf.Parent.Children[index-1]
		moveKey := sibling.Keys[len(sibling.Keys)-1]
		moveValue := sibling.Values[len(sibling.Values)-1]

		// Move key and value from left sibling to current leaf
		leaf.Keys = append([]int{moveKey}, leaf.Keys...)
		leaf.Values = append([]string{moveValue}, leaf.Values...)

		// Remove key and value from left sibling
		sibling.Keys = sibling.Keys[:len(sibling.Keys)-1]
		sibling.Values = sibling.Values[:len(sibling.Values)-1]

		// Update parent key
		leaf.Parent.Keys[index-1] = leaf.Keys[0]
		return
	}

	// Try to borrow from right sibling
	if index < len(leaf.Parent.Children)-1 && len(leaf.Parent.Children[index+1].Keys) > minKeys {
		sibling = leaf.Parent.Children[index+1]
		moveKey := sibling.Keys[0]
		moveValue := sibling.Values[0]

		// Move key and value from right sibling to current leaf
		leaf.Keys = append(leaf.Keys, moveKey)
		leaf.Values = append(leaf.Values, moveValue)

		// Remove key and value from right sibling
		sibling.Keys = sibling.Keys[1:]
		sibling.Values = sibling.Values[1:]

		// Update parent key
		leaf.Parent.Keys[index] = sibling.Keys[0]
		return
	}

	// Merge with sibling
	if index > 0 {
		sibling = leaf.Parent.Children[index-1]
	} else {
		sibling = leaf.Parent.Children[index+1]
	}

	// Merge leaf with sibling
	sibling.Keys = append(sibling.Keys, leaf.Keys...)
	sibling.Values = append(sibling.Values, leaf.Values...)

	// Remove reference to merged leaf
	copy(leaf.Parent.Keys[index:], leaf.Parent.Keys[index+1:])
	leaf.Parent.Keys = leaf.Parent.Keys[:len(leaf.Parent.Keys)-1]

	copy(leaf.Parent.Children[index:], leaf.Parent.Children[index+1:])
	leaf.Parent.Children = leaf.Parent.Children[:len(leaf.Parent.Children)-1]

	t.balanceLeafNode(leaf.Parent)
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
