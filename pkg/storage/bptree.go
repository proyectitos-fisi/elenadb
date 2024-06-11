package storage

import (
	"fmt"
	"io"
	"os"
)

// Just a test with a simple btree in go
// This is similar in Rust, Struct and an implementation
// A node in a btree
type BPTreeNode struct {
	Keys     []int         // slice of an int
	children []*BPTreeNode // childens
	isLeaf   bool
	_        *BPTreeNode
	next     *BPTreeNode
	pageIDs  []int
}

func NewBPTreeNode(isLeaf bool) *BPTreeNode {
	return &BPTreeNode{
		Keys:     []int{},
		children: []*BPTreeNode{},
		isLeaf:   isLeaf,
		pageIDs:  []int{},
	}
}

type BPTree struct {
	Root *BPTreeNode
	t    int // Minimum degree (defines the range for number of Keys)
}

func NewBPTree(t int) *BPTree {
	Root := NewBPTreeNode(true)
	return &BPTree{Root: Root, t: t}
}

// This is the insertion
func (tree *BPTree) Insert(key, pageID int) {
	Root := tree.Root
	if len(Root.Keys) == 2*tree.t-1 {
		newRoot := NewBPTreeNode(false)
		newRoot.children = append(newRoot.children, Root)
		tree.splitChild(newRoot, 0)
		tree.Root = newRoot
	}
	tree.insertNonFull(tree.Root, key, pageID)
}

func (tree *BPTree) insertNonFull(node *BPTreeNode, key, pageID int) {
	i := len(node.Keys) - 1
	if node.isLeaf {
		node.Keys = append(node.Keys, 0)
		node.pageIDs = append(node.pageIDs, 0)
		for i >= 0 && key < node.Keys[i] {
			node.Keys[i+1] = node.Keys[i]
			node.pageIDs[i+1] = node.pageIDs[i]
			i--
		}
		node.Keys[i+1] = key
		node.pageIDs[i+1] = pageID
	} else {
		for i >= 0 && key < node.Keys[i] {
			i--
		}
		i++
		if len(node.children[i].Keys) == 2*tree.t-1 {
			tree.splitChild(node, i)
			if key > node.Keys[i] {
				i++
			}
		}
		tree.insertNonFull(node.children[i], key, pageID)
	}
}

func (tree *BPTree) splitChild(parent *BPTreeNode, i int) {
	t := tree.t
	y := parent.children[i]
	z := NewBPTreeNode(y.isLeaf)

	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = z

	parent.Keys = append(parent.Keys, 0)
	copy(parent.Keys[i+1:], parent.Keys[i:])
	parent.Keys[i] = y.Keys[t-1]

	z.Keys = append(z.Keys, y.Keys[t:]...)
	y.Keys = y.Keys[:t-1]

	if y.isLeaf {
		z.pageIDs = append(z.pageIDs, y.pageIDs[t:]...)
		y.pageIDs = y.pageIDs[:t-1]
		z.next = y.next
		y.next = z
	} else {
		z.children = append(z.children, y.children[t:]...)
		y.children = y.children[:t]
	}
}

// this is the search
func (tree *BPTree) Search(key int) (*BPTreeNode, int) {
	return tree.search(tree.Root, key)
}

func (tree *BPTree) search(node *BPTreeNode, key int) (*BPTreeNode, int) {
	i := 0
	for i < len(node.Keys) && key > node.Keys[i] {
		i++
	}
	if i < len(node.Keys) && key == node.Keys[i] {
		return node, i
	}
	if node.isLeaf {
		return nil, -1
	}
	return tree.search(node.children[i], key)
}

// inserting pages id and keys
func (tree *BPTree) InsertKeyValue(file *os.File, key, value int) {
	// Find or create a page for the new tuple
	pageID := findOrCreatePage(file)

	// Insert the tuple into the page
	page, err := ReadPage(file, pageID)
	if err != nil {
		fmt.Println("Error reading page:", err)
		return
	}
	page.Tuples = append(page.Tuples, Tuple{Key: key, Value: value})
	page.NumTuples++

	err = WritePage(file, page)
	if err != nil {
		fmt.Println("Error writing page:", err)
		return
	}

	// Insert the key and pageID into the B+Tree
	tree.Insert(key, pageID)
}

func findOrCreatePage(file *os.File) int {
	// For simplicity, always use page 0 in this example.
	pageID := 0
	page, err := ReadPage(file, pageID)
	if err != nil && err != io.EOF {
		fmt.Println("Error reading page:", err)
		return -1
	}
	if err == io.EOF {
		// Initialize the page if it doesn't exist
		page = &PageKV{
			PageID:          pageID,
			NumTuples:       0,
			FreeSpaceOffset: 10, // Start after the header
			Tuples:          []Tuple{},
		}
		WritePage(file, page)
	}
	return pageID
}

// print the tree
func PrintTree(node *BPTreeNode, level int) {
	if node != nil {
		fmt.Printf("Level %d: %v\n", level, node.Keys)
		if !node.isLeaf {
			for _, child := range node.children {
				PrintTree(child, level+1)
			}
		}
	}
}
