package storage

import "fmt"

// Just a test with a simple btree in go
// This is similar in Rust, Struct and an implementation
// A node in a btree
type BPTreeNode struct {
	Keys     []int         // slice of an int
	children []*BPTreeNode // childens
	isLeaf   bool
	parent   *BPTreeNode
	next     *BPTreeNode
}

func NewBPTreeNode(isLeaf bool) *BPTreeNode {
	return &BPTreeNode{
		Keys:     []int{},
		children: []*BPTreeNode{},
		isLeaf:   isLeaf,
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
func (tree *BPTree) Insert(key int) {
	Root := tree.Root
	if len(Root.Keys) == 2*tree.t-1 {
		newRoot := NewBPTreeNode(false)
		newRoot.children = append(newRoot.children, Root)
		tree.splitChild(newRoot, 0)
		tree.Root = newRoot
	}
	tree.insertNonFull(tree.Root, key)
}

func (tree *BPTree) insertNonFull(node *BPTreeNode, key int) {
	i := len(node.Keys) - 1
	if node.isLeaf {
		node.Keys = append(node.Keys, 0)
		for i >= 0 && key < node.Keys[i] {
			node.Keys[i+1] = node.Keys[i]
			i--
		}
		node.Keys[i+1] = key
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
		tree.insertNonFull(node.children[i], key)
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

	if !y.isLeaf {
		z.children = append(z.children, y.children[t:]...)
		y.children = y.children[:t]
	}

	if y.isLeaf {
		z.next = y.next
		y.next = z
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
