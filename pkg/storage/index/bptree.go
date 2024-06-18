package storage

// import (
// 	"fmt"
// )

// // Just a test with a simple btree in go
// // This is similar in Rust, Struct and an implementation
// // A node in a btree
// type BPTreeNode struct {
// 	Keys     []int         // slice of an int
// 	Values   []int         // slice of associated values
// 	children []*BPTreeNode // children nodes
// 	isLeaf   bool
// 	next     *BPTreeNode
// }

// func NewBPTreeNode(isLeaf bool) *BPTreeNode {
// 	return &BPTreeNode{
// 		Keys:     []int{},
// 		Values:   []int{},
// 		children: []*BPTreeNode{},
// 		isLeaf:   isLeaf,
// 	}
// }

// type BPTree struct {
// 	Root *BPTreeNode
// 	t    int // Minimum degree (defines the range for number of Keys)
// }

// func NewBPTree(t int) *BPTree {
// 	Root := NewBPTreeNode(true)
// 	return &BPTree{Root: Root, t: t}
// }

// // This is the insertion
// func (tree *BPTree) Insert(key int, value int) {
// 	Root := tree.Root
// 	if len(Root.Keys) == 2*tree.t-1 {
// 		newRoot := NewBPTreeNode(false)
// 		newRoot.children = append(newRoot.children, Root)
// 		tree.splitChild(newRoot, 0)
// 		tree.Root = newRoot
// 	}
// 	tree.insertNonFull(tree.Root, key, value)
// }

// func (tree *BPTree) insertNonFull(node *BPTreeNode, key int, value int) {
// 	i := len(node.Keys) - 1
// 	if node.isLeaf {
// 		node.Keys = append(node.Keys, 0)
// 		node.Values = append(node.Values, 0)
// 		for i >= 0 && key < node.Keys[i] {
// 			node.Keys[i+1] = node.Keys[i]
// 			node.Values[i+1] = node.Values[i]
// 			i--
// 		}
// 		node.Keys[i+1] = key
// 		node.Values[i+1] = value
// 	} else {
// 		for i >= 0 && key < node.Keys[i] {
// 			i--
// 		}
// 		i++
// 		if len(node.children[i].Keys) == 2*tree.t-1 {
// 			tree.splitChild(node, i)
// 			if key > node.Keys[i] {
// 				i++
// 			}
// 		}
// 		tree.insertNonFull(node.children[i], key, value)
// 	}
// }

// func (tree *BPTree) splitChild(parent *BPTreeNode, i int) {
// 	t := tree.t
// 	y := parent.children[i]
// 	z := NewBPTreeNode(y.isLeaf)

// 	parent.children = append(parent.children, nil)
// 	copy(parent.children[i+2:], parent.children[i+1:])
// 	parent.children[i+1] = z

// 	parent.Keys = append(parent.Keys, 0)
// 	parent.Values = append(parent.Values, 0)
// 	copy(parent.Keys[i+1:], parent.Keys[i:])
// 	copy(parent.Values[i+1:], parent.Values[i:])
// 	parent.Keys[i] = y.Keys[t-1]
// 	parent.Values[i] = y.Values[t-1]

// 	z.Keys = append(z.Keys, y.Keys[t:]...)
// 	z.Values = append(z.Values, y.Values[t:]...)
// 	y.Keys = y.Keys[:t-1]
// 	y.Values = y.Values[:t-1]

// 	if y.isLeaf {
// 		z.next = y.next
// 		y.next = z
// 	} else {
// 		z.children = append(z.children, y.children[t:]...)
// 		y.children = y.children[:t]
// 	}
// }

// // this is the search
// func (tree *BPTree) Search(key int) (*BPTreeNode, int) {
// 	return tree.search(tree.Root, key)
// }

// func (tree *BPTree) search(node *BPTreeNode, key int) (*BPTreeNode, int) {
// 	i := 0
// 	for i < len(node.Keys) && key > node.Keys[i] {
// 		i++
// 	}
// 	if i < len(node.Keys) && key == node.Keys[i] {
// 		return node, i
// 	}
// 	if node.isLeaf {
// 		return nil, -1
// 	}
// 	return tree.search(node.children[i], key)
// }

// func (tree *BPTree) Delete(key int) {
// 	tree.delete(tree.Root, key)
// 	if len(tree.Root.Keys) == 0 && !tree.Root.isLeaf {
// 		tree.Root = tree.Root.children[0]
// 	}
// }

// func (tree *BPTree) delete(node *BPTreeNode, key int) {
// 	t := tree.t
// 	i := 0
// 	for i < len(node.Keys) && key > node.Keys[i] {
// 		i++
// 	}

// 	if i < len(node.Keys) && key == node.Keys[i] {
// 		if node.isLeaf {
// 			// Case 1: The key is in a leaf node
// 			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
// 			node.Values = append(node.Values[:i], node.Values[i+1:]...)
// 		} else {
// 			// Case 2: The key is in an internal node
// 			if len(node.children[i].Keys) >= t {
// 				// Case 2a: The left child has at least t keys
// 				predKey := node.children[i].Keys[len(node.children[i].Keys)-1]
// 				predValue := node.children[i].Values[len(node.children[i].Values)-1]
// 				node.Keys[i] = predKey
// 				node.Values[i] = predValue
// 				tree.delete(node.children[i], predKey)
// 			} else if len(node.children[i+1].Keys) >= t {
// 				// Case 2b: The right child has at least t keys
// 				succKey := node.children[i+1].Keys[0]
// 				succValue := node.children[i+1].Values[0]
// 				node.Keys[i] = succKey
// 				node.Values[i] = succValue
// 				tree.delete(node.children[i+1], succKey)
// 			} else {
// 				// Case 2c: Both children have t-1 keys
// 				tree.merge(node, i)
// 				tree.delete(node.children[i], key)
// 			}
// 		}
// 	} else {
// 		if node.isLeaf {
// 			// Key not found
// 			return
// 		}
// 		if len(node.children[i].Keys) == t-1 {
// 			if i > 0 && len(node.children[i-1].Keys) >= t {
// 				// Borrow from the left sibling
// 				tree.borrowFromLeft(node, i)
// 			} else if i < len(node.children)-1 && len(node.children[i+1].Keys) >= t {
// 				// Borrow from the right sibling
// 				tree.borrowFromRight(node, i)
// 			} else {
// 				// Merge with a sibling
// 				if i < len(node.children)-1 {
// 					tree.merge(node, i)
// 				} else {
// 					tree.merge(node, i-1)
// 				}
// 			}
// 			if len(node.Keys) == 0 {
// 				// If the root node becomes empty, update the root
// 				tree.Root = node.children[0]
// 			}
// 		}
// 		tree.delete(node.children[i], key)
// 	}
// }

// func (tree *BPTree) merge(parent *BPTreeNode, i int) {
// 	left := parent.children[i]
// 	right := parent.children[i+1]

// 	// Merge right node into left node
// 	left.Keys = append(left.Keys, parent.Keys[i])
// 	left.Values = append(left.Values, parent.Values[i])
// 	left.Keys = append(left.Keys, right.Keys...)
// 	left.Values = append(left.Values, right.Values...)

// 	if !left.isLeaf {
// 		left.children = append(left.children, right.children...)
// 	}

// 	parent.Keys = append(parent.Keys[:i], parent.Keys[i+1:]...)
// 	parent.Values = append(parent.Values[:i], parent.Values[i+1:]...)
// 	parent.children = append(parent.children[:i+1], parent.children[i+2:]...)
// }

// func (tree *BPTree) borrowFromLeft(parent *BPTreeNode, i int) {
// 	child := parent.children[i]
// 	leftSibling := parent.children[i-1]

// 	child.Keys = append([]int{parent.Keys[i-1]}, child.Keys...)
// 	child.Values = append([]int{parent.Values[i-1]}, child.Values...)
// 	parent.Keys[i-1] = leftSibling.Keys[len(leftSibling.Keys)-1]
// 	parent.Values[i-1] = leftSibling.Values[len(leftSibling.Values)-1]
// 	leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
// 	leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]

// 	if !child.isLeaf {
// 		child.children = append([]*BPTreeNode{leftSibling.children[len(leftSibling.children)-1]}, child.children...)
// 		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
// 	}
// }

// func (tree *BPTree) borrowFromRight(parent *BPTreeNode, i int) {
// 	child := parent.children[i]
// 	rightSibling := parent.children[i+1]

// 	child.Keys = append(child.Keys, parent.Keys[i])
// 	child.Values = append(child.Values, parent.Values[i])
// 	parent.Keys[i] = rightSibling.Keys[0]
// 	parent.Values[i] = rightSibling.Values[0]
// 	rightSibling.Keys = rightSibling.Keys[1:]
// 	rightSibling.Values = rightSibling.Values[1:]

// 	if !child.isLeaf {
// 		child.children = append(child.children, rightSibling.children[0])
// 		rightSibling.children = rightSibling.children[1:]
// 	}
// }

// // print the tree
// func (tree *BPTree) PrintTree() {
// 	var printSubtree func(node *BPTreeNode, level int)
// 	printSubtree = func(node *BPTreeNode, level int) {
// 		if node == nil {
// 			return
// 		}

// 		fmt.Printf("Level %d: ", level)
// 		for i, key := range node.Keys {
// 			fmt.Printf("%d:%d ", key, node.Values[i])
// 		}
// 		fmt.Println()

// 		if !node.isLeaf {
// 			for _, child := range node.children {
// 				printSubtree(child, level+1)
// 			}
// 		}
// 	}

// 	printSubtree(tree.Root, 0)
// }
