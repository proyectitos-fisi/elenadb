package storage

// Just a test with a simple btree in go
// This is similar in Rust, Struct and an implementation
// A node in a btree
type BPTreeNode struct {
	keys     []int         // slice of an int
	children []*BPTreeNode // childens
	isLeaf   bool
	parent   *BPTreeNode
	next     *BPTreeNode
}

func NewBPTreeNode(isLeaf bool) *BPTreeNode {
	return &BPTreeNode{
		keys:     []int{},
		children: []*BPTreeNode{},
		isLeaf:   isLeaf,
	}
}

type BPTree struct {
	root *BPTreeNode
	t    int // Minimum degree (defines the range for number of keys)
}

func NewBPTree(t int) *BPTree {
	root := NewBPTreeNode(true)
	return &BPTree{root: root, t: t}
}

// This is the insertion
func (tree *BPTree) Insert(key int) {
	root := tree.root
	if len(root.keys) == 2*tree.t-1 {
		newRoot := NewBPTreeNode(false)
		newRoot.children = append(newRoot.children, root)
		tree.splitChild(newRoot, 0)
		tree.root = newRoot
	}
	tree.insertNonFull(tree.root, key)
}

func (tree *BPTree) insertNonFull(node *BPTreeNode, key int) {
	i := len(node.keys) - 1
	if node.isLeaf {
		node.keys = append(node.keys, 0)
		for i >= 0 && key < node.keys[i] {
			node.keys[i+1] = node.keys[i]
			i--
		}
		node.keys[i+1] = key
	} else {
		for i >= 0 && key < node.keys[i] {
			i--
		}
		i++
		if len(node.children[i].keys) == 2*tree.t-1 {
			tree.splitChild(node, i)
			if key > node.keys[i] {
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

	parent.keys = append(parent.keys, 0)
	copy(parent.keys[i+1:], parent.keys[i:])
	parent.keys[i] = y.keys[t-1]

	z.keys = append(z.keys, y.keys[t:]...)
	y.keys = y.keys[:t-1]

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
	return tree.search(tree.root, key)
}

func (tree *BPTree) search(node *BPTreeNode, key int) (*BPTreeNode, int) {
	i := 0
	for i < len(node.keys) && key > node.keys[i] {
		i++
	}
	if i < len(node.keys) && key == node.keys[i] {
		return node, i
	}
	if node.isLeaf {
		return nil, -1
	}
	return tree.search(node.children[i], key)
}
