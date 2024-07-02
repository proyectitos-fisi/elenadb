package storage

import (
	"fisi/elenadb/pkg/buffer"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/storage/page"
	"fmt"
	// "fmt"
)

const numberNodes = 500

// BPTree es la estructura principal del B+ Tree
type BPTree struct {
	bufferPoolManager *buffer.BufferPoolManager
	fileId            common.FileID_t
	RootPageID        common.PageID_t
}

// NewBPTree crea una nueva instancia del B+ Tree
func NewBPTree(bufferPoolManager *buffer.BufferPoolManager, catalog common.FileID_t) *BPTree {
	rootPage := createEmptyPage(bufferPoolManager, catalog)

	return &BPTree{
		bufferPoolManager: bufferPoolManager,
		fileId:            catalog,
		RootPageID:        rootPage.PageID,
	}
}

// createEmptyPage crea una nueva página vacía y la retorna
func createEmptyPage(bufferPoolManager *buffer.BufferPoolManager, catalog common.FileID_t) *page.BTreePage {
	p := bufferPoolManager.NewPage(catalog)
	if p == nil {
		panic("No se pudo crear una nueva página")
	}
	bTreePage := page.NewBTreePage(p.PageId, page.LeafPage)
	data, err := bTreePage.Serialize()
	if err != nil {
		panic(fmt.Sprintf("Error serializing BTreePage: %v", err))
	}
	copy(p.Data, data)
	bufferPoolManager.WriteDataToPageAndPin(p.PageId, data)
	bufferPoolManager.UnpinPage(p.PageId, true)
	return bTreePage
}

// getPage obtiene una página del Buffer Pool Manager y la convierte a BTreePage
func (tree *BPTree) getPage(pageID common.PageID_t) *page.BTreePage {
	p := tree.bufferPoolManager.FetchPage(pageID)
	if p == nil {
		return nil
	}
	bTreePage, err := page.BTreePageFromRawData(p.Data)
	if err != nil {
		fmt.Printf("Error converting Page to BTreePage: %v", p.Data)
		panic(fmt.Sprintf("Error converting Page to BTreePage: %v", err))
	}
	tree.bufferPoolManager.UnpinPage(p.PageId, false)
	return bTreePage
}

// createPage crea una nueva página usando el Buffer Pool Manager y la convierte a BTreePage
func (tree *BPTree) createPage(pageType page.BTreePageType) *page.BTreePage {
	p := tree.bufferPoolManager.NewPage(tree.fileId)
	if p == nil {
		return nil
	}
	bTreePage := page.NewBTreePage(p.PageId, pageType)
	data, err := bTreePage.Serialize()
	if err != nil {
		panic(fmt.Sprintf("Error serializing BTreePage: %v", err))
	}
	copy(p.Data, data)
	return bTreePage
}

// Insert inserta una clave-valor en el B+ Tree
func (tree *BPTree) Insert(key int, value uint64) {
	if tree.RootPageID == common.InvalidPageID {
		newPage := tree.createPage(page.LeafPage)
		if newPage == nil {
			panic("(1) No se pudo crear una nueva página")
		}
		tree.RootPageID = newPage.PageID
		tree.initializeRootPage(newPage, key, value)
	} else {
		tree.insertIntoNode(tree.RootPageID, key, value)
	}
	// fmt.Printf("Terminó la inserción de %v\n\n", key)
}

// initializeRootPage inicializa la página raíz
func (tree *BPTree) initializeRootPage(rootPage *page.BTreePage, key int, value uint64) {
	rootPage.Keys = append(rootPage.Keys, key)
	rootPage.Values = append(rootPage.Values, value)
	data, _ := rootPage.Serialize()
	tree.bufferPoolManager.WriteDataToPageAndPin(rootPage.PageID, data)
	tree.bufferPoolManager.UnpinPage(rootPage.PageID, true)
	tree.bufferPoolManager.UnpinPage(rootPage.PageID, true)
}

// insertIntoNode inserta una clave-valor en un nodo dado
func (tree *BPTree) insertIntoNode(pageID common.PageID_t, key int, value uint64) {
	nodePage := tree.getPage(pageID)
	if nodePage == nil {
		panic("No se pudo obtener la página del nodo")
	}

	// fmt.Printf("Looking at the node: %v\n", nodePage)
	if nodePage.PageType == page.InternalPage {
		// fmt.Print("Looking for the correct node to append....\n")
		childIndex := tree.findIndex(nodePage.Keys, key)
		childPageID := nodePage.Children[childIndex]
		tree.bufferPoolManager.UnpinPage(nodePage.PageID, false)
		tree.insertIntoNode(childPageID, key, value)
		return
	}

	if len(nodePage.Keys) < numberNodes {
		tree.insertIntoLeaf(nodePage, key, value)
	} else {
		// fmt.Print("Overflow....\n")
		tree.splitNode(nodePage, key, value)
	}
}

// insertIntoLeaf inserta una clave-valor en una página hoja
func (tree *BPTree) insertIntoLeaf(nodePage *page.BTreePage, key int, value uint64) {
	index := tree.findIndex(nodePage.Keys, key)
	nodePage.Keys = append(nodePage.Keys[:index], append([]int{key}, nodePage.Keys[index:]...)...)
	nodePage.Values = append(nodePage.Values[:index], append([]uint64{value}, nodePage.Values[index:]...)...)
	data, err := nodePage.Serialize()
	if err != nil {
		panic(fmt.Sprintf("Error serializing BTreePage: %v", err))
	}
	// fmt.Printf("Inserting to leaf.......\n")
	// fmt.Printf("Node: %v\n", nodePage)
	tree.bufferPoolManager.WriteDataToPageAndPin(nodePage.PageID, data)
	tree.bufferPoolManager.UnpinPage(nodePage.PageID, true)
	tree.bufferPoolManager.UnpinPage(nodePage.PageID, true)
}

// splitNode maneja el desbordamiento de un nodo
func (tree *BPTree) splitNode(nodePage *page.BTreePage, key int, value uint64) {
	newPage := tree.createPage(nodePage.PageType)
	if newPage == nil {
		panic("(2) No se pudo crear una nueva página")
	}

	index := tree.findIndex(nodePage.Keys, key)
	allKeys := append(nodePage.Keys[:index], append([]int{key}, nodePage.Keys[index:]...)...)
	allValues := append(nodePage.Values[:index], append([]uint64{value}, nodePage.Values[index:]...)...)
	midIndex := len(allKeys) / 2

	newPage.Keys = append(newPage.Keys, allKeys[midIndex:]...)
	newPage.Values = append(newPage.Values, allValues[midIndex:]...)
	// newPage.Children = append(newPage.Children, nodePage.PageID)

	nodePage.Keys = allKeys[:midIndex]
	nodePage.Values = allValues[:midIndex]
	nodePage.Children = append(nodePage.Children, newPage.PageID)

	// fmt.Printf("Splitting.......\n")
	// fmt.Printf("newpage: %v - nodepage: %v\n", newPage, nodePage)

	data1, _ := nodePage.Serialize()
	data2, _ := newPage.Serialize()

	tree.updateParentNode(nodePage, newPage, newPage.Keys[0])

	tree.bufferPoolManager.WriteDataToPageAndPin(nodePage.PageID, data1)
	tree.bufferPoolManager.WriteDataToPageAndPin(newPage.PageID, data2)
	tree.bufferPoolManager.UnpinPage(nodePage.PageID, true)
	tree.bufferPoolManager.UnpinPage(newPage.PageID, true)
	tree.bufferPoolManager.UnpinPage(nodePage.PageID, true)
	tree.bufferPoolManager.UnpinPage(newPage.PageID, true)
}

// splitInternal divide el parent y ahonda el arbol un nivel más
func (tree *BPTree) splitInternal(oldNode *page.BTreePage) {
	newInternalPage := tree.createPage(page.InternalPage)
	if newInternalPage == nil {
		panic("Failed to create a new internal page")
	}

	midIndex := len(oldNode.Keys) / 2

	// Promote the key at the middle index
	promotedKey := oldNode.Keys[midIndex]

	// Populate the new internal page with keys and children from the old node
	newInternalPage.Keys = append(newInternalPage.Keys, oldNode.Keys[midIndex+1:]...)
	newInternalPage.Children = append(newInternalPage.Children, oldNode.Children[midIndex+1:]...)

	// Update the old node to keep keys and children up to but not including the middle index
	oldNode.Keys = oldNode.Keys[:midIndex]
	oldNode.Children = oldNode.Children[:midIndex+1]

	// fmt.Printf("Splitting internal node.......\n")
	// fmt.Printf("newInternalPage: %v\n", newInternalPage)
	// fmt.Printf("oldNode: %v\n", oldNode)

	// Serialize and write data to pages
	data1, _ := oldNode.Serialize()
	data2, _ := newInternalPage.Serialize()

	tree.bufferPoolManager.WriteDataToPageAndPin(oldNode.PageID, data1)
	tree.bufferPoolManager.WriteDataToPageAndPin(newInternalPage.PageID, data2)

	tree.bufferPoolManager.UnpinPage(oldNode.PageID, true)
	tree.bufferPoolManager.UnpinPage(newInternalPage.PageID, true)
	tree.bufferPoolManager.UnpinPage(oldNode.PageID, true)
	tree.bufferPoolManager.UnpinPage(newInternalPage.PageID, true)

	// Update the parent node after splitting
	tree.updateParentNode(oldNode, newInternalPage, promotedKey)
}

// updateParentNode actualiza el nodo padre después de una división
func (tree *BPTree) updateParentNode(oldNode *page.BTreePage, newNode *page.BTreePage, promotedKey int) {
	// promotedKey := newNode.Keys[0]
	// fmt.Printf("PROMOTED KEY: %v", promotedKey)
	if oldNode.PageID == tree.RootPageID {
		newRootPage := tree.createPage(page.InternalPage)
		if newRootPage == nil {
			panic("(3) No se pudo crear una nueva página raíz")
		}

		newRootPage.Keys = append(newRootPage.Keys, promotedKey)
		// newRootPage.Values = append(newRootPage.Values, oldNode.Values...)
		newRootPage.Children = append(newRootPage.Children, oldNode.PageID, newNode.PageID)

		// fmt.Printf("TESTING THIS: %v", len(newRootPage.Keys))
		if len(newRootPage.Keys) > numberNodes {
			tree.splitInternal(newRootPage)
		}

		// fmt.Printf("newrootpage: %v\n", newRootPage)

		tree.RootPageID = newRootPage.PageID
		data, err := newRootPage.Serialize()
		if err != nil {
			panic(fmt.Sprintf("Error serializing BTreePage: %v", err))
		}
		tree.bufferPoolManager.WriteDataToPageAndPin(newRootPage.PageID, data)
		tree.bufferPoolManager.UnpinPage(newRootPage.PageID, true)
		// tree.bufferPoolManager.UnpinPage(newRootPage.PageID, true)
	} else {
		parentPageID := tree.findParent(oldNode.PageID)
		parentPage := tree.getPage(parentPageID)
		// fmt.Printf("IDK why is this working:")
		index := tree.findIndex(parentPage.Keys, promotedKey)

		parentPage.Keys = append(parentPage.Keys[:index], append([]int{promotedKey}, parentPage.Keys[index:]...)...)
		parentPage.Children = append(parentPage.Children[:index+1], append([]common.PageID_t{newNode.PageID}, parentPage.Children[index+1:]...)...)

		// fmt.Printf("TESTING THIS IN A PARENT NOT A ROOT: %v", len(parentPage.Keys))
		if len(parentPage.Keys) > numberNodes {
			tree.splitInternal(parentPage)
		}

		data, err := parentPage.Serialize()
		if err != nil {
			panic(fmt.Sprintf("Error serializing BTreePage: %v", err))
		}
		// fmt.Printf("newpage: %v %v\n", parentPage.Keys, parentPage.Children)

		// JEJEJEJEJEJEJJEEJJEJEJEJEJEJEJJEJEJEJEJEJEJEJEJEJJEJEJEJEJEJEJEJEJEJJEJEJEJEJEJEJEJJEJEJEJEJEJJEJEJEJEJJEJEJE
		if len(parentPage.Keys) > numberNodes {
			tree.splitNode(parentPage, parentPage.Keys[len(parentPage.Keys)/2], 0) // No value needed for internal split
		}

		tree.bufferPoolManager.WriteDataToPageAndPin(parentPage.PageID, data)
		tree.bufferPoolManager.UnpinPage(parentPage.PageID, true)
		tree.bufferPoolManager.UnpinPage(parentPage.PageID, true)
	}
}

// findParent encuentra el ID de la página padre de un nodo dado
func (tree *BPTree) findParent(childPageID common.PageID_t) common.PageID_t {
	return tree.searchParent(tree.RootPageID, childPageID)
}

// searchParent busca recursivamente el nodo padre de un nodo dado
func (tree *BPTree) searchParent(currentPageID, targetPageID common.PageID_t) common.PageID_t {
	currentPage := tree.getPage(currentPageID)
	if currentPage == nil {
		return common.InvalidPageID
	}

	if currentPage.PageType == page.LeafPage {
		return common.InvalidPageID
	}

	for _, childID := range currentPage.Children {
		if childID == targetPageID {
			return currentPageID
		}
		childPage := tree.getPage(childID)
		if childPage != nil && childPage.PageType == page.InternalPage {
			parentID := tree.searchParent(childID, targetPageID)
			if parentID != common.InvalidPageID {
				return parentID
			}
		}
	}
	return common.InvalidPageID
}

// Search busca una clave en el B+ Tree
func (tree *BPTree) Search(key int) (uint64, bool) {
	return tree.searchNode(tree.RootPageID, key)
}

// searchNode busca una clave en un nodo específico del árbol B+
func (tree *BPTree) searchNode(pageID common.PageID_t, key int) (uint64, bool) {
	nodePage := tree.getPage(pageID)
	if nodePage == nil {
		return 0, false
	}

	// Iterar sobre las claves del nodo para encontrar el índice adecuado
	index := 0
	for ; index < len(nodePage.Keys); index++ {
		if key <= nodePage.Keys[index] {
			break
		}
	}

	// fmt.Printf("Looking in the node %v at index %v\n\n", nodePage, index)

	// Verificar si estamos en una hoja y si la clave está presente
	if nodePage.PageType == page.LeafPage {
		// fmt.Printf("index: %v, nodePageKeys: %v\n", index, nodePage.Keys)
		if index < len(nodePage.Keys) && nodePage.Keys[index] == key {
			return nodePage.Values[index], true
		}
		return 0, false
	}

	// Si la clave está en una página interna (nodo interno), debemos ajustar el índice
	if index < len(nodePage.Keys) && nodePage.Keys[index] == key {
		// La clave está en el nodo interno, pero necesitamos encontrar su posición real en la página hoja
		index++ // Avanzamos al siguiente índice para ir al hijo correcto
	}

	// Recursivamente buscar en el hijo correspondiente
	childPageID := nodePage.Children[index]
	tree.bufferPoolManager.UnpinPage(pageID, false)
	return tree.searchNode(childPageID, key)
}

// findIndex encuentra el índice donde debería estar la clave en un slice ordenado de claves
func (tree *BPTree) findIndex(keys []int, key int) int {
	// Búsqueda binaria
	low, high := 0, len(keys)
	for low < high {
		mid := low + (high-low)/2
		if keys[mid] < key {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

// PrintTree imprime el árbol B+ a través de un recorrido BFS
func (tree *BPTree) PrintTree() {
	// fmt.Println("Printing B+ Tree:")

	if tree.RootPageID == common.InvalidPageID {
		// fmt.Println("Empty tree")
		return
	}

	queue := []common.PageID_t{tree.RootPageID}
	levelSizes := []int{1}

	for len(queue) > 0 {
		currLevelSize := levelSizes[0]
		levelSizes = levelSizes[1:]

		var nextLevel []common.PageID_t
		for i := 0; i < currLevelSize; i++ {
			pageID := queue[0]
			queue = queue[1:]

			nodePage := tree.getPage(pageID)
			if nodePage == nil {
				continue
			}

			// fmt.Printf("PageID: %d, Keys: %v, Values: %v, Ref: %v\n", nodePage.PageID, nodePage.Keys, nodePage.Values, nodePage.Children)

			if nodePage.PageType == page.InternalPage {
				nextLevel = append(nextLevel, nodePage.Children...)
			}

			tree.bufferPoolManager.UnpinPage(nodePage.PageID, false)
		}

		if len(nextLevel) > 0 {
			queue = append(queue, nextLevel...)
			levelSizes = append(levelSizes, len(nextLevel))
		}
	}
}

// RangeSearch busca un rango de claves en el B+ Tree
func (tree *BPTree) RangeSearch(startKey int, endKey int, currentPageID common.PageID_t) ([]int, []uint64) {
	var keys []int
	var values []uint64
	found := false

	// Find the starting node
	// currentPageID := tree.RootPageID
	for {
		nodePage := tree.getPage(currentPageID)
		if nodePage == nil {
			fmt.Printf("No hay page, %v", currentPageID)
			return nil, nil
		}

		if nodePage.PageType == page.LeafPage {
			// Add all keys and values in range
			for i, key := range nodePage.Keys {
				if key >= startKey && key < endKey {
					// fmt.Printf("Appending keys and values..... %v - %v\n", key, nodePage.Values[i])
					keys = append(keys, key)
					values = append(values, nodePage.Values[i])
				} else if key == endKey {
					keys = append(keys, key)
					values = append(values, nodePage.Values[i])
					found = true
					break
				}
			}

			if found {
				// fmt.Printf("FOUND IS TRUE\n")
				tree.bufferPoolManager.UnpinPage(nodePage.PageID, false)
				break
			}
			// Move to the next leaf node
			if len(nodePage.Children) != 0 {
				// fmt.Printf("Going to next leaf node.... %v\n", nodePage.Children[0])
				currentPageID = nodePage.Children[0]
				tree.bufferPoolManager.UnpinPage(nodePage.PageID, false)
			} else {
				tree.bufferPoolManager.UnpinPage(nodePage.PageID, false)
				break
			}
		} else {
			// Find the appropriate child node
			// fmt.Printf("Looking for the appropiate child node....\n")
			index := 0
			for index < len(nodePage.Keys) && startKey > nodePage.Keys[index] {
				index++
			}
			if index < len(nodePage.Keys) && startKey <= nodePage.Keys[index] {
				// fmt.Printf("Looking in: %v\n", nodePage.Children[index])
				currentPageID = nodePage.Children[index]
			} else {
				// fmt.Printf("Looking in: %v\n", nodePage.Children[len(nodePage.Children)-1])
				currentPageID = nodePage.Children[len(nodePage.Children)-1]
			}
			tree.bufferPoolManager.UnpinPage(nodePage.PageID, false)
		}
	}

	return keys, values
}
