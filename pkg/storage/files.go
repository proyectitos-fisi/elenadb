package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

const PageSize = 4096 // 4KB pages

type Page struct {
	PageID          int
	NumTuples       int
	FreeSpaceOffset int
	Tuples          []Tuple
}

type Tuple struct {
	Key   int
	Value int
}

func ReadPage(file *os.File, pageID int) (*Page, map[int]int, error) {
	offset := int64(pageID * PageSize)
	buf := make([]byte, PageSize)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, nil, err
	}

	page := &Page{
		PageID:          pageID,
		NumTuples:       int(binary.LittleEndian.Uint32(buf[4:8])),
		FreeSpaceOffset: int(binary.LittleEndian.Uint16(buf[8:10])),
		Tuples:          []Tuple{},
	}

	index := make(map[int]int)

	pos := 10
	for i := 0; i < page.NumTuples; i++ {
		key := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		value := int(binary.LittleEndian.Uint32(buf[pos+4 : pos+8]))
		page.Tuples = append(page.Tuples, Tuple{Key: key, Value: value})
		index[key] = i
		pos += 8
	}

	return page, index, nil
}

func WritePage(file *os.File, page *Page) error {
	offset := int64(page.PageID * PageSize)
	buf := make([]byte, PageSize)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(page.PageID))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(page.NumTuples))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(page.FreeSpaceOffset))

	pos := 10
	for _, tuple := range page.Tuples {
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(tuple.Key))
		binary.LittleEndian.PutUint32(buf[pos+4:pos+8], uint32(tuple.Value))
		pos += 8
	}

	_, err := file.WriteAt(buf, offset)
	return err
}

func Testing() {
	// Create a new B+Tree with minimum degree 2
	tree := NewBPTree(4)

	// Create or open the file to store pages
	file, err := os.OpenFile("pages.dat", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// tree.Insert(23, 1)
	// tree.Insert(42, 1)
	// tree.Insert(2312, 1)
	// tree.Insert(232, 1)
	// PrintTree(tree.Root, 0)
	// tree.Delete(23)
	// PrintTree(tree.Root, 0)
	// Insert some key-value pairs into the B+Tree
	tree.InsertKeyValue(file, 50, 1000)
	tree.InsertKeyValue(file, 161, 1000)
	tree.InsertKeyValue(file, 166, 1000)
	tree.InsertKeyValue(file, 265, 1000)
	tree.InsertKeyValue(file, 261, 1000)
	tree.InsertKeyValue(file, 25, 1000)
	tree.InsertKeyValue(file, 42, 1000)
	tree.InsertKeyValue(file, 12, 1000)
	tree.InsertKeyValue(file, 2235, 1000)

	page, index, err := ReadPage(file, 0)
	if err != nil {
		fmt.Println("Error reading the file:", err)
		return
	}

	// Save B+Tree to file
	err = tree.SaveToFile("btree.dat")
	if err != nil {
		fmt.Println("Error saving B+Tree:", err)
		return
	}

	// Load B+Tree from file
	loadedTree, err := LoadBPTreeFromFile("btree.dat")
	if err != nil {
		fmt.Println("Error loading B+Tree:", err)
		return
	}

	fmt.Printf("Page: %+v\n", page)
	fmt.Printf("Index: %+v\n", index)
	fmt.Println("Loaded B+Tree:")
	// PrintTree(loadedTree.Root, 0)
	loadedTree.PrintTree()
}
