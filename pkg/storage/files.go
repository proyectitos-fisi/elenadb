package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

const PageSize = 4096 // 4KB pages

type PageKV struct {
	PageID          int
	NumTuples       int
	FreeSpaceOffset int
	Tuples          []Tuple
}

type Tuple struct {
	Key   int
	Value int
}

func ReadPage(file *os.File, pageID int) (*PageKV, error) {
	offset := int64(pageID * PageSize)
	buf := make([]byte, PageSize)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	page := &PageKV{
		PageID:          pageID,
		NumTuples:       int(binary.LittleEndian.Uint32(buf[4:8])),
		FreeSpaceOffset: int(binary.LittleEndian.Uint16(buf[8:10])),
		Tuples:          []Tuple{},
	}

	pos := 10
	for i := 0; i < page.NumTuples; i++ {
		key := int(binary.LittleEndian.Uint32(buf[pos : pos+4]))
		value := int(binary.LittleEndian.Uint32(buf[pos+4 : pos+8]))
		page.Tuples = append(page.Tuples, Tuple{Key: key, Value: value})
		pos += 8
	}

	return page, nil
}

func WritePage(file *os.File, page *PageKV) error {
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
	tree := NewBPTree(2)

	// Create or open the file to store pages
	file, err := os.OpenFile("pages.dat", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	// Insert some key-value pairs into the B+Tree
	// tree.InsertKeyValue(file, 50, 1000)
	// tree.InsertKeyValue(file, 161, 1000)
	// tree.InsertKeyValue(file, 166, 1000)
	// tree.InsertKeyValue(file, 265, 1000)

	PrintTree(tree.Root, 0)

	page, err := ReadPage(file, 0)

	fmt.Printf("%+v\n", page)
	if page == nil {
		fmt.Println("Error reading the file:", err)
	}

	// Search for a key
	node, index := tree.Search(30)
	if node != nil {
		fmt.Printf("Found key 30 at node: %v at index: %d\n", node.Keys, index)
	} else {
		fmt.Println("Key 30 not found")
	}
}
