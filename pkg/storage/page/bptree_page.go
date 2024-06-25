package page

import (
	"bytes"
	"encoding/gob"
	"fisi/elenadb/pkg/common"
)

// BTreePageType define el tipo de página del B+ Tree
type BTreePageType int

const (
	InternalPage BTreePageType = iota
	LeafPage
)

// BTreePage representa una página en el B+ Tree
type BTreePage struct {
	PageID   common.PageID_t
	PageType BTreePageType
	Keys     []int
	Values   []int
	Children []common.PageID_t
}

// NewBTreePage crea una nueva página del B+ Tree
func NewBTreePage(pageID common.PageID_t, pageType BTreePageType) *BTreePage {
	return &BTreePage{
		PageID:   pageID,
		PageType: pageType,
		Keys:     []int{},
		Values:   []int{},
		Children: []common.PageID_t{},
	}
}

// Serialize serializa una BTreePage a un slice de bytes
func (p *BTreePage) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize deserializa un slice de bytes a una BTreePage
func Deserialize(data []byte) (*BTreePage, error) {
	var p BTreePage
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&p)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func BTreePageFromRawData(rawData []byte) (*BTreePage, error) {
	return Deserialize(rawData)
}
