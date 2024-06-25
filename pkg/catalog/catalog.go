//===----------------------------------------------------------------------===//
//
//                         🚄 ElenaDB ®
//
// catalog.go
//
// Identification: pkg/catalog/catalog.go
//
// Copyright (c) 2024
//
//===----------------------------------------------------------------------===//

package catalog

import (
	"fisi/elenadb/pkg/catalog/schema"
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"fisi/elenadb/pkg/storage/table"
)

type IndexType int

const (
	BPTreeIndex IndexType = iota
	HashTableIndex
)

/* Metadata about a Table */
type TableInfo struct {
	Schema schema.Schema
	Name   string
	Table  table.TableHeap
	OID    uint32
}

func NewTableInfo(schema schema.Schema, name string, table table.TableHeap, oid uint32) *TableInfo {
	return &TableInfo{
		Schema: schema,
		Name:   name,
		Table:  table,
		OID:    oid,
	}
}

/* Metadata about an Index */
type IndexInfo struct {
	KeySchema    schema.Schema
	Name         string
	Index        any
	OID          uint32
	TableName    string
	KeySize      uintptr
	IsPrimaryKey bool
	IndexType    IndexType
}

func NewIndexInfo(
	keySchema schema.Schema,
	name string,
	index any,
	indexOID uint32,
	tableName string,
	keySize uintptr,
	isPrimaryKey bool,
	indexType IndexType,
) *IndexInfo {
	return &IndexInfo{
		KeySchema:    keySchema,
		Name:         name,
		Index:        index,
		OID:          indexOID,
		TableName:    tableName,
		KeySize:      keySize,
		IsPrimaryKey: isPrimaryKey,
		IndexType:    indexType,
	}
}

type Catalog struct {
	DiskManager *storage_disk.DiskManager
}

func NewCatalog(diskManager *storage_disk.DiskManager) *Catalog {
	return &Catalog{
		DiskManager: diskManager,
	}
}
