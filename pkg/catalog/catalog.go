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
	"fisi/elenadb/pkg/common"
)

type IndexType string

const (
	BPTreeIndex    IndexType = "BPTreeIndex"
	HashTableIndex IndexType = "HashTableIndex"
)

/* Metadata about a Table */
type TableMetadata struct {
	Name string
	// SQL Create statement that created this table
	SqlCreate string // columns
	Schema    schema.Schema
}

func NewTableInfo(name string, oid uint32) *TableMetadata {
	return &TableMetadata{}
}

/* Metadata about an Index */
type IndexMetadata struct {
	Schema       schema.Schema
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
) *IndexMetadata {
	return &IndexMetadata{
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
	// Casos de uso:
	// file_id -> filename
	// table_name -> TableMetadata
	// index_name -> IndexMetadata
	FileIdToFilenameMap map[common.FileID_t]string
	TableMetadataMap    map[string]*TableMetadata
	IndexMetadataMap    map[string]*IndexMetadata
}

func NewCatalog( /* toOODOOOOOO */ ) *Catalog {
	return &Catalog{}
}

func (c *Catalog) IndexMetadata(table string) *IndexMetadata {
	return c.IndexMetadataMap[table]
}

func (c *Catalog) FilenameFromFileId(fileId common.FileID_t) string {
	return c.FileIdToFilenameMap[fileId]
}

func (c *Catalog) GetTableMetadata(table string) *TableMetadata {
	return c.TableMetadataMap[table]

	// if table == database.ELENA_META_TABLE_NAME {
	// 	return &TableMetadata{
	// 		Name:      database.ELENA_META_TABLE_NAME,
	// 		SqlCreate: database.ELENA_META_CREATE_SQL,
	// 		Schema:    *database.ElenaMetaSchema,
	// 	}
	// }

	// result, _, _, err := c.Db.ExecuteThisBaby(
	// 	"dame { table_name, sql } de elena_meta donde { table_name = " + table + " } pe",
	// )

	// if err != nil {
	// 	panic("Unable to query elena_meta: " + err.Error())
	// }

	// tuple := <-result

	// if nil != <-result {
	// 	panic("Multiple rows returned for table " + table)
	// }

	// if tuple == nil {
	// 	return nil
	// }
}
