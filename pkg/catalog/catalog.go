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
	"fisi/elenadb/pkg/meta"
)

type IndexType string

const (
	BPTreeIndex    IndexType = "BPTreeIndex"
	HashTableIndex IndexType = "HashTableIndex"
)

/* Metadata about a Table */
type TableMetadata struct {
	Name   string
	FileID common.FileID_t
	// SQL Create statement that created this table
	SqlCreate string // columns
	Schema    schema.Schema
}

func NewTableInfo(name string, oid uint32) *TableMetadata {
	return &TableMetadata{}
}

/* Metadata about an Index */
type IndexMetadata struct {
	Name      string
	FileID    common.FileID_t
	Root      common.PageID_t
	SqlCreate string
}

func NewIndexInfo(
	name string,
	fileID common.FileID_t,
	root common.PageID_t,
	sqlCreate string,
) *IndexMetadata {
	return &IndexMetadata{
		Name:      name,
		FileID:    fileID,
		Root:      root,
		SqlCreate: sqlCreate,
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

// un catalog skeleton, vacío no más
func EmptyCatalog() *Catalog {
	return &Catalog{
		FileIdToFilenameMap: nil,
		TableMetadataMap:    nil,
		IndexMetadataMap:    nil,
	}
}

// un catalog que será llenado
func FillCatalog(
	fileIdToFilenameMap map[common.FileID_t]string,
	tableMetadataMap map[string]*TableMetadata,
	indexMetadataMap map[string]*IndexMetadata,
) *Catalog {
	return &Catalog{
		FileIdToFilenameMap: fileIdToFilenameMap,
		TableMetadataMap:    tableMetadataMap,
		IndexMetadataMap:    indexMetadataMap,
	}
}

func (c *Catalog) IndexMetadata(table string) *IndexMetadata {
	return c.IndexMetadataMap[table]
}

func (c *Catalog) FilenameFromFileId(fileId common.FileID_t) string {
	return c.FileIdToFilenameMap[fileId]
}

func (c *Catalog) GetTableMetadata(table string) *TableMetadata {
	if table == meta.ELENA_META_TABLE_NAME {
		return &TableMetadata{
			Name:      meta.ELENA_META_TABLE_NAME,
			SqlCreate: meta.ELENA_META_CREATE_SQL,
			Schema:    *meta.ElenaMetaSchema,
		}
	}
	return c.TableMetadataMap[table]
}

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
