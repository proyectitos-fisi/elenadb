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
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/meta"
	"fmt"
	"strings"
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
	TableMetadataMap map[string]*TableMetadata
	IndexMetadataMap map[string]*IndexMetadata
}

// un catalog skeleton, vacío no más
func EmptyCatalog() *Catalog {
	return &Catalog{
		TableMetadataMap: make(map[string]*TableMetadata),
		IndexMetadataMap: make(map[string]*IndexMetadata),
	}
}

// un catalog que será llenado
func FillCatalog(
	tableMetadataMap map[string]*TableMetadata,
	indexMetadataMap map[string]*IndexMetadata,
) *Catalog {
	return &Catalog{
		TableMetadataMap: tableMetadataMap,
		IndexMetadataMap: indexMetadataMap,
	}
}

func (c *Catalog) IndexMetadata(table string) *IndexMetadata {
	return c.IndexMetadataMap[table]
}

func (c *Catalog) FilenameFromFileId(fileId common.FileID_t) *string {
	if fileId == 0 {
		__ := meta.ELENA_META_TABLE_FILE
		return &__
	}

	for _, table := range c.TableMetadataMap {
		if table.FileID == fileId {
			__ := fmt.Sprintf("%s.table", table.Name)
			return &__
		}
	}
	for _, index := range c.IndexMetadataMap {
		if index.FileID == fileId {
			__ := fmt.Sprintf("%s.index", index.Name)
			return &__
		}
	}
	return nil
}

func (c *Catalog) RegisterTableMetadata(table string, metadata *TableMetadata) {
	c.TableMetadataMap[table] = metadata
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

// tableCol is a string in the format "table.column"
func (c *Catalog) getTableColumnMetadata(tableCol string) *column.Column {
	table_column := strings.Split(tableCol, ".")
	table := table_column[0]
	column := table_column[1]

	for _, col := range c.TableMetadataMap[table].Schema.GetColumns() {
		if col.ColumnName == column {
			return &col
		}
	}

	return nil
}
