//===----------------------------------------------------------------------===//
//
//                         🚄 ElenaDB ®
//
// meta.go
//
// Identification: pkg/meta/meta.go
//
// Copyright (c) 2024
//
//===----------------------------------------------------------------------===//

package meta

import (
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/storage/table/value"
)

const ELENA_META_TABLE_NAME = "elena_meta"
const ELENA_META_TABLE_FILE = "elena_meta.table"

var ElenaMetaSchema = schema.NewSchema([]column.Column{
	{ColumnName: "file_id", ColumnType: value.TypeInt32, IsUnique: true, IsIdentity: true},
	{ColumnName: "type", ColumnType: value.TypeVarChar, StorageSize: 5},
	{ColumnName: "name", ColumnType: value.TypeVarChar, StorageSize: 255},
	{ColumnName: "root", ColumnType: value.TypeInt32},
	{ColumnName: "sql", ColumnType: value.TypeVarChar, StorageSize: 255}, // FIXME should be bigger
})

const ELENA_META_CREATE_SQL = `creame tabla elena_meta {
	file_id int       @id,
	type    char(5),
	name    char(255) @unique,
	root    int,
	sql     char(255),
} pe`

// dame { rid } de elena_meta pe
// The RID column is a ghost column, hidden by default.
// The RID has the format (file_id,actual_page_id,slot_number), i.e. (4,1,1).
const ELENA_RID_GHOST_COLUMN_NAME = "rid"
const ELENA_RID_GHOST_COLUMN_LEN = 12
