package database

import (
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/storage/table/value"
)

const ELENA_META_TABLE_NAME = "elena_meta"
const ELENA_META_TABLE_FILE = "elena_meta.table"

var ElenaMetaSchema = schema.NewSchema([]column.Column{
	{ColumnName: "type", ColumnType: value.TypeVarChar, StorageSize: 5},
	{ColumnName: "name", ColumnType: value.TypeVarChar, StorageSize: 255},
	{ColumnName: "table", ColumnType: value.TypeVarChar, StorageSize: 255},
	{ColumnName: "sql", ColumnType: value.TypeVarChar, StorageSize: 2048},
})
