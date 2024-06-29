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
	{ColumnName: "id", ColumnType: value.TypeInt32, IsUnique: true},
	{ColumnName: "type", ColumnType: value.TypeVarChar, StorageSize: 5},
	{ColumnName: "name", ColumnType: value.TypeVarChar, StorageSize: 255},
	{ColumnName: "file_id", ColumnType: value.TypeInt32, IsUnique: true},
	{ColumnName: "root", ColumnType: value.TypeInt32},
	{ColumnName: "sql", ColumnType: value.TypeVarChar, StorageSize: 2048},
})

// TODO(@pandadiestro): suppot varchar paramater
const ELENA_META_CREATE_SQL = `creame tabla elena_meta {
	id   int @unique,
	type char,
	name char,
	file_id int @unique,
	root int,
	sql  char,
} pe`

// " { type char(5), name char(255), table char(255), sql char(2048), } pe",
