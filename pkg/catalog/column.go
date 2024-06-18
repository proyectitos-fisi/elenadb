package catalog

import "fisi/elenadb/pkg/storage/table/value"

type Column struct {
	ColumnType  value.TypeId
	ColumnName  string
	StorageSize int
	Offset      int
}

func CopyColumn(c Column) Column {
	return Column{
		ColumnType:  c.ColumnType,
		ColumnName:  c.ColumnName,
		StorageSize: c.StorageSize,
		Offset:      c.Offset,
	}
}

func NewColumn(columnType value.TypeId, columnName string) Column {
	return Column{
		ColumnType: columnType,
		ColumnName: columnName,
	}
}
