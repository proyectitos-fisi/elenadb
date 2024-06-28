package column

import "fisi/elenadb/pkg/storage/table/value"

type Column struct {
	ColumnType  value.ValueType
	ColumnName  string
	StorageSize uint16
	IsUnique    bool
	IsNullable  bool
}

func CopyColumn(c Column) Column {
	return Column{
		ColumnType:  c.ColumnType,
		ColumnName:  c.ColumnName,
		StorageSize: c.StorageSize,
		IsUnique:    c.IsUnique,
		IsNullable:  c.IsNullable,
	}
}

func NewColumn(columnType value.ValueType, columnName string) Column {
	return Column{
		ColumnType: columnType,
		ColumnName: columnName,
	}
}

func NewSizedColumn(columnType value.ValueType, columnName string, storageSize uint16) Column {
	return Column{
		ColumnType:  columnType,
		ColumnName:  columnName,
		StorageSize: storageSize,
	}
}
