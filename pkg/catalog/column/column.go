package column

import "fisi/elenadb/pkg/storage/table/value"

type Column struct {
	ColumnType  value.ValueType
	ColumnName  string
	StorageSize uint8
	IsUnique    bool
	IsNullable  bool
	IsForeign   bool
	IsIdentity  bool
}

func CopyColumn(c Column) Column {
	return Column{
		ColumnType:  c.ColumnType,
		ColumnName:  c.ColumnName,
		StorageSize: c.StorageSize,
		IsUnique:    c.IsUnique,
		IsNullable:  c.IsNullable,
		IsForeign:   c.IsForeign,
		IsIdentity:  c.IsIdentity,
	}
}

func NewColumn(columnType value.ValueType, columnName string) Column {
	return Column{
		ColumnType: columnType,
		ColumnName: columnName,
	}
}

func NewSizedColumn(columnType value.ValueType, columnName string, storageSize uint8) Column {
	return Column{
		ColumnType:  columnType,
		ColumnName:  columnName,
		StorageSize: storageSize,
	}
}
