package column

import "fisi/elenadb/pkg/storage/table/value"

type Column struct {
	ColumnType  value.ValueType
	ColumnName  string
	StorageSize uint16
	Offset      uint16
}

func CopyColumn(c Column) Column {
	return Column{
		ColumnType:  c.ColumnType,
		ColumnName:  c.ColumnName,
		StorageSize: c.StorageSize,
		Offset:      c.Offset,
	}
}

func NewColumn(columnType value.ValueType, columnName string) Column {
	return Column{
		ColumnType: columnType,
		ColumnName: columnName,
	}
}
