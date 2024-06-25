package schema

import (
	"fisi/elenadb/pkg/catalog/column"
	catalog "fisi/elenadb/pkg/catalog/column"
	"fmt"
	"strings"
)

type Schema struct {
	columns          []column.Column
	uninlinedColumns []column.Column
	AllInlined       bool
	Size             uint16
}

func NewSchema(columns []column.Column) *Schema {
	allInlined := true
	currentOffset := uint16(0)
	thisColumns := make([]column.Column, len(columns))
	thisUninlinedColumns := make([]column.Column, 0)

	for _, c := range columns {
		newCol := column.CopyColumn(c)

		if !c.ColumnType.IsInlinedType() {
			allInlined = false
			thisUninlinedColumns = append(thisUninlinedColumns, newCol)
		}
		newCol.Offset = currentOffset

		currentOffset += c.ColumnType.TypeSize()
		thisColumns = append(thisColumns, newCol)
	}

	return &Schema{
		AllInlined:       allInlined,
		Size:             currentOffset,
		columns:          columns,
		uninlinedColumns: thisUninlinedColumns,
	}
}

func (s *Schema) GetColumns() []catalog.Column {
	return s.columns
}

func (s *Schema) GetColumnCount() int {
	return len(s.columns)
}

func (s *Schema) GetColumn(col_idx int) catalog.Column {
	return s.columns[col_idx]
}

func (s *Schema) PrintTableDivisor() {
	bdivider := strings.Builder{}

	for _, column := range s.columns {
		bdivider.WriteString("+")
		bdivider.WriteString(strings.Repeat("-", len(column.ColumnName)+8))
		bdivider.WriteString("+")
	}
	divider := bdivider.String()

	fmt.Println(divider)
}

func (s *Schema) PrintAsTableHeader() {
	bdivider := strings.Builder{}

	for _, column := range s.columns {
		bdivider.WriteString("+")
		bdivider.WriteString(strings.Repeat("-", len(column.ColumnName)+8))
		bdivider.WriteString("+")
	}
	divider := bdivider.String()

	fmt.Println(divider)
	fmt.Print("| ")

	for _, column := range s.columns {
		fmt.Print(column.ColumnName)
		fmt.Print(strings.Repeat(" ", 8-1))
	}
	fmt.Println("|")
	fmt.Println(divider)
}
