package schema

import (
	"fisi/elenadb/pkg/catalog/column"
	catalog "fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/storage/table/value"
	"fisi/elenadb/pkg/utils"
	"fmt"
	"strings"
)

type Schema struct {
	columns []column.Column
}

func NewSchema(columns []column.Column) *Schema {
	thisColumns := make([]column.Column, len(columns))

	for _, c := range columns {
		newCol := column.CopyColumn(c)
		thisColumns = append(thisColumns, newCol)
	}

	return &Schema{columns: columns}
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

// === Display functions ===

func GetMinimumSpacingForType(columnType value.ValueType) int {
	// TODO: parser is not returning types D:
	switch columnType {
	case value.TypeBoolean:
		return 4
	case value.TypeInt32:
		return 12
	case value.TypeFloat32:
		return 12
	case value.TypeVarChar:
		return 16
	default:
		panic("unreachable: getMinimumSpacingForType(" + string(columnType) + ")")
	}
}

func (s *Schema) PrintTableDivisor() {
	bdivider := strings.Builder{}

	for _, column := range s.columns {
		spacing := utils.Max(
			len(column.ColumnName),
			GetMinimumSpacingForType(column.ColumnType),
		)

		bdivider.WriteString("+")
		bdivider.WriteString(strings.Repeat("-", spacing+2))
		bdivider.WriteString("+")
	}
	divider := bdivider.String()

	fmt.Println(divider)
}

func (s *Schema) PrintAsTableHeader() {
	bdivider := strings.Builder{}

	for _, column := range s.columns {
		spacing := utils.Max(
			len(column.ColumnName),
			GetMinimumSpacingForType(column.ColumnType),
		)

		bdivider.WriteString("+")
		bdivider.WriteString(strings.Repeat("-", spacing+2))
	}
	bdivider.WriteString("+")
	divider := bdivider.String()

	fmt.Println(divider)

	for _, column := range s.columns {
		spacing := utils.Max(
			len(column.ColumnName),
			GetMinimumSpacingForType(column.ColumnType),
		)

		fmt.Print("| ")
		fmt.Print(column.ColumnName)
		fmt.Print(strings.Repeat(" ", spacing-len(column.ColumnName)))
	}
	fmt.Println(" |")
	fmt.Println(divider)
}
