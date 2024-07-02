package schema

import (
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/storage/table/value"
	"fisi/elenadb/pkg/utils"
	"fmt"
	"strings"
)

type Schema struct {
	columns []column.Column
}

func NewSchema(columns []column.Column) *Schema {
	return &Schema{columns: columns}
}

func EmptySchema() *Schema {
	return &Schema{columns: []column.Column{}}
}

func (s *Schema) GetColumns() []column.Column {
	return s.columns
}

func (s *Schema) GetColumnCount() int {
	return len(s.columns)
}

func (s *Schema) IsEmpty() bool {
	return len(s.columns) == 0
}

func (s *Schema) GetColumn(col_idx int) column.Column {
	return s.columns[col_idx]
}

func (s *Schema) AppendColumn(col column.Column) {
	s.columns = append(s.columns, col)
}

// === Display functions ===

func GetMinimumSpacingForType(columnType value.ValueType) int {
	switch columnType {
	case value.TypeBoolean:
		return 4
	case value.TypeInt32:
		return 6
	case value.TypeFloat32:
		return 6
	case value.TypeVarChar:
		return 24
	default:
		return 5
		// panic("unreachable: getMinimumSpacingForType(" + string(columnType) + ")")
	}
}

func GetTableColSpacingFromColumn(column column.Column) int {
	if column.ColumnType == value.TypeVarChar {
		return utils.Max(
			utils.Min(
				GetMinimumSpacingForType(column.ColumnType),
				int(column.StorageSize),
			), len(ExtractColumnName(column.ColumnName)),
		)
	} else {
		return utils.Max(
			len(ExtractColumnName(column.ColumnName)),
			GetMinimumSpacingForType(column.ColumnType),
		)
	}
}

func (s *Schema) PrintTableDivisor() {
	bdivider := strings.Builder{}

	for _, column := range s.columns {
		spacing := GetTableColSpacingFromColumn(column)

		bdivider.WriteString("+")
		bdivider.WriteString(strings.Repeat("-", spacing+2))
	}
	bdivider.WriteString("+")
	divider := bdivider.String()

	fmt.Println(divider)
}

func (s *Schema) PrintAsTableHeader() {
	fmt.Println()
	s.PrintTableDivisor()
	for _, column := range s.columns {
		colName := ExtractColumnName(column.ColumnName)

		spacing := GetTableColSpacingFromColumn(column)

		fmt.Print("| ")
		fmt.Print(colName)
		fmt.Print(strings.Repeat(" ", spacing-len(colName)+1))
	}
	fmt.Println("|")
	s.PrintTableDivisor()
}

func ExtractColumnName(field string) string {
	splitted := strings.Split(field, ".")
	if len(splitted) == 1 {
		return splitted[0]
	}
	return splitted[1]
}

func SchemasAreEquivalent(s1 *Schema, s2 *Schema) bool {
	if s1.GetColumnCount() != s2.GetColumnCount() {
		return false
	}

	for i := 0; i < s1.GetColumnCount(); i++ {
		c1 := s1.GetColumn(i)
		c2 := s2.GetColumn(i)

		if c1.ColumnName != c2.ColumnName {
			return false
		}
		if c1.ColumnType != c2.ColumnType {
			return false
		}
		if c1.IsNullable != c2.IsNullable {
			return false
		}
		if c1.IsUnique != c2.IsUnique {
			return false
		}
		if c1.StorageSize != c2.StorageSize {
			return false
		}
	}

	return true
}
