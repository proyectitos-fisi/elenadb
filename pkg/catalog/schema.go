package catalog

type Schema struct {
	columns          []Column
	uninlinedColumns []Column
	AllInlined       bool
	Length           int
}

func NewSchema(columns []Column) *Schema {
	allInlined := true
	currentOffset := 0
	thisColumns := make([]Column, len(columns))
	thisUninlinedColumns := make([]Column, 0)

	for _, c := range columns {
		newCol := CopyColumn(c)

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
		Length:           currentOffset,
		columns:          columns,
		uninlinedColumns: thisUninlinedColumns,
	}
}

func (s *Schema) GetColumns() []Column {
	return s.columns
}

func (s *Schema) GetColumnCount() int {
	return len(s.columns)
}

func (s *Schema) GetColumn(col_idx uint32) Column {
	return s.columns[col_idx]
}
