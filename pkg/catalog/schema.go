package catalog

type Schema struct {
	columns []Column
}

func (s *Schema) GetColumns() []Column {
	return s.columns
}

func (s *Schema) GetColumn(col_idx uint32) Column {
	return s.columns[col_idx]
}
