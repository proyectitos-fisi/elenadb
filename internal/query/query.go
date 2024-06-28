package query

import (
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/storage/table/value"
)

type QueryInstrType string

const (
	QueryCreate   QueryInstrType = "creame"
	QueryRetrieve QueryInstrType = "dame"
	QueryInsert   QueryInstrType = "mete"
	QueryDelete   QueryInstrType = "borra"
	QueryUpdate   QueryInstrType = "cambia"
)

type QueryField struct {
	Foreign     bool
	Name        string
	Type        value.ValueType
	Length      uint8
	Value       interface{}
	ForeignPath string
	Nullable    bool
	Annotations []string
}

type Query struct {
	QueryType      QueryInstrType
	QueryInstrName string
	QueryDbInstr   bool
	Fields         []QueryField
	Filter         *QueryFilter
}

func (q *Query) GetSchema() *schema.Schema {
	cols := make([]column.Column, 0, len(q.Fields))

	for _, f := range q.Fields {
		cols = append(cols, column.NewColumn(f.Type, f.Name))
	}
	return schema.NewSchema(cols)
}
