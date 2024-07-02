package query

import (
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/storage/table/value"
	"strconv"
	"strings"
)

type QueryInstrType string

const (
	QueryCreate   QueryInstrType = "creame"
	QueryRetrieve QueryInstrType = "dame"
	QueryInsert   QueryInstrType = "mete"
	QueryErase    QueryInstrType = "borra"
	QueryUpdate   QueryInstrType = "cambia"
)

type QueryFieldAnnotation string

const (
	AnnotationId     QueryFieldAnnotation = "id"
	AnnotationUnique QueryFieldAnnotation = "unique"
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
	Filter         *QueryFilter `json:"-"`
	Returning      []string
    OrderedBy      string
}

// WARNING: This function may lose information if your query is one of: ["meta", "borra", "cambia"]
// because it doesn't know the schema of the table or its constraints.
//
// However, this does perform as expected for "dame" and "creame" when they
// have all the necessary information. i.e. are properly binded in the sqlPipeline.
func (q *Query) GetSchema() *schema.Schema {
	// if q.QueryType != QueryCreate && q.QueryType != QueryRetrieve {
	// 	panic("unreachable: GetSchema() called on a query that doesn't have a schema: " + string(q.QueryType))
	// }

	cols := make([]column.Column, 0, len(q.Fields))

	for _, f := range q.Fields {
		cols = append(cols, column.Column{
			ColumnName:  f.Name,
			ColumnType:  f.Type,
			StorageSize: f.Length,
			IsUnique:    f.HasAnnotation(AnnotationUnique),
			IsNullable:  f.Nullable,
			IsForeign:   f.Foreign,
			IsIdentity:  f.HasAnnotation(AnnotationId),
		})
	}
	return schema.NewSchema(cols)
}

func (qf *QueryField) HasAnnotation(annotation QueryFieldAnnotation) bool {
	for _, a := range qf.Annotations {
		if a == string(annotation) {
			return true
		}
	}
	return false
}

func (qf *QueryField) AsTupleValue() *value.Value {
	if qf.Value == nil {
		// We assert this as nil because AsTupleValue should only be called in
		// queries that define a value for its fields, like "mete"
		panic("unreachable: AsTupleValue() called on nil value: " + qf.Name)
	}

	var newValue value.Value
	switch qf.Type {
	case value.TypeInt32:
		newValue = *value.NewInt32Value(qf.Value.(int32))
	case value.TypeFloat32:
		newValue = *value.NewFloat32Value(qf.Value.(float32))
	case value.TypeVarChar:
		// we strip the first and last character because they are quotes
		newValue = *value.NewVarCharValue(qf.Value.(string), int(qf.Length))
	case value.TypeBoolean:
		newValue = *value.NewBooleanValue(qf.Value.(bool))
	default:
		panic("unreachable: unknown type")
	}
	return &newValue
}

func (qf *QueryField) AsTupleValueNillable() *value.Value {
	var newValue value.Value
	switch qf.Type {
	case value.TypeInt32:
		if qf.Value == nil {
			newValue = *value.NewInt32Value(0)
		} else {
			newValue = *value.NewInt32Value(qf.Value.(int32))
		}
	case value.TypeFloat32:
		if qf.Value == nil {
			newValue = *value.NewFloat32Value(0)
		} else {
			newValue = *value.NewFloat32Value(qf.Value.(float32))
		}
	case value.TypeVarChar:
		if qf.Value == nil {
			newValue = *value.NewVarCharValue("", int(qf.Length))
		} else {
			// we strip the first and last character because they are quotes
			newValue = *value.NewVarCharValue(qf.Value.(string), int(qf.Length))
		}
	case value.TypeBoolean:
		if qf.Value == nil {
			newValue = *value.NewBooleanValue(false)
		} else {
			newValue = *value.NewBooleanValue(qf.Value.(bool))
		}
	default:
		panic("unreachable: unknown type")
	}
	return &newValue
}

func (qf *QueryField) AsString() string {
	builder := strings.Builder{}
	builder.WriteString(qf.Name)
	builder.WriteString(" ")

	switch qf.Type {
	case value.TypeBoolean:
		builder.WriteString("bool")
	case value.TypeInt32:
		builder.WriteString("int")
	case value.TypeFloat32:
		builder.WriteString("float")
	case value.TypeVarChar:
		builder.WriteString("char(")
		// uint8 to string
		conv := strconv.Itoa(int(qf.Length))
		builder.WriteString(conv)
		builder.WriteString(")")
	default:
		builder.WriteString("invalid")
	}

	for _, annotation := range qf.Annotations {
		builder.WriteString(" @")
		builder.WriteString(annotation)
	}
	return builder.String()
}

func (q *Query) AsQueryText() string {
	if q.QueryType != QueryCreate {
		panic("unreachable: AsQueryText() should be only used for 'creame' queries")
	}

	builder := strings.Builder{}
	builder.WriteString("creame tabla ")
	builder.WriteString(q.QueryInstrName)
	builder.WriteString(" { ")
	for _, f := range q.Fields {
		builder.WriteString(f.AsString())
		builder.WriteString(", ")
	}
	builder.WriteString("} pe")
	return builder.String()
}
