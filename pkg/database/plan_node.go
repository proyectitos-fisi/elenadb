package database

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/storage/table/value"
	"os"
	"strings"
)

type PlanNode interface {
	Next() *tuple.Tuple
	Schema() *schema.Schema
	ToString() string
}

type PlanNodeType string

const (
	PlanNodeTypeSeqScan   PlanNodeType = "SeqScan"
	PlanNodeTypeIndexScan PlanNodeType = "IndexScan"
	PlanNodeTypeInsert    PlanNodeType = "Insert"
	PlanNodeTypeUpdate    PlanNodeType = "Update"
	PlanNodeTypeDelete    PlanNodeType = "Delete"
	PlanNodeTypeCreate    PlanNodeType = "Create"
	PlanNodeTypeProject   PlanNodeType = "Project"
	PlanNodeTypeFilter    PlanNodeType = "Filter"
	PlanNodeTypeJoin      PlanNodeType = "Join"
	PlanNodeTypeSort      PlanNodeType = "Sort"
	PlanNodeTypeLimit     PlanNodeType = "Limit"
	PlanNodeTypeGroupBy   PlanNodeType = "TopN"
)

type PlanNodeBase struct {
	Type     PlanNodeType
	Children []PlanNode
	Database *ElenaDB
}

// func (p *PlanNodeBase) Next() *tuple.Tuple {
// 	panic("call to PlanNodeBase.Next() from plan_node=" + string(p.Type))
// }

// func (p *PlanNodeBase) Schema() *schema.Schema {
// 	panic("call to PlanNodeBase.Schema() from plan_node=" + string(p.Type))
// }

// func (p *PlanNodeBase) Print() *schema.Schema {
// 	panic("call to PlanNodeBase.Print() from plan_node=" + string(p.Type))
// }

type SeqScanPlanNode struct {
	PlanNodeBase
	Table string
}

func (s *SeqScanPlanNode) Execute() {
}

type IndexScanPlanNode struct {
	PlanNodeBase
	Table string
	Index string
}

// =========== "dame" ===========
type SelectPlanNode struct {
	PlanNodeBase
	Table string
	Query *query.Query
}

func (s *SelectPlanNode) Next() *tuple.Tuple {
	return nil
}

func (s *SelectPlanNode) Schema() *schema.Schema {
	cols := make([]column.Column, 0)

	for _, f := range s.Query.Fields {
		cols = append(cols, column.NewColumn(f.Type, f.Name))
	}
	return schema.NewSchema(cols)
}

func (s *SelectPlanNode) ToString() string {
	return "SelectPlanNode(" + s.Table + ")"
}

// =========== "creame" ===========

type CreatePlanNode struct {
	PlanNodeBase
	Table   string
	Query   *query.Query
	Created bool
}

func (c *CreatePlanNode) Next() *tuple.Tuple {
	if c.Created {
		return nil
	}
	os.Create(c.Database.DbPath + c.Table + ".table")
	c.Database.ExecuteThisBaby("mete {} en elena_meta")

	c.Created = true
	val := value.NewValue(value.TypeBoolean, []byte{1})

	return tuple.NewFromValues([]value.Value{*val})
}

func (c *CreatePlanNode) Schema() *schema.Schema {
	col := column.NewColumn(value.TypeBoolean, "created")
	return schema.NewSchema([]column.Column{col})
}

func (c *CreatePlanNode) ToString() string {
	fields := strings.Builder{}

	for _, f := range c.Query.Fields {
		fields.WriteString(f.Name)
		fields.WriteString(": ")
		fields.WriteString(f.Type.AsString())
		fields.WriteString(", ")
	}

	return "CreatePlanNode(" + c.Table + "){ " + fields.String() + "}"
}

// Static assertions for PlanNodeBase implementors.
var _ PlanNode = (*SelectPlanNode)(nil)
var _ PlanNode = (*CreatePlanNode)(nil)
