package plan

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/storage/table/value"
)

type PlanNode interface {
	Next() *tuple.Tuple
	Schema() *schema.Schema
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
}

func (p *PlanNodeBase) Next() *tuple.Tuple {
	panic("call to PlanNodeBase.Next() from plan_node=" + string(p.Type))
}

func (p *PlanNodeBase) Schema() *schema.Schema {
	panic("call to PlanNodeBase.Schema() from plan_node=" + string(p.Type))
}

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

// =========== "creame" ===========

type CreatePlanNode struct {
	PlanNodeBase
	Table   string
	Query   *query.Query
	created bool
}

func (c *CreatePlanNode) Next() *tuple.Tuple {
	if c.created {
		return nil
	}
	c.created = true
	val := value.NewValue(value.TypeBoolean, []byte{1})

	return tuple.NewFromValues([]value.Value{*val})
}

func (c *CreatePlanNode) Schema() *schema.Schema {
	col := column.NewColumn(value.TypeBoolean, "created")
	return schema.NewSchema([]column.Column{col})
}

// Static assertions for PlanNodeBase implementors.
var _ PlanNode = (*PlanNodeBase)(nil)
var _ PlanNode = (*CreatePlanNode)(nil)
