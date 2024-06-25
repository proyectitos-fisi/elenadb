package database

import (
	"fisi/elenadb/internal/query"
)

func SelectPlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	return &SelectPlanNode{
		PlanNodeBase: PlanNodeBase{
			Type:     PlanNodeTypeSeqScan,
			Children: nil,
			Database: db,
		},
		Table: query.QueryInstrName,
		Query: query,
	}, nil
}
func InsertPlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	return nil, NonImplementedPlanError{planName: "mete"}
}
func UpdatePlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	return nil, NonImplementedPlanError{planName: "cambia"}
}
func DeletePlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	return nil, NonImplementedPlanError{planName: "borra"}
}
func CreatePlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	return &CreatePlanNode{
		PlanNodeBase: PlanNodeBase{
			Type:     PlanNodeTypeCreate,
			Children: nil,
			Database: db,
		},
		Table:   query.QueryInstrName,
		Query:   query,
		Created: false,
	}, nil
}

/* Plan errors */

// UnknownPlanError is returned when the planner does not recognize the query type.
type UnknownPlanError struct{}

func (u UnknownPlanError) Error() string {
	return "Unknown plan error"
}

type NonImplementedPlanError struct {
	planName string
}

func (n NonImplementedPlanError) Error() string {
	return "Non-implemented plan: " + n.planName
}

func OptimizeQueryPlan(inputPlan PlanNode) PlanNode {
	return inputPlan
}

func MakeQueryPlan(inputQuery *query.Query, db *ElenaDB) (PlanNode, error) {
	switch inputQuery.QueryType {
	case query.QueryCreate: // creame
		return CreatePlanBuilder(inputQuery, db)
	case query.QueryRetrieve: // dame
		return SelectPlanBuilder(inputQuery, db)
	case query.QueryInsert: // mete
		return InsertPlanBuilder(inputQuery, db)
	case query.QueryDelete: // borra
		return DeletePlanBuilder(inputQuery, db)
	case query.QueryUpdate: // cambia
		return UpdatePlanBuilder(inputQuery, db)
	default:
		return nil, UnknownPlanError{}
	}
}
