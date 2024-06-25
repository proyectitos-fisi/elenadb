package plan

import "fisi/elenadb/internal/query"

func MakeSelectPlan(*query.Query) (*PlanNodeBase, error) {
	return nil, NonImplementedPlanError{planName: "dame"}
}
func MakeInsertPlan(*query.Query) (*PlanNodeBase, error) {
	return nil, NonImplementedPlanError{planName: "mete"}
}
func MakeUpdatePlan(*query.Query) (*PlanNodeBase, error) {
	return nil, NonImplementedPlanError{planName: "cambia"}
}
func MakeDeletePlan(*query.Query) (*PlanNodeBase, error) {
	return nil, NonImplementedPlanError{planName: "borra"}
}
func MakeCreatePlan(query *query.Query) (PlanNode, error) {
	return &CreatePlanNode{
		PlanNodeBase: PlanNodeBase{
			Type:     PlanNodeTypeCreate,
			Children: nil,
		},
		Table:   query.QueryInstrName,
		Query:   query,
		created: false,
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
