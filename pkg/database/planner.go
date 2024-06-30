package database

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/meta"
)

func SelectPlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	tableMetadata := db.Catalog.GetTableMetadata(query.QueryInstrName)

	// TODO: query for available indexes
	// index := db.Catalog.IndexMetadata(query.QueryInstrName)
	// if index != nil {
	// 	return
	// }

	return &ProjectionPlanNode{
		PlanNodeBase: PlanNodeBase{
			Type:     PlanNodeTypeProject,
			Database: db,
			Children: []PlanNode{
				&SeqScanPlanNode{
					PlanNodeBase: PlanNodeBase{
						Type:     PlanNodeTypeSeqScan,
						Children: nil,
						Database: db,
					},
					Table:         query.QueryInstrName,
					Query:         query,
					TableMetadata: tableMetadata,
					Cursor:        NewPagesCursorFromParts(tableMetadata.FileID, 0, 0),
				},
			},
		},
		ProjectionQuery: query,
		TableMetadata:   tableMetadata,
	}, nil
}
func InsertPlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	tableMetadata := db.Catalog.GetTableMetadata(query.QueryInstrName)
	if tableMetadata == nil {
		return nil, TableDoesNotExistError{table: query.QueryInstrName}
	}

	return &MetePlanNode{
		PlanNodeBase: PlanNodeBase{
			Type:     PlanNodeTypeInsert,
			Children: nil,
			Database: db,
		},
		Table:         query.QueryInstrName,
		Query:         query,
		TableMetadata: tableMetadata,
		Inserted:      false,
	}, nil
}
func UpdatePlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	return nil, NonImplementedPlanError{planName: "cambia"}
}
func DeletePlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	return nil, NonImplementedPlanError{planName: "borra"}
}
func CreatePlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	tableMetadata := db.Catalog.GetTableMetadata(query.QueryInstrName)

	if tableMetadata != nil {
		if tableMetadata.Name == meta.ELENA_META_TABLE_NAME {
			// Since meta_table will "always" exists, tableMetadata is never null, so we perform
			// a strict check to see if the meta_table actually exists. (on disk)
			if db.HasMetaTable() {
				return nil, TableAlreadyExistsError{table: query.QueryInstrName}
			}
		} else {
			return nil, TableAlreadyExistsError{table: query.QueryInstrName}
		}
	}

	return &CreamePlanNode{
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

type TableDoesNotExistError struct {
	table string
}

func (e TableDoesNotExistError) Error() string {
	return "Table does not exist: " + e.table
}

type TableAlreadyExistsError struct {
	table string
}

func (e TableAlreadyExistsError) Error() string {
	return "Table already exists: " + e.table
}

func isTableExistsError(err error) bool {
	_, ok := err.(TableAlreadyExistsError)
	return ok
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
