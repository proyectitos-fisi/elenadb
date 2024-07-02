package database

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/meta"
	"fisi/elenadb/pkg/storage/table/value"
)

func SelectPlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	tableMetadata := db.Catalog.GetTableMetadata(query.QueryInstrName)

	// TODO: query for available indexes
	// index := db.Catalog.IndexMetadata(query.QueryInstrName)

	if tableMetadata == nil {
		return nil, TableDoesNotExistError{table: query.QueryInstrName}
	}

	var selectPlan PlanNode

	// FLAG_ ESTRUCTURA: tree
	selectPlan = &SeqScanPlanNode{
		PlanNodeBase: PlanNodeBase{
			Type:     PlanNodeTypeSeqScan,
			Children: nil,
			Database: db,
		},
		Table:         query.QueryInstrName,
		Query:         query,
		TableMetadata: tableMetadata,
		Cursor:        NewPagesCursorFromParts(tableMetadata.FileID, 0, 0),
		CurrentPage:   nil,
	}

	if query.Filter != nil {
		query.Filter.Resolver = func(columnName string) value.ValueType {
			cols := tableMetadata.Schema.GetColumns()
			for idx, _ := range cols {
				col := cols[idx]

				if col.ColumnName == columnName {
					return col.ColumnType
				}
			}
			return value.TypeInvalid
		}
		selectPlan = &FilterPlanNode{
			PlanNodeBase: PlanNodeBase{
				Type:     PlanNodeTypeFilter,
				Database: db,
				Children: []PlanNode{
					selectPlan,
				},
			},
			FilterQuery:   query,
			TableMetadata: tableMetadata,
		}
	}

	// FLAG_ALGORITMO: heap sort
	if query.OrderedBy != nil {
		sortedColIdx := -1
		for idx, col := range tableMetadata.Schema.GetColumns() {
			if col.ColumnName == *query.OrderedBy {
				sortedColIdx = idx
				break
			}
		}
		if sortedColIdx == -1 {
			return nil, ColumnNotFoundError{column: *query.OrderedBy, table: query.QueryInstrName}
		}

		selectPlan = &SortPlanNode{
			PlanNodeBase: PlanNodeBase{
				Type:     PlanNodeTypeSort,
				Database: db,
				Children: []PlanNode{
					selectPlan,
				},
			},
			SortByQuery:     query,
			TableMetadata:   tableMetadata,
			SortedColIdx:    sortedColIdx,
			IsOrderAsc:      query.IsAscending,
			TuplesHeapAccum: nil,
			Sorted:          false, // initially
		}
		// TODO: order asc?
	}

	// FLAG_ ESTRUCTURA: tree
	return &ProjectionPlanNode{
		PlanNodeBase: PlanNodeBase{
			Type:     PlanNodeTypeProject,
			Database: db,
			Children: []PlanNode{
				selectPlan,
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
		//Table:         query.QueryInstrName,
		Query:         query,
		TableMetadata: tableMetadata,
		Inserted:      false,
	}, nil
}
func UpdatePlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	return nil, NonImplementedPlanError{planName: "cambia"}
}
func DeletePlanBuilder(query *query.Query, db *ElenaDB) (PlanNode, error) {
	tableMetadata := db.Catalog.GetTableMetadata(query.QueryInstrName)
	if tableMetadata == nil {
		return nil, TableDoesNotExistError{table: query.QueryInstrName}
	}
	// FLAG_ESRUCTURA:  Tabla hash (implícita)
	query.Filter.Resolver = func(columnName string) value.ValueType {
		cols := tableMetadata.Schema.GetColumns()
		for idx, _ := range cols {
			col := cols[idx]

			if col.ColumnName == columnName {
				return col.ColumnType
			}
		}
		return value.TypeInvalid
	}

	if query.Filter != nil {
		query.Filter.Resolver = func(columnName string) value.ValueType {
			cols := tableMetadata.Schema.GetColumns()
			for idx, _ := range cols {
				col := cols[idx]

				if col.ColumnName == columnName {
					return col.ColumnType
				}
			}
			return value.TypeInvalid
		}

		return &DeletePlanNode{
			PlanNodeBase: PlanNodeBase{
				Type:     PlanNodeTypeProject,
				Database: db,
				Children: []PlanNode{
					&FilterPlanNode{
						PlanNodeBase: PlanNodeBase{
							Type:     PlanNodeTypeFilter,
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
									CurrentPage:   nil,
								},
							},
						},
						FilterQuery:   query,
						TableMetadata: tableMetadata,
					},
				},
			},
			Query:         query,
			TableMetadata: tableMetadata,
		}, nil
	}

	return &DeletePlanNode{
		PlanNodeBase: PlanNodeBase{
			Type: PlanNodeTypeDelete,
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
			Database: db,
		},
		TableMetadata: tableMetadata,
		Query:         query,
	}, nil
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
	case query.QueryErase: // borra
		return DeletePlanBuilder(inputQuery, db)
	case query.QueryUpdate: // cambia
		return UpdatePlanBuilder(inputQuery, db)
	default:
		return nil, UnknownPlanError{}
	}
}
