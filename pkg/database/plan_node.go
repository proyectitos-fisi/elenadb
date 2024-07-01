package database

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/catalog"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/meta"
	"fisi/elenadb/pkg/storage/page"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/storage/table/value"
	"fmt"
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

type IndexScanPlanNode struct {
	PlanNodeBase
	Table string
	Index string
}

// =========== "dame" ===========

// Sequential Scan on table
type SeqScanPlanNode struct {
	PlanNodeBase
	Table         string
	Query         *query.Query
	TableMetadata *catalog.TableMetadata
	Cursor        *PagesCursor
}

func (plan *SeqScanPlanNode) Next() *tuple.Tuple {
	// FIXME: hacer fetchPage 10 veces crashea el programa

	for {
		pageToScan := plan.Database.bufferPool.FetchPage(plan.Cursor.PageId)
		if pageToScan == nil {
			return nil
		}
		defer plan.Database.bufferPool.UnpinPage(plan.Cursor.PageId, false)
		slottedPage := page.NewSlottedPageFromRawPage(pageToScan)
		for i := plan.Cursor.SlotNum; uint16(i) < slottedPage.GetNSlots(); i++ {
			// plan.Database.log.Debug("iterating over slots (%d/%d)", i, slottedPage.GetNSlots())
			t := slottedPage.ReadTuple(&plan.TableMetadata.Schema, i)
			plan.Cursor.NextSlot()
			if t == nil {
				// deleted tuple
				continue
			}
			return t
		}

		// We finished scanning the page, let's move to the next one
		plan.Cursor.NextPage()
	}
}

func (s *SeqScanPlanNode) Schema() *schema.Schema {
	return &s.TableMetadata.Schema
}

func (s *SeqScanPlanNode) ToString() string {
	return "SeqScanPlanNode(" + s.Table + ")"
}

// =========== projection ===========

type ProjectionPlanNode struct {
	PlanNodeBase
	ProjectionQuery *query.Query
	TableMetadata   *catalog.TableMetadata
}

func (p *ProjectionPlanNode) Next() *tuple.Tuple {
	for _, child := range p.Children {
		for {
			// For each child until exhausted (generally we only have one child)
			tupleToProject := child.Next()
			if tupleToProject == nil {
				break
			}

			values := make([]value.Value, 0, len(p.ProjectionQuery.Fields))
			for _, field := range p.ProjectionQuery.Fields {
				for idx, col := range child.Schema().GetColumns() {
					if schema.ExtractColumnName(field.Name) == col.ColumnName {
						values = append(values, tupleToProject.Values[idx])
						break
					}
				}
			}
			return tuple.NewFromValues(values)
		}
	}
	return nil
}

func (p *ProjectionPlanNode) Schema() *schema.Schema {
	return p.ProjectionQuery.GetSchema()
}

func (p *ProjectionPlanNode) ToString() string {
	formattedFields := strings.Builder{}

	for _, f := range p.ProjectionQuery.Fields {
		formattedFields.WriteString(f.Name)
		formattedFields.WriteString(": ")
		formattedFields.WriteString(f.Type.AsString())
		formattedFields.WriteString(", ")
	}

	return fmt.Sprintf("ProjectionPlanNode{ %s }", formattedFields.String())
}

// =========== "creame" ===========

type CreamePlanNode struct {
	PlanNodeBase
	Table   string
	Query   *query.Query
	Created bool
}

func (plan *CreamePlanNode) Next() *tuple.Tuple {
	if plan.Created {
		return nil
	}
	// we create an empty table file!
	os.Create(plan.Database.DbPath + plan.Table + ".table")

	queryText := plan.Query.AsQueryText()

	// This is the metadata of the table
	tuples, _, _, _, err := plan.Database.ExecuteThisBaby(
		fmt.Sprintf(
			"mete { type: \"table\", name: \"%s\", root: 0, sql: \"%s\" } en %s pe",
			plan.Table, queryText, meta.ELENA_META_TABLE_NAME,
		),
	)
	if err != nil {
		panic(err)
	}
	// update catalog that a new table was created
	result := <-tuples
	fileId := result.Values[0].AsInt32()
	plan.Database.Catalog.RegisterTableMetadata(plan.Table, &catalog.TableMetadata{
		Name:      plan.Table,
		Schema:    *plan.Query.GetSchema(),
		FileID:    common.FileID_t(fileId),
		SqlCreate: queryText,
	})
	plan.Created = true
	return nil
}

func (c *CreamePlanNode) Schema() *schema.Schema {
	// Creame does not return
	return schema.EmptySchema()
}

func (c *CreamePlanNode) ToString() string {
	fields := strings.Builder{}

	for _, f := range c.Query.Fields {
		fields.WriteString(f.Name)
		fields.WriteString(": ")
		fields.WriteString(f.Type.AsString())
		fields.WriteString(", ")
	}

	return "CreamePlanNode(" + c.Table + "){ " + fields.String() + "}"
}

// ============== "mete" ==============

type MetePlanNode struct {
	PlanNodeBase
	Table string
	// So we can get the values the user is passing
	Query *query.Query
	// So we can know how to insert the tuple
	TableMetadata *catalog.TableMetadata
	Inserted      bool
}

func (plan *MetePlanNode) Next() *tuple.Tuple {
	if plan.Inserted {
		return nil
	}
	nextId := int32(0)

	fileId := plan.TableMetadata.FileID
	// Calculates the tuple size from the query fields
	tupleSize := uint16(0)
	for idx, _ := range plan.TableMetadata.Schema.GetColumns() {
		tupleSize += plan.Query.Fields[idx].AsTupleValueNillable().SizeOnDisk()
	}

	// FIXME: adquire lock!!!
	pageToWrite := plan.Database.bufferPool.FetchLastPage(fileId)
	var slottedPage *page.SlottedPage
	if pageToWrite == nil {
		// file is empty. this page is zeroed
		pageToWrite = plan.Database.bufferPool.NewPage(fileId)
		// plan.Database.log.Debug("About to write into page %s", pageToWrite.PageId.ToString())
		slottedPage = page.NewEmptySlottedPage(pageToWrite)
	} else {
		// file exists and it's a slotted page so we parse it
		slottedPage = page.NewSlottedPageFromRawPage(pageToWrite)
		nextId = int32(slottedPage.Header.LastInsertedId) + 1
		if slottedPage.Header.FreeSpace < tupleSize {
			// We need to create a new page
			nextId = int32(slottedPage.Header.LastInsertedId) + 1
			pageToWrite = plan.Database.bufferPool.NewPage(fileId)
			slottedPage = page.NewEmptySlottedPage(pageToWrite)
		}
	}

	// We need to create a tuple, so we iterate over the query fields

	// ASSERT: at this point, binder should have resolved the query to match the table schema
	values := make([]value.Value, 0, len(plan.Query.Fields))

	for idx, col := range plan.TableMetadata.Schema.GetColumns() {
		// Identity columns need to be populated first (they are autoincremental)
		if col.IsIdentity {
			// We assume the last slot contains the last id
			values = append(values, *value.NewInt32Value(nextId))
		} else {
			// Otherwise, we just append the value
			values = append(values, *plan.Query.Fields[idx].AsTupleValue())
		}
	}

	tupleToInsert := tuple.NewFromValues(values)

	err := slottedPage.AppendTuple(tupleToInsert)
	slottedPage.SetLastInsertedId(nextId)

	if err != nil {
		panic(err)
	}

	// Write the page back to disk
	plan.Database.bufferPool.UnpinPage(pageToWrite.PageId, true)

	plan.Database.bufferPool.FlushPage(pageToWrite.PageId) // FIXME: don't flush
	plan.Inserted = true

	if len(plan.Query.Returning) == 0 {
		return nil
	}

	mappedValues := make([]value.Value, len(plan.Query.Returning))

	for idx, field := range plan.Query.Returning {
		for i, col := range plan.TableMetadata.Schema.GetColumns() {
			if schema.ExtractColumnName(field) == col.ColumnName {
				mappedValues[idx] = tupleToInsert.Values[i]
				break
			}
		}
	}

	return tuple.NewFromValues(mappedValues)
}

func (plan *MetePlanNode) Schema() *schema.Schema {
	schm := schema.EmptySchema()

	for _, field := range plan.Query.Returning {
		for _, col := range plan.TableMetadata.Schema.GetColumns() {
			if schema.ExtractColumnName(field) == col.ColumnName {
				schm.AppendColumn(col)
				break
			}
		}
	}

	return schm
}

func (i *MetePlanNode) ToString() string {
	return "InsertPlanNode(" + i.Table + ")"
}

// Static assertions for PlanNodeBase implementors.
var _ PlanNode = (*SeqScanPlanNode)(nil)
var _ PlanNode = (*CreamePlanNode)(nil)
var _ PlanNode = (*MetePlanNode)(nil)
var _ PlanNode = (*ProjectionPlanNode)(nil)
