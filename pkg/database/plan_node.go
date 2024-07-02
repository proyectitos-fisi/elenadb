package database

import (
	"container/heap"
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/catalog"
	"fisi/elenadb/pkg/catalog/column"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/meta"
	storage "fisi/elenadb/pkg/storage/index"
	"fisi/elenadb/pkg/storage/page"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/storage/table/value"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type PlanNode interface {
	// FLAG_ALGORITMO: Volcano model (iterator model) for query processing
	Next() (*tuple.Tuple, error)
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

// FLAG_ESTRUCTURA: tree (PlanNode y sus implementaciones(SeqScanPlanNode, FilterPlanNode, etc.))
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
	Query         *query.Query
	TableMetadata *catalog.TableMetadata
	Cursor        *PagesCursor
	CurrentPage   *page.Page
}

// FLAG_ALGORITMO: recorrido secuencial?? greedy??

func (plan *SeqScanPlanNode) Next() (*tuple.Tuple, error) {
	for {
		if plan.CurrentPage == nil || plan.CurrentPage.PageId != plan.Cursor.PageId {
			plan.CurrentPage = plan.Database.bufferPool.FetchPage(plan.Cursor.PageId)
		}

		if plan.CurrentPage == nil {
			plan.Database.bufferPool.UnpinPage(plan.Cursor.PageId, false)
			return nil, nil
		}
		slottedPage := page.NewSlottedPageFromRawPage(plan.CurrentPage)
		for i := plan.Cursor.SlotNum; uint16(i) < slottedPage.GetNSlots(); i++ {
			t := slottedPage.ReadTuple(&plan.TableMetadata.Schema, i)
			plan.Cursor.NextSlot()
			if t == nil {
				// deleted tuple
				continue
			}
			// SeqScan allows us to have the RID column in the format (file_id,page_id,slot)
			t.Values = append(
				t.Values,
				*value.NewVarCharValue(
					fmt.Sprintf(
						"(%d,%d,%d)",
						plan.TableMetadata.FileID, plan.Cursor.PageId.GetActualPageId(), i,
					), meta.ELENA_RID_GHOST_COLUMN_LEN,
				),
			)
			return t, nil
		}

		// We finished scanning the page, let's move to the next one
		plan.Database.bufferPool.UnpinPage(plan.Cursor.PageId, false)
		plan.Cursor.NextPage()
	}
}

func (s *SeqScanPlanNode) Schema() *schema.Schema {
	copiedSchema := s.TableMetadata.Schema
	// We append the hidden RID column here (See meta.go)
	copiedSchema.AppendColumn(column.Column{
		ColumnName:  meta.ELENA_RID_GHOST_COLUMN_NAME,
		ColumnType:  value.TypeVarChar,
		StorageSize: meta.ELENA_RID_GHOST_COLUMN_LEN,
	})
	return &copiedSchema
}

func (s *SeqScanPlanNode) ToString() string {
	formattedFields := strings.Builder{}
	fields := s.TableMetadata.Schema.GetColumns()
	numFields := len(fields)

	for i, f := range fields {
		formattedFields.WriteString("\t")
		formattedFields.WriteString(f.ColumnName)
		formattedFields.WriteString(":")
		formattedFields.WriteString(strings.ToUpper(string(f.ColumnType)))

		if i < numFields-1 {
			formattedFields.WriteString(",\n")
		}
	}

	return fmt.Sprintf("SeqScanPlanNode { table=%s } | (\n    %s \n    )\n", s.TableMetadata.Name, formattedFields.String())
}

// ========== ordenado por ==========

// FLAG_ESTRUCTURA: priority queue (heap)
type TuplesHeap struct {
	tuples   []*tuple.Tuple
	byColIdx int
	colType  value.ValueType
	asc      bool
}

func (h *TuplesHeap) Len() int { return len(h.tuples) }
func (h *TuplesHeap) Less(i, j int) bool {
	switch h.colType {
	case value.TypeInt32:
		if h.asc {
			return h.tuples[i].Values[h.byColIdx].AsInt32() < h.tuples[j].Values[h.byColIdx].AsInt32()
		} else {
			return h.tuples[i].Values[h.byColIdx].AsInt32() > h.tuples[j].Values[h.byColIdx].AsInt32()
		}
	case value.TypeFloat32:
		if h.asc {
			return h.tuples[i].Values[h.byColIdx].AsFloat32() < h.tuples[j].Values[h.byColIdx].AsFloat32()
		} else {
			return h.tuples[i].Values[h.byColIdx].AsFloat32() > h.tuples[j].Values[h.byColIdx].AsFloat32()
		}
	case value.TypeVarChar:
		if h.asc {
			return h.tuples[i].Values[h.byColIdx].AsVarchar() < h.tuples[j].Values[h.byColIdx].AsVarchar()
		} else {
			return h.tuples[i].Values[h.byColIdx].AsVarchar() > h.tuples[j].Values[h.byColIdx].AsVarchar()
		}
	case value.TypeBoolean:
		if h.asc {
			return h.tuples[i].Values[h.byColIdx].AsBoolean() && !h.tuples[j].Values[h.byColIdx].AsBoolean()
		} else {
			return !h.tuples[i].Values[h.byColIdx].AsBoolean() && h.tuples[j].Values[h.byColIdx].AsBoolean()
		}
	default:
		panic("unhandled type")
	}
}
func (h *TuplesHeap) Swap(i, j int) { h.tuples[i], h.tuples[j] = h.tuples[j], h.tuples[i] }

func (h *TuplesHeap) Push(x interface{}) {
	h.tuples = append(h.tuples, x.(*tuple.Tuple))
}

var _ heap.Interface = &TuplesHeap{}

func (h *TuplesHeap) Pop() interface{} {
	old := h.tuples
	n := len(old)
	x := old[n-1]
	h.tuples = old[0 : n-1]
	return x
}

type SortPlanNode struct {
	PlanNodeBase
	SortByQuery     *query.Query
	SortedColIdx    int
	TableMetadata   *catalog.TableMetadata
	IsOrderAsc      bool
	TuplesHeapAccum *TuplesHeap
	Sorted          bool
}

func (plan *SortPlanNode) Next() (*tuple.Tuple, error) {
	// We need to load all the tuples into memory, sort them and return them one by one

	if !plan.Sorted {
		plan.TuplesHeapAccum = &TuplesHeap{
			tuples:   make([]*tuple.Tuple, 0, 4),
			byColIdx: plan.SortedColIdx,
			colType:  plan.TableMetadata.Schema.GetColumns()[plan.SortedColIdx].ColumnType,
			asc:      plan.IsOrderAsc,
		}
		for {
			tupleToSort, err := plan.Children[0].Next()
			if err != nil {
				return nil, err
			}
			if tupleToSort == nil {
				break
			}
			heap.Push(plan.TuplesHeapAccum, tupleToSort)
		}
		plan.Sorted = true
	}
	// Now we have all the tuples in the heap, let's return them on-demand

	if plan.TuplesHeapAccum.Len() == 0 {
		return nil, nil
	}

	t := heap.Pop(plan.TuplesHeapAccum).(*tuple.Tuple)
	return t, nil
}

func (plan *SortPlanNode) Schema() *schema.Schema {
	return &plan.TableMetadata.Schema
}

func (plan *SortPlanNode) ToString() string {
	formattedFields := strings.Builder{}
	fields := plan.SortByQuery.Fields
	numFields := len(fields)

	for i, f := range fields {
		formattedFields.WriteString("    ")
		formattedFields.WriteString(f.Name)
		formattedFields.WriteString(":")
		formattedFields.WriteString(strings.ToUpper(f.Type.AsString()))

		if i < numFields-1 {
			formattedFields.WriteString(",\n")
		}
	}
	return fmt.Sprintf("SortPlanNode (\n%s\n)\n    %s", formattedFields.String(), plan.PlanNodeBase.Children[0].ToString())
}

// ============= filter =============

type FilterPlanNode struct {
	PlanNodeBase
	FilterQuery   *query.Query
	TableMetadata *catalog.TableMetadata
	IsBorra       bool // borra queries need the RID column
}

func (plan *FilterPlanNode) Next() (*tuple.Tuple, error) {
	for _, child := range plan.Children {
		for {
			// For each child until exhausted (generally we only have one child)
			tupleToFilter, err := child.Next()
			if err != nil {
				return nil, err
			}
			if tupleToFilter == nil {
				break
			}

			valuesMap := make(map[string]interface{})
			for idx, col := range child.Schema().GetColumns() {
				switch tupleToFilter.Values[idx].Type {
				case value.TypeInt32:
					valuesMap[col.ColumnName] = tupleToFilter.Values[idx].AsInt32()
				case value.TypeFloat32:
					valuesMap[col.ColumnName] = tupleToFilter.Values[idx].AsFloat32()
				case value.TypeBoolean:
					valuesMap[col.ColumnName] = tupleToFilter.Values[idx].AsBoolean()
				case value.TypeVarChar:
					valuesMap[col.ColumnName] = tupleToFilter.Values[idx].AsVarchar()
				default:
					panic("unhandled type")
				}
			}

			matches, err := plan.FilterQuery.Filter.Exec(valuesMap)
			if err != nil {
				return nil, err
			}

			if matches {
				return tupleToFilter, nil
			}
		}
	}
	return nil, nil
}

func (plan *FilterPlanNode) Schema() *schema.Schema {
	return plan.Children[0].Schema()
}

func (plan *FilterPlanNode) ToString() string {
	// TODO(@damaris): format nicely
	bulder := strings.Builder{}
	bulder.WriteString("FilterPlanNode {\n")
	for _, f := range plan.FilterQuery.Fields {
		bulder.WriteString("         ")
		bulder.WriteString(f.Name)
		bulder.WriteString(":")
		bulder.WriteString(strings.ToUpper(f.Type.AsString()))
		bulder.WriteString("\n")
	}
	bulder.WriteString("}\n")
	for _, c := range plan.Children {
		bulder.WriteString("    ")
		bulder.WriteString(c.ToString())
	}
	return bulder.String()

}

// =========== projection ===========

type ProjectionPlanNode struct {
	PlanNodeBase
	ProjectionQuery *query.Query
	TableMetadata   *catalog.TableMetadata
}

func (p *ProjectionPlanNode) Next() (*tuple.Tuple, error) {
	for _, child := range p.Children {
		for {
			// For each child until exhausted (generally we only have one child)
			tupleToProject, err := child.Next()
			if err != nil {
				return nil, err
			}
			if tupleToProject == nil {
				break
			}

			values := make([]value.Value, 0, len(p.ProjectionQuery.Fields))
			for _, field := range p.ProjectionQuery.Fields {
				for idx, col := range child.Schema().GetColumns() {
					if schema.ExtractColumnName(field.Name) == schema.ExtractColumnName(col.ColumnName) {
						values = append(values, tupleToProject.Values[idx])
						break
					}
				}
			}
			return tuple.NewFromValues(values), nil
		}
	}
	return nil, nil
}

func (p *ProjectionPlanNode) Schema() *schema.Schema {
	return p.ProjectionQuery.GetSchema()
}

func (p *ProjectionPlanNode) ToString() string {
	formattedFields := strings.Builder{}
	fields := p.ProjectionQuery.Fields
	numFields := len(fields)

	for i, f := range fields {
		formattedFields.WriteString("    ")
		formattedFields.WriteString(f.Name)
		formattedFields.WriteString(":")
		formattedFields.WriteString(strings.ToUpper(f.Type.AsString()))

		if i < numFields-1 {
			formattedFields.WriteString(",\n")
		}
	}

	/*columnum := strings.Builder{}
	for i := 0; i < numFields; i++ {
		columnum.WriteString("#0.")
		columnum.WriteString(strconv.Itoa(i))
		if i < numFields-1 {
			columnum.WriteString(", ")
		}
	}*/

	return fmt.Sprintf("ProjectionPlanNode (\n%s\n)\n    %s", formattedFields.String(), p.PlanNodeBase.Children[0].ToString())
}

// =========== "creame" ===========

type CreamePlanNode struct {
	PlanNodeBase
	Table   string
	Query   *query.Query
	Created bool
}

func (plan *CreamePlanNode) Next() (*tuple.Tuple, error) {
	if plan.Created {
		return nil, nil
	}
	// we create an empty table file!
	os.Create(plan.Database.DbPath + plan.Table + ".table")

	queryText := plan.Query.AsQueryText()

	// This is the metadata of the table
	tuples, _, _, _, err := plan.Database.ExecuteThisBaby(
		fmt.Sprintf(
			"mete { type: \"table\", name: \"%s\", root: 0, sql: \"%s\" } en %s retornando { file_id } pe",
			plan.Table, queryText, meta.ELENA_META_TABLE_NAME,
		), false)
	if err != nil {
		panic(err)
	}
	// update catalog that a new table was created
	result := <-tuples
	if result.IsError() {
		return nil, result.Error
	}

	fileId := result.Value.Values[0].AsInt32()
	plan.Database.Catalog.RegisterTableMetadata(plan.Table, &catalog.TableMetadata{
		Name:      plan.Table,
		Schema:    *plan.Query.GetSchema(),
		FileID:    common.FileID_t(fileId),
		SqlCreate: queryText,
	})
	plan.Database.Catalog.RegisterIndexMetadata(fmt.Sprintf("%s.id", plan.Table), &catalog.IndexMetadata{
		Name:      plan.Table,
		Root:      common.NewPageIdFromParts(common.FileID_t(fileId), common.APageID_t(0)),
		FileID:    common.FileID_t(fileId),
		SqlCreate: queryText,
	})

	bptree := storage.NewBPTree(plan.Database.bufferPool, common.FileID_t(fileId))
	plan.Database.ExecuteThisBaby(
		fmt.Sprintf(
			"mete { type: \"table\", name: \"%s\", root: \"%s\", sql: \"%s\" } en %s retornando { file_id } pe",
			plan.Table, "index", bptree.RootPageID.ToString(), meta.ELENA_META_TABLE_NAME,
		), false)

	plan.Created = true
	return nil, nil
}

func (c *CreamePlanNode) Schema() *schema.Schema {
	// Creame does not return
	return schema.EmptySchema()
}

func (c *CreamePlanNode) ToString() string {
	formattedFields := strings.Builder{}
	fields := c.Query.Fields
	numFields := len(fields)

	for i, f := range fields {
		formattedFields.WriteString("    ")
		formattedFields.WriteString(f.Name)
		formattedFields.WriteString(":")
		formattedFields.WriteString(strings.ToUpper(string(f.Type)))

		if i < numFields-1 {
			formattedFields.WriteString(",\n")
		}
	}

	return fmt.Sprintf("CreatePlanNode { table=%s } | (\n%s\n)\n", c.Table, formattedFields.String())
}

// ============== "mete" ==============

type MetePlanNode struct {
	PlanNodeBase
	//Table string
	// So we can get the values the user is passing
	Query *query.Query
	// So we can know how to insert the tuple
	TableMetadata *catalog.TableMetadata
	Inserted      bool
	NeedsScan     bool
}

func (plan *MetePlanNode) Next() (*tuple.Tuple, error) {
	if plan.Inserted {
		return nil, nil
	}

	if plan.NeedsScan {
		canBeInserted := true
		repeatedColumn := ""
		repeatedColumnValue := ""
		for {
			scannedTuple, err := plan.Children[0].Next()
			if err != nil {
				return nil, err
			}
			if scannedTuple == nil {
				break
			}
			// We need to check if the tuple can be inserted
		outerloop:
			for _, field := range plan.Query.Fields {
				for idx, col := range plan.TableMetadata.Schema.GetColumns() {
					if col.IsUnique && schema.ExtractColumnName(field.Name) == col.ColumnName {
						if field.IsEqualToValue(&scannedTuple.Values[idx]) {
							canBeInserted = false
							repeatedColumn = col.ColumnName
							repeatedColumnValue = scannedTuple.Values[idx].FormatAsString()
							break outerloop
						}
					}
				}
			}
		}
		if canBeInserted {
			plan.NeedsScan = false
		} else {
			return nil, fmt.Errorf("@unique column \"%s\" has a repeated value \"%s\"", repeatedColumn, repeatedColumnValue)
		}
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
		slottedPage = page.NewEmptySlottedPage(pageToWrite)
	} else {
		// file exists and it's a slotted page so we parse it
		slottedPage = page.NewSlottedPageFromRawPage(pageToWrite)
		nextId = int32(slottedPage.Header.LastInsertedId) + 1
		if slottedPage.HasSpaceForThisTupleSize(tupleSize) {
			// We need to create a new page
			plan.Database.bufferPool.UnpinPage(pageToWrite.PageId, false)
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
		} else if col.IsNullable && *&plan.Query.Fields[idx].Value == nil {
			values = append(values, *plan.Query.Fields[idx].AsNullRepresentation())
		} else {
			// Otherwise, we just append the value
			values = append(values, *plan.Query.Fields[idx].AsTupleValue())
		}
	}

	tupleToInsert := tuple.NewFromValues(values)

	err := slottedPage.AppendTuple(tupleToInsert)
	slottedPage.SetLastInsertedId(nextId)

	if err != nil {
		return nil, err
	}

	// Write the page back to disk
	plan.Database.bufferPool.UnpinPage(pageToWrite.PageId, true)

	// plan.Database.bufferPool.FlushPage(pageToWrite.PageId) // FIXME: don't flush
	plan.Inserted = true

	if len(plan.Query.Returning) == 0 {
		return tupleToInsert, nil
	}

	// Map the tuple to match the "retornando" fields
	mappedValues := make([]value.Value, len(plan.Query.Returning))

	for idx, field := range plan.Query.Returning {
		for i, col := range plan.TableMetadata.Schema.GetColumns() {
			if schema.ExtractColumnName(field) == col.ColumnName {
				mappedValues[idx] = tupleToInsert.Values[i]
				break
			}
		}
	}

	return tuple.NewFromValues(mappedValues), nil
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
	formattedFields := strings.Builder{}
	fields := i.Query.Fields
	numFields := len(fields)

	for i, f := range fields {
		if f.Value != nil {
			formattedFields.WriteString("    ")
			formattedFields.WriteString(f.Name)
			formattedFields.WriteString(":")
			formattedFields.WriteString(strings.ToUpper(string(f.Type)))

			if i < numFields-1 {
				formattedFields.WriteString(",\n")
			}
		}
	}

	return fmt.Sprintf("InsertPlanNode { table=%s } | (\n%s\n)\n", i.TableMetadata.Name, formattedFields.String())
}

// ======== "borra" ========
type DeletePlanNode struct {
	PlanNodeBase
	TableMetadata *catalog.TableMetadata
	Query         *query.Query
}

func (plan *DeletePlanNode) Next() (*tuple.Tuple, error) {
	child := plan.Children[0]
	// For each child until exhausted (generally we only have one child)
	tupleToDelete, err := child.Next()
	if err != nil {
		return nil, err
	}
	if tupleToDelete == nil {
		return nil, nil
	}

	// search for the RID column to get the page_id and slot to delete
	for idx, col := range child.Schema().GetColumns() {
		if col.ColumnName == meta.ELENA_RID_GHOST_COLUMN_NAME {
			rid := tupleToDelete.Values[idx].AsVarchar()
			// RID is in the format "(file_id,actual_page_id,slot)"
			ridParts := strings.Split(rid[1:len(rid)-1], ",")
			if len(ridParts) != 3 {
				return nil, fmt.Errorf("invalid RID format: %s", rid)
			}
			fileId, _ := strconv.Atoi(ridParts[0])
			aPageId, _ := strconv.Atoi(ridParts[1])
			tupleSlot, _ := strconv.Atoi(ridParts[2])

			pageId := common.NewPageIdFromParts(common.FileID_t(fileId), common.APageID_t(aPageId))

			rawPage := plan.Database.bufferPool.FetchPage(pageId)
			if rawPage == nil {
				return nil, fmt.Errorf("page %s not found", pageId)
			}

			slottedPage := page.NewSlottedPageFromRawPage(rawPage)
			slottedPage.DeleteTuple(common.SlotNumber_t(tupleSlot))
			plan.Database.bufferPool.UnpinPage(pageId, true)
			plan.Database.bufferPool.FlushPage(pageId) // FIXME: don't flush

			return tupleToDelete, nil
		}
	}
	return nil, nil
}

func (plan *DeletePlanNode) Schema() *schema.Schema {
	return schema.EmptySchema()
}

func (plan *DeletePlanNode) ToString() string {
	return "DeletePlanNode(" + plan.TableMetadata.Name + ")"
}

// Static assertions for PlanNodeBase implementors.
var _ PlanNode = (*SeqScanPlanNode)(nil)
var _ PlanNode = (*CreamePlanNode)(nil)
var _ PlanNode = (*MetePlanNode)(nil)
var _ PlanNode = (*ProjectionPlanNode)(nil)
var _ PlanNode = (*DeletePlanNode)(nil)
var _ PlanNode = (*FilterPlanNode)(nil)
var _ PlanNode = (*SortPlanNode)(nil)
