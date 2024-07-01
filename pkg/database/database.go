//===----------------------------------------------------------------------===//
//
//                         🚄 ElenaDB ®
//
// database.go
//
// Identification: pkg/database/database.go
//
// Copyright (c) 2024
//
//===----------------------------------------------------------------------===//

package database

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/buffer"
	"fisi/elenadb/pkg/catalog"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/meta"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/storage/table/value"
	"fisi/elenadb/pkg/utils"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

type ElenaDB struct {
	// Elena database directory
	DbPath string
	// Elena's buffer pool manager
	bufferPool *buffer.BufferPoolManager
	// Whether this instance created the database for the first time
	IsJustCreated bool
	Catalog       *catalog.Catalog
	log           *common.Logger
	NextQueryID   atomic.Uint32
}

// Creates the Elena Instance. Should be called only once per process.
// Long live to ELENA! WE LOVE ELENA! 🚄!!
func StartElenaBusiness(dbPath string) (*ElenaDB, error) {
	dbPath = utils.WithTrailingSlash(dbPath)
	common.GloablDbDir = dbPath

	ctlg := catalog.EmptyCatalog()
	bpm := buffer.NewBufferPoolManager(dbPath, common.BufferPoolSize, common.LRUKReplacerK, ctlg)

	elena := &ElenaDB{
		DbPath:        dbPath,
		bufferPool:    bpm,
		IsJustCreated: false,
		Catalog:       ctlg,
		log:           common.NewLogger('🚄'),
	}
	elena.log.Boot("\n🌫  ElenaDB just started")

	err := elena.CreateDatabaseIfNotExists()
	if err != nil {
		return nil, err
	}

	err = elena.CreateMetaTableIfNotExists()
	if err != nil {
		return nil, err
	}

	err = elena.PopulateCatalog()
	if err != nil {
		return nil, err
	}

	return elena, nil
}

// Populate catalog
func (elena *ElenaDB) PopulateCatalog() error {
	elena.log.Boot("populating catalog")
	tableMetadataMap := make(map[string]*catalog.TableMetadata)
	indexMetadataMap := make(map[string]*catalog.IndexMetadata)

	tuples, _, _, _, err := elena.ExecuteThisBaby("dame todo de elena_meta pe")
	if err != nil {
		return err
	}

	for tuple := range tuples {
		fileId := tuple.Values[0].AsInt32()
		fileType := tuple.Values[1].AsVarchar()
		name := tuple.Values[2].AsVarchar()
		root := tuple.Values[3].AsInt32()
		sql := tuple.Values[4].AsVarchar()

		if fileType == "table" {
			parser := query.NewParser()
			tableSchema, err := parser.Parse(strings.NewReader(sql))
			if err != nil {
				return err
			}
			tableMetadataMap[name] = &catalog.TableMetadata{
				Name:      name,
				FileID:    common.FileID_t(fileId),
				SqlCreate: sql,
				Schema:    *tableSchema[0].GetSchema(),
			}
		} else if fileType == "index" {
			indexMetadataMap[name] = &catalog.IndexMetadata{
				Name:      name,
				FileID:    common.FileID_t(fileId),
				Root:      common.PageID_t(root),
				SqlCreate: sql,
			}
		}
	}

	elena.Catalog.TableMetadataMap = tableMetadataMap
	elena.Catalog.IndexMetadataMap = indexMetadataMap
	return nil
}

// Executes a SQL query. The steps are as follows:
// - (sqlPipeline) Parse the query
// - (sqlPipeline) Analize/bind the query
// - (sqlPipeline) Optimize the query
// - Make a plan based on the query
// - Optimize the plan
// - Execute the plan, fetching the tuples one by one
func (db *ElenaDB) ExecuteThisBaby(input string) (chan *tuple.Tuple, *schema.Schema, *query.Query, PlanNode, error) {
	queryId := db.NextQueryId()
	db.log.Info("\nquery(%d): %s", queryId, input)

	parsedQuery, err := db.sqlPipeline(input)
	if err != nil {
		db.log.Error("query(%d): %s", queryId, err.Error())
		return nil, nil, nil, nil, err
	}

	nodePlan, err := MakeQueryPlan(parsedQuery, db)

	if err != nil {
		db.log.Error("query(%d): %s", queryId, err.Error())
		return nil, nil, nil, nil, err
	}
	nodePlan = OptimizeQueryPlan(nodePlan)

	count := 0
	tuples := make(chan *tuple.Tuple)

	go func() {
		for {
			tuple := nodePlan.Next() // executor
			if tuple == nil {
				break
			}
			count++
			tuples <- tuple
		}
		db.log.Info("query(%d): -> %d tuples", queryId, count)
		close(tuples)
	}()

	return tuples, nodePlan.Schema(), parsedQuery, nodePlan, nil
}

func (e *ElenaDB) CreateDatabaseIfNotExists() error {
	if utils.DirExists(e.DbPath) {
		return nil
	}

	err := os.Mkdir(e.DbPath, os.ModePerm)
	if err != nil {
		return err
	}

	e.IsJustCreated = true

	return nil
}

func (db *ElenaDB) CreateMetaTableIfNotExists() error {
	if db.HasMetaTable() {
		db.log.Boot("found meta table 'elena_meta.table'")
		return nil
	}

	db.log.Boot("creating meta table 'elena_meta.table'")
	result, _, _, _, err := db.ExecuteThisBaby(meta.ELENA_META_CREATE_SQL)
	if err != nil {
		return err
	}

	<-result
	return nil
}

// Parses, analizes, optimizes and prepares (in-place) the query for execution.
func (db *ElenaDB) sqlPipeline(input string) (*query.Query, error) {
	parser := query.NewParser()
	statements, err := parser.Parse(strings.NewReader(input))
	if err != nil {
		return nil, err
	}
	parsedQuery := &statements[0]

	// dame
	if parsedQuery.QueryType == query.QueryRetrieve {
		tableMetaData := db.Catalog.GetTableMetadata(parsedQuery.QueryInstrName)
		if tableMetaData == nil {
			return nil, TableDoesNotExistError{table: parsedQuery.QueryInstrName}
		}

		resolvedFields := make([]query.QueryField, 0)

		for _, field := range parsedQuery.Fields {
			// Resolve "todo" (*)
			if field.Name == "todo" {
				for _, col := range tableMetaData.Schema.GetColumns() {
					resolvedFields = append(resolvedFields, query.QueryField{
						Foreign:     col.IsForeign,
						Name:        fmt.Sprintf("%s.%s", tableMetaData.Name, col.ColumnName),
						Type:        col.ColumnType,
						Length:      uint8(col.StorageSize),
						Value:       nil,
						ForeignPath: "",
						Nullable:    col.IsNullable,
						Annotations: []string{},
					})
				}
				// Resolve other fields
			} else {
				exists := false
				for _, col := range tableMetaData.Schema.GetColumns() {
					if col.ColumnName == field.Name {
						exists = true
						resolvedFields = append(resolvedFields, query.QueryField{
							Foreign:     col.IsForeign,
							Name:        fmt.Sprintf("%s.%s", tableMetaData.Name, col.ColumnName),
							Type:        col.ColumnType,
							Length:      uint8(col.StorageSize),
							Value:       nil,
							ForeignPath: "",
							Nullable:    col.IsNullable,
							Annotations: []string{},
						})
					}
				}
				if !exists {
					return nil, fmt.Errorf("Column \"%s\" does not exist in  table \"%s\"", field.Name, parsedQuery.QueryInstrName)
				}
			}
		}

		// tableMetaData.Schema
		parsedQuery.Fields = resolvedFields
	}

	// mete
	if parsedQuery.QueryType == query.QueryInsert {
		tableMetaData := db.Catalog.GetTableMetadata(parsedQuery.QueryInstrName)
		if tableMetaData == nil {
			return nil, TableDoesNotExistError{table: parsedQuery.QueryInstrName}
		}
		resolvedFields := make([]query.QueryField, 0)

		// First we iterate over the table columns since "mete" need to define ALL
		// the columns or the row, and they MUST be ordered
		for _, col := range tableMetaData.Schema.GetColumns() {
			exists := false
			for _, field := range parsedQuery.Fields {
				if field.Name == col.ColumnName {
					if col.IsIdentity {
						return nil, fmt.Errorf("Column \"%s\" is @id and cannot be inserted", col.ColumnName)
					}

					// Parser parses all values as string, so we need to resolve them to their respective types
					resolvedValue, err := resolveAnyValueFromType(col.ColumnType, field.Value)
					if err != nil {
						return nil, err
					}

					resolvedFields = append(resolvedFields, query.QueryField{
						Foreign:     col.IsForeign,
						Name:        fmt.Sprintf("%s.%s", tableMetaData.Name, col.ColumnName),
						Type:        col.ColumnType,
						Length:      uint8(col.StorageSize),
						Value:       resolvedValue,
						ForeignPath: "",
						Nullable:    col.IsNullable,
						Annotations: []string{},
					})
					exists = true
				}
			}
			// If the user didn't pass the column, we need to check if it's nullable
			if !exists {
				// Identity columns can't be passed on queries so that's ok
				if !col.IsNullable && !col.IsIdentity {
					return nil, fmt.Errorf("Non nullable column \"%s\" is missing", col.ColumnName)
				}
				// If is nullable we insert it ourselves as NULL
				resolvedFields = append(resolvedFields, query.QueryField{
					Foreign:     col.IsForeign,
					Name:        fmt.Sprintf("%s.%s", tableMetaData.Name, col.ColumnName),
					Type:        col.ColumnType,
					Length:      uint8(col.StorageSize),
					Value:       nil,
					ForeignPath: "",
					Nullable:    col.IsNullable,
					Annotations: []string{},
				})
			}
		}
		parsedQuery.Fields = resolvedFields

		// Then we iterate over the query fields so we can check
		// - if the field exists in the table, otherwise error
		// - if the field is given the correct type, otherwise error
		for _, field := range parsedQuery.Fields {
			exists := false
			for _, col := range tableMetaData.Schema.GetColumns() {
				if field.Name == fmt.Sprintf("%s.%s", tableMetaData.Name, col.ColumnName) {
					exists = true
					break
				}
			}
			if !exists {
				return nil, ColumnNotFoundError{field.Name, tableMetaData.Name}
			}
		}

		// The final check if to see if the fields defined in "retornando" exist in the table
		for _, field := range parsedQuery.Returning {
			exists := false
			for _, col := range tableMetaData.Schema.GetColumns() {
				if field == col.ColumnName {
					exists = true
					break
				}
			}
			if !exists {
				return nil, ColumnNotFoundError{field, tableMetaData.Name}
			}
		}
	}

	// creame
	if parsedQuery.QueryType == query.QueryCreate {
		columnsSet := make(map[string]bool)

		identityCols := 0
		for _, field := range parsedQuery.Fields {
			if columnsSet[field.Name] {
				return nil, fmt.Errorf("Column \"%s\" is duplicated", field.Name)
			}
			if field.HasAnnotation("id") {
				identityCols++
			}
			columnsSet[field.Name] = true
		}
		if identityCols != 1 {
			return nil, fmt.Errorf("Table must have exactly one @id column")
		}
	}

	// TODO(@pandadiestro): analize query filters
	// TODO(@pandadiestro): prepare: resolve wildcards, order 'mete' statements fields, etc.
	return parsedQuery, nil
}

// The parser parses all values as string, so we need to resolve them to their
// respective types.
// TODO: Test if this works
func resolveAnyValueFromType(vType value.ValueType, val any) (any, error) {
	switch vType {
	case value.TypeInt32:
		v, err := strconv.Atoi(val.(string))
		if err != nil {
			return nil, InvalidValueForTypeError{vvalType: vType, val: val.(string)}
		}
		return int32(v), nil
	case value.TypeVarChar:
		return val.(string), nil
	case value.TypeBoolean:
		v, err := strconv.ParseBool(val.(string))
		if err != nil {
			return nil, InvalidValueForTypeError{vvalType: vType, val: val.(string)}
		}
		return v, nil
	case value.TypeFloat32:
		v, err := strconv.ParseFloat(val.(string), 32)
		if err != nil {
			return nil, InvalidValueForTypeError{vvalType: vType, val: val.(string)}
		}
		return v, nil
	default:
		return nil, fmt.Errorf("Unknown value type: %s", vType)
	}
}

func (db *ElenaDB) HasMetaTable() bool {
	return utils.FileExists(db.DbPath + meta.ELENA_META_TABLE_FILE)
}

func (db *ElenaDB) NextQueryId() uint32 {
	return db.NextQueryID.Add(1)
}

type InvalidValueForTypeError struct {
	vvalType value.ValueType
	val      string
}

func (e InvalidValueForTypeError) Error() string {
	return fmt.Sprintf("Invalid value \"%s\" for type %s", e.val, e.vvalType)
}

type ColumnNotFoundError struct {
	column string
	table  string
}

func (e ColumnNotFoundError) Error() string {
	return fmt.Sprintf("Column \"%s\" not found in table \"%s\"", e.column, e.table)
}

func (e *ElenaDB) Close() {
	e.bufferPool.FlushEntirePool() // clueless
	// TODO: flush elena_meta
}
