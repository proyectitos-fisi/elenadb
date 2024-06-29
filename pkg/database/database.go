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
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/utils"
	"os"
	"strings"
)

type ElenaDB struct {
	diskManager *storage_disk.DiskManager
	// Elena database directory
	DbPath string
	// Elena's buffer pool manager
	bufferPool *buffer.BufferPoolManager
	// Whether this instance created the database for the first time
	IsJustCreated bool
}

// Creates the Elena Instance. Should be called only once per process.
// Long live to ELENA! WE LOVE ELENA! 🚄!!
func StartElenaBusiness(dbPath string) (*ElenaDB, error) {
	dbPath = utils.WithTrailingSlash(dbPath)

	bpm := buffer.NewBufferPoolManager(dbPath, common.BufferPoolSize, common.LRUKReplacerK)

	elena := &ElenaDB{
		DbPath:        dbPath,
		bufferPool:    bpm,
		IsJustCreated: false,
		// empty catalog
	}

	err := elena.CreateDatabaseIfNotExists()
	if err != nil {
		return nil, err
	}

	err = elena.CreateMetaTableIfNotExists()
	if err != nil {
		return nil, err
	}
	return elena, nil
}

// Populate catalog
func (elena *ElenaDB) PopulateCatalog() error {
	tableMetadataMap := make(map[string]*catalog.TableMetadata)
	indexMetadataMap := make(map[string]*catalog.IndexMetadata)

	tuples, sch, plan, err := elena.ExecuteThisBaby("dame todo de elena_meta pe")
	if err != nil {
		return err
	}

	for tuple := range tuples {
		fileType := tuple.Values[1].AsVarchar()
		name := tuple.Values[2].AsVarchar()
		fileId := tuple.Values[3].AsInt32()
		root := tuple.Values[4].AsInt32()
		sql := tuple.Values[5].AsVarchar()

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

	return nil
}

// Executes a SQL query. The steps are as follows:
// - (sqlPipeline) Parse the query
// - (sqlPipeline) Analize/bind the query
// - (sqlPipeline) Optimize the query
// - Make a plan based on the query
// - Optimize the plan
// - Execute the plan, fetching the tuples one by one
func (e *ElenaDB) ExecuteThisBaby(input string) (chan *tuple.Tuple, *schema.Schema, PlanNode, error) {
	parsedQuery, err := e.sqlPipeline(input)
	if err != nil {
		return nil, nil, nil, err
	}

	nodePlan, err := MakeQueryPlan(parsedQuery, e)

	if err != nil {
		return nil, nil, nil, err
	}
	nodePlan = OptimizeQueryPlan(nodePlan)

	tuples := make(chan *tuple.Tuple)

	go func() {
		for {
			tuple := nodePlan.Next() // executor
			if tuple == nil {
				break
			}

			tuples <- tuple
		}
		close(tuples)
	}()

	return tuples, nodePlan.Schema(), nodePlan, nil
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

func (e *ElenaDB) CreateMetaTableIfNotExists() error {
	if utils.FileExists(e.DbPath + meta.ELENA_META_TABLE_FILE) {
		return nil
	}

	result, _, _, err := e.ExecuteThisBaby(meta.ELENA_META_CREATE_SQL)
	if err != nil {
		return err
	}

	<-result
	return nil
}

// Parses, analizes, optimizes and prepares (in-place) the query for execution.
func (e *ElenaDB) sqlPipeline(input string) (*query.Query, error) {
	parser := query.NewParser()
	statements, err := parser.Parse(strings.NewReader(input))

	if err != nil {
		return nil, err
	}

	// TODO(@pandadiestro): analize query (use the catalog)
	// TODO(@pandadiestro): prepare: resolve wildcards, order 'mete' statements fields, etc.

	return &statements[0], nil
}

func (e *ElenaDB) Close() {
	e.bufferPool.FlushEntirePool() // clueless
}
