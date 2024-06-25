package database

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/buffer"
	"fisi/elenadb/pkg/catalog/schema"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/plan"
	storage_disk "fisi/elenadb/pkg/storage/disk"
	"fisi/elenadb/pkg/storage/table/tuple"
	"fisi/elenadb/pkg/utils"
	"os"
	"strings"
)

type ElenaDB struct {
	diskManager *storage_disk.DiskManager
	// Elena database directory
	dbPath string
	// Elena's buffer pool manager
	bufferPool *buffer.BufferPoolManager
	// Whether this instance created the database for the first time
	IsJustCreated bool
}

// Creates the Elena Instance. Should be called only once per process.
// Long live to ELENA! WE LOVE ELENA! 🚄!!
func StartElenaBusiness(dbPath string) (*ElenaDB, error) {
	diskManager, err := storage_disk.NewDiskManager(dbPath)

	if err != nil {
		panic(err)
	}

	bpm := buffer.NewBufferPoolManager(common.BufferPoolSize, diskManager, common.LRUKReplacerK)

	elena := &ElenaDB{
		diskManager:   diskManager,
		dbPath:        dbPath,
		bufferPool:    bpm,
		IsJustCreated: false,
	}
	elena.CreateDatabaseIfNotExists()

	return elena, nil
}

// Executes a SQL query. The steps are as follows:
// - (sqlPipeline) Parse the query
// - (sqlPipeline) Analize/bind the query
// - (sqlPipeline) Optimize the query
// - Make a plan based on the query
// - Optimize the plan
// - Execute the plan, fetching the tuples one by one
func (e *ElenaDB) ExecuteThisBaby(input string) (*schema.Schema, chan tuple.Tuple, error) {
	parsedQuery, err := e.sqlPipeline(input)
	if err != nil {
		return nil, nil, err
	}

	nodePlan, err := e.makePlan(parsedQuery)

	if err != nil {
		return nil, nil, err
	}
	nodePlan = e.optimizePlan(nodePlan)

	tuples := make(chan tuple.Tuple)

	go func() {
		for {
			tuple := nodePlan.Next()
			if tuple == nil {
				break
			}

			tuples <- *tuple
		}
		close(tuples)
	}()

	return nodePlan.Schema(), tuples, nil
}

func (e *ElenaDB) CreateDatabaseIfNotExists() error {
	if utils.DirExists(e.dbPath) {
		return nil
	}

	err := os.Mkdir(e.dbPath, os.ModePerm)
	if err != nil {
		return err
	}

	e.IsJustCreated = true

	return nil
}

func (e *ElenaDB) optimizePlan(inputPlan plan.PlanNode) plan.PlanNode {
	return inputPlan
}

func (e *ElenaDB) makePlan(inputQuery *query.Query) (plan.PlanNode, error) {
	switch inputQuery.QueryType {
	case query.QueryCreate:
		return plan.MakeCreatePlan(inputQuery)
	case query.QueryRetrieve:
		return plan.MakeSelectPlan(inputQuery)
	case query.QueryInsert:
		return plan.MakeInsertPlan(inputQuery)
	case query.QueryDelete:
		return plan.MakeDeletePlan(inputQuery)
	case query.QueryUpdate:
		return plan.MakeUpdatePlan(inputQuery)
	default:
		return nil, plan.UnknownPlanError{}
	}
}

// Parses, analizes, optimizes and prepares (in-place) the query for execution.
func (e *ElenaDB) sqlPipeline(input string) (*query.Query, error) {
	parser := query.NewParser()
	statements, err := parser.Parse(strings.NewReader(input))

	if err != nil {
		return nil, err
	}

	// TODO(@pandadiestro): analize query (use the catalog)
	// TODO(@pandadiestro): optimize query (nice to have)
	// TODO(@pandadiestro): prepare: resolve wildcards, order 'mete' statements fields, etc.

	return &statements[0], nil
}

func (e *ElenaDB) Close() {
	e.bufferPool.FlushEntirePool() // clueless
}
