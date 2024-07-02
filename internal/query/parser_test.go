package query_test

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/storage/table/value"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsingCreame(t *testing.T) {
	input := "creame tabla elena_meta { type char(5), } pe"

	parser := query.NewParser()
	results, err := parser.Parse(strings.NewReader(input))

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	result := results[0]

	if result.QueryType != query.QueryCreate {
		t.Fatalf("unexpected query type: %s", result.QueryType)
	}

	assert.Equal(t, "elena_meta", result.QueryInstrName)
	assert.Equal(t, 1, len(result.Fields))
	assert.Equal(t, "type", result.Fields[0].Name)
	assert.Equal(t, value.TypeVarChar, result.Fields[0].Type)
}

func TestParsingMeteWithRetornando(t *testing.T) {
	input := "mete { num: 6 } en some_table retornando { a, b } pe"

	parser := query.NewParser()
	results, err := parser.Parse(strings.NewReader(input))

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	result := results[0]

	if result.QueryType != query.QueryInsert {
		t.Fatalf("unexpected query type: %s", result.QueryType)
	}

	assert.Equal(t, "some_table", result.QueryInstrName)
	assert.Equal(t, 1, len(result.Fields))
	assert.Equal(t, "num", result.Fields[0].Name)
	assert.Equal(t, []string{"a", "b"}, result.Returning)
}

func TestParsingMeteWithoutRetornando(t *testing.T) {
	input := "mete { num1: 6, str2: \"abcdefg\" } en some_table pe"

	parser := query.NewParser()
	results, err := parser.Parse(strings.NewReader(input))

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	result := results[0]

	if result.QueryType != query.QueryInsert {
		t.Fatalf("unexpected query type: %s", result.QueryType)
	}

	assert.Equal(t, "some_table", result.QueryInstrName)
	assert.Equal(t, 2, len(result.Fields))
	assert.Equal(t, "num1", result.Fields[0].Name)
	assert.Equal(t, "6", result.Fields[0].Value)
	assert.Equal(t, "str2", result.Fields[1].Name)
	assert.Equal(t, "abcdefg", result.Fields[1].Value)
	assert.Nil(t, result.Returning)
}

func TestBorra(t *testing.T) {
	input := "borra de some_table donde (id == 5 y name == andrius) pe"

	parser := query.NewParser()
	results, err := parser.Parse(strings.NewReader(input))

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	result := results[0]
	if result.QueryType != query.QueryErase {
		t.Fatalf("unexpected query type: %s", result.QueryType)
	}

	assert.Equal(t, "some_table", result.QueryInstrName)
    t.Log(result.Filter.Out.GetAll())
	assert.Nil(t, result.Returning)
}

func TestOrderingBy(t *testing.T) {
	input := "dame todo de some_table ordenando por columna donde (id == 5 y name == andrius) pe"

	parser := query.NewParser()
	results, err := parser.Parse(strings.NewReader(input))

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	result := results[0]
	if result.QueryType != query.QueryRetrieve {
		t.Fatalf("unexpected query type: %s", result.QueryType)
	}

	assert.Equal(t, "columna", result.OrderedBy)
    t.Log(result.Filter.Out.GetAll())
	assert.Nil(t, result.Returning)
}


