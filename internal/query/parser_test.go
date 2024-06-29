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
