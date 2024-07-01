package query_test

import (
    "bufio"
    "fisi/elenadb/internal/query"
    "fisi/elenadb/internal/tokens"
    "strings"
    "testing"
)


func TestExec(t *testing.T) {
    tests := []struct{
        query  string
        mapper map[string]interface{}
        expect    bool
    }{
        {
            query: `(id >= 5)`,
            mapper: map[string]interface{}{
                "id": 32,
                "name": "ramirez",
                "loqsea": 6.0,
            },
            expect: true,
        },
        {
            query: "(id >= 5 y loqsea == 6) o name <= ramirez",
            mapper: map[string]interface{}{
                "id": 0,
                "name": "pamirez",
                "loqsea": 50.0,
            },
            expect: true,
        },
        {
            query: "(id >= 5 y loqsea == 5) o (name == ramirez y isGerencial == true)",
            mapper: map[string]interface{}{
                "id": 0,
                "name": "hola",
                "loqsea": 4.0,
                "isGerencial": true,
            },
            expect: false,
        },
    }

    for index := range tests {
        filter := query.NewQueryFilter()
        read := bufio.NewReader(strings.NewReader(tests[index].query))
        tks, tksE := tokens.Tokenize(read)

        if tksE != nil {
            t.Log("error on tokenizer:", tksE)
            t.FailNow()
        }

        for {
            tk, err := tks.Next()
            if err != nil {
                break
            }

            filter.Push(&tk)
        }

        loaderr := filter.Load()
        if loaderr != nil {
            t.Fatal(loaderr)
        }

        resCmp, err := filter.Exec(tests[index].mapper)
        if err != nil {
            t.Fatal(err)
        }

        if resCmp != tests[index].expect {
            t.Fatalf("result on %s is wrong: expected %v got %v\n", tests[index].query, tests[index].expect, resCmp)
        }

        t.Logf("result on %s is correct: expected %v got %v", tests[index].query, tests[index].expect, resCmp)
    }
}




