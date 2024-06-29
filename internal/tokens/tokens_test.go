package tokens_test

import (
	"bufio"
	"fisi/elenadb/internal/tokens"
	"strings"
	"testing"
)


func TestTokenize(t *testing.T) {
    tests := []struct{
        query  string
        expect []tokens.Token
    }{
        {
            query: `new "text with a spaced string"`,
            expect: []tokens.Token{
                {
                    Type: tokens.TkWord,
                    Data: "new",
                },
                {
                    Type: tokens.TkString,
                    Data: `text with a spaced string`,
                },
            },
        },
        {
            query: `new "text @with y o a spaced string"`,
            expect: []tokens.Token{
                {
                    Type: tokens.TkWord,
                    Data: "new",
                },
                {
                    Type: tokens.TkString,
                    Data: `text @with y o a spaced string`,
                },
            },
        },
        {
            query: `dame { a, todo, c } y o yo oyu oo yde elena_meta pe`,
            expect: []tokens.Token{
                {
                    Type: tokens.TkWord,
                    Data: "new",
                },
                {
                    Type: tokens.TkString,
                    Data: `text @with y o a spaced string`,
                },
                {
                    Type: tokens.TkWord,
                    Data: "lol",
                },
            },
        },
        {
            query: `new "faulty unclosed string `,
        },
    }

    for index := range tests {
        reader := bufio.NewReader(strings.NewReader(tests[index].query))
        tks, tksE := tokens.Tokenize(reader)
        if tksE != nil {
            t.Log("error on tokenizer:", tksE)
            t.FailNow()
        }

        if tks.Size() != len(tests[index].expect) {
            t.Logf("test failed on %s: expected %d tokens, got %d tokens", tests[index].query, len(tests[index].expect), tks.Size())
            t.Logf("tokens: %v", tks.GetAll())
            t.FailNow()
        }

        for i := 0;; i++ {
            tk, tkE := tks.Next()
            if tkE != nil {
                break
            }

            if tk.Type != tests[index].expect[i].Type {
                t.Logf("test failed on %s: expected %s, got %s for token %v",
                    tests[index].query,
                    tokens.LexedTokenStepNameTable[tk.Type],
                    tokens.LexedTokenStepNameTable[tests[index].expect[i].Type],
                    tk,
                )

                t.FailNow()
            }

            if tk.Data != tests[index].expect[i].Data {
                t.Logf("test failed on %s: expected %s, got %s for token %v", tests[index].query, tk.Data, tests[index].expect[i].Data, tk)
                t.FailNow()
            }
        }

        t.Log("test passed with tokens: ", tks.GetAll())
    }
}



