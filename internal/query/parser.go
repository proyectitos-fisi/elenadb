package query

import (
	"bufio"
	"fisi/elenadb/internal/tokens"
	"fmt"
	"io"
)

type Parser struct {
    parseFnMap map[StepType]ParseFn
    fsm        *FsmNode
    resetter   *FsmNode
}

func (par *Parser) reset() {
    par.fsm = par.resetter
}

func NewParser() *Parser {
    fs := defaultParseFsm()
    return &Parser{
        parseFnMap: defaultParseFnTable,
        fsm: fs,
        resetter: fs,
    }
}

func (par *Parser) Test(tk *tokens.Token) error {
    if tk == nil {
        return fmt.Errorf("uninitialized tokenizer")
    }

    expKeys := []string{}
    for key := range par.fsm.Children {
        if par.fsm.Children[key].Eval(tk) {
            par.fsm = par.fsm.Children[key]
            return nil
        }

        if len(par.fsm.Children[key].ExpectedString) == 0 {
            expKeys = append(expKeys, "any")
            continue
        }

        expKeys = append(expKeys, par.fsm.Children[key].ExpectedString)
    }

    return fmt.Errorf("expected one of %v, got %s instead", expKeys, tk.Data)
}

func (par *Parser) stepParseExec(qu *QueryBuilder, data string) error {
    if par.parseFnMap[par.fsm.Step] == nil {
        return nil
    }

    err := par.parseFnMap[par.fsm.Step](qu, data)
    if err != nil {
        return err
    }

    return nil
}

func (par *Parser) Parse(rd io.Reader) ([]Query, error) {
    br := bufio.NewReader(rd)
    tokenIter, tokenIterErr := tokens.Tokenize(br)
    if tokenIterErr != nil {
        return nil, tokenIterErr
    }

    newQuery := NewQueryBuilder()
    defer par.reset()

    for {
        tk, err := tokenIter.Next()
        if err != nil {
            if len(par.fsm.Children) == 0 || par.fsm.Eof {
                break
            }

            expKeys := []string{}
            for key := range par.fsm.Children {
                if len(par.fsm.Children[key].ExpectedString) == 0 {
                    expKeys = append(expKeys, "any")
                    continue
                }

                expKeys = append(expKeys, par.fsm.Children[key].ExpectedString)
            }

            return nil, fmt.Errorf("expected one of %v, got EOF instead", expKeys)
        }

        tkTestErr := par.Test(&tk)
        if tkTestErr != nil {
            return nil, tkTestErr
        }

        parseErr := par.stepParseExec(newQuery, tk.Data)
        if parseErr != nil {
            return nil, parseErr
        }
    }

    return newQuery.qu, nil
}
