package main

import (
	"fisi/elenadb/internal/query"
	"fmt"
)


func main() {
    //iter := query.Tokenize(bufio.NewReader(os.Stdin))
    //fmt.Printf("%#v\n", iter)
    //for {
    //    tk, err := iter.Next()
    //    if err != nil {
    //        break
    //    }
//
    //    fmt.Printf("%v\n", tk)
    //}

    fs := query.NewFsm()
    fs.AddRule(&query.FsmNode{
        Step: query.FsmCreateStep,
        Expect: "creame",
        Children: map[query.StepType]*query.FsmNode{},
    }, query.FsmCreateStep)

    fs.AddRule(&query.FsmNode{
        Step: query.FsmTable,
        Expect: "tabla",
        Children: map[query.StepType]*query.FsmNode{},
    }, query.FsmCreateStep, query.FsmTable)

    fmt.Println((*fs).Children[query.FsmCreateStep])
    fmt.Println((*((*fs).Children[query.FsmCreateStep])).Children[query.FsmTable])
}



