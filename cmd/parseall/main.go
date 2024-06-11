package main

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/internal/debugutils"
	"log"
	"os"
)


func main() {
    par := query.NewParser()
    qu, err := par.Parse(os.Stdin)
    if err != nil {
        log.Println(err)
        os.Exit(1)
        return
    }

    debugutils.PrettyPrint(qu)
}



