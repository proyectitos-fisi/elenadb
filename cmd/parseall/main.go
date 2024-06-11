package main

import (
	"fisi/elenadb/internal/debugutils"
	"fisi/elenadb/internal/query"
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



