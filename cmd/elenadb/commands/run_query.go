package commands

import (
	"fisi/elenadb/cli/repl"
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/database"
	"fmt"

	"github.com/urfave/cli/v2"
)

func RunQuery(_ *cli.Context, dbDir string, inputQuery string) error {
	elena, err := database.StartElenaBusiness(dbDir)
	defer elena.RestInPeace()
	if err != nil {
		return err
	}
	parser := query.NewParser()
	elapsed, err := repl.ExecuteAndDisplay(elena, parser, inputQuery)
	if err != nil {
		fmt.Printf(
			"\n\033[31mError:\033[0m %v"+
				"\n🚄 0 row(s) (%s)\n",
			err, elapsed,
		)
	}
	return nil
}
