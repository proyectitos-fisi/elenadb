package commands

import (
	"fisi/elenadb/pkg/utils"
	"fmt"

	"github.com/urfave/cli/v2"
)

func RunQuery(_ *cli.Context, dbDir string, query string) error {
	if !utils.DirExists(dbDir) {
		return fmt.Errorf("database %s does not exist", dbDir)
	}

	// TODO: run db logic
	fmt.Printf("🚆 Running query '%s' on database '%s'\n", query, dbDir)
	return nil
}
