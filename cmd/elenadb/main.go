package main

import (
	"fmt"
	"os"

	"fisi/elenadb/elena/commands"
	"fisi/elenadb/elena/repl"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/utils"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:            common.Name,
		Usage:           common.Description,
		UsageText:       fmt.Sprintf("%s <db> [query | file.sql]", common.Name),
		Version:         common.Version,
		HideHelpCommand: true,
		Commands: []*cli.Command{
			{
				Name:  "<db>",
				Usage: "db directory to work with",
				// no action, just redirect to the root command
			},
		},
		Action: func(ctx *cli.Context) error {
			dbDirectory := ctx.Args().First()
			toExecute := ctx.Args().Get(1)

			if dbDirectory == "" {
				// nothing passed, show help
				return cli.ShowAppHelp(ctx)
			}

			if dbDirectory == "" {
				return fmt.Errorf("missing database name. use --create <db>")
			}

			if toExecute == "" {
				return repl.StartREPL(dbDirectory)
			}

			if utils.FileExists(toExecute) {
				return commands.RunFromFile(ctx, dbDirectory, toExecute)
			}

			if toExecute != "" {
				return commands.RunQuery(ctx, dbDirectory, toExecute)
			}
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
