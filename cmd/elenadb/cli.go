package main

import (
	"fmt"
	"io/fs"
	"os"

	"fisi/elenadb/pkg/database"
	"fisi/elenadb/pkg/storage"
	"fisi/elenadb/pkg/utils"

	"github.com/urfave/cli/v2"
)

func elenaCreate(_ *cli.Context, name string) error {
	if utils.FileOrDirExists(name) {
		return fmt.Errorf("db %s already exists", name)
	}

	// TODO: create db logic
	fmt.Printf("🚆 Creating db %s\n", name)
	err := os.Mkdir(name, fs.FileMode(0755))
	return err
}

func runFile(_ *cli.Context, dbDir string, file string) error {
	f, err := os.Open(file)

	if err != nil {
		return err
	}
	defer f.Close()

	// TODO: run file logic
	fmt.Printf("🚆 Running file '%s' on database '%s'\n", file, dbDir)
	return nil
}

func run(_ *cli.Context, dbDir string, query string) error {
	if !utils.DirExists(dbDir) {
		return fmt.Errorf("database %s does not exist", dbDir)
	}

	// TODO: run db logic
	fmt.Printf("🚆 Running query '%s' on database '%s'\n", query, dbDir)
	return nil
}

func main() {
	// just testing
	// t := 3
	// tree := storage.NewBPTree(t)

	// keys := []int{10, 20, 5, 6, 12, 30, 7, 17, 8, 9, 40, 24}
	// for _, key := range keys {
	// 	tree.Insert(key)
	// }

	// key := 17
	// node, index := tree.Search(key)
	// if node != nil {
	// 	fmt.Printf("Found key %d at index %d in node with keys %v\n", key, index, node.Keys)
	// } else {
	// 	fmt.Printf("Key %d not found\n", key)
	// }

	// storage.PrintTree(tree.Root, 0)

	// another testing
	storage.Testing()

	// cli

	app := &cli.App{
		Name:            database.Name,
		Usage:           database.Description,
		UsageText:       fmt.Sprintf("%s [--create] <db> [query | file.sql]", database.Name),
		Version:         database.Version,
		HideHelpCommand: true,
		Commands: []*cli.Command{
			{
				Name:  "<db>",
				Usage: "db directory to work with",
				// no action, just redirect to the root command
			},
		},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "create",
				Usage: "creates the <db> database",
			},
		},
		Action: func(ctx *cli.Context) error {
			dbDirectory := ctx.Args().First()
			create := ctx.Bool("create")
			toExecute := ctx.Args().Get(1)

			if dbDirectory == "" && !create {
				// nothing passed, show help
				return cli.ShowAppHelp(ctx)
			}

			if dbDirectory == "" {
				return fmt.Errorf("missing database name. use --create <db>")
			}

			if toExecute == "" && utils.DirExists(dbDirectory) && !create {
				// TODO: nothing to do. Start REPL?
				fmt.Println("🚆 Nothing to do. Try passing a [query] or [file.sql]")
				return nil
			}
			if toExecute == "" && !utils.DirExists(dbDirectory) && !create {
				return fmt.Errorf("database %s does not exist", dbDirectory)
			}

			if create {
				err := elenaCreate(ctx, dbDirectory)
				if err != nil {
					return err
				}
			}

			if utils.FileExists(toExecute) {
				return runFile(ctx, dbDirectory, toExecute)
			}

			if toExecute != "" {
				return run(ctx, dbDirectory, toExecute)
			}
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
