package commands

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func RunFromFile(_ *cli.Context, dbDir string, file string) error {
	f, err := os.Open(file)

	if err != nil {
		return err
	}
	defer f.Close()

	// TODO: run file logic
	fmt.Printf("🚆 Running file '%s' on database '%s'\n", file, dbDir)
	return nil
}
