package repl

import (
	"fisi/elenadb/cli/commands"
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/common"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"
	"github.com/go-json-experiment/json"
	"github.com/hokaccha/go-prettyjson"

	liner "github.com/proyectitos-fisi/elena-prompt"
)

const (
	PromptInitial = "elena> "
	PromptWaiting = "   ... "
	PromptEnd     = "pe"
	HistoryFile   = ".elenadb_repl_history"
)

var (
	history_fn = filepath.Join(os.TempDir(), HistoryFile)
)

func StartREPL(dbName string) error {
	created, err := commands.ElenaCreate(dbName, true)
	if err != nil {
		return err
	}

	repl := liner.NewLiner()
	defer repl.Close()

	repl.SetTabCompletionStyle(liner.TabCircular)
	repl.SetMultiLineMode(true)
	repl.SetCtrlCAborts(false)

	repl.SetProxy(SyntaxHighlighting)
	repl.SetCompleter(func(line string) (c []string) { return })

	if f, err := os.Open(history_fn); err == nil {
		repl.ReadHistory(f)
		f.Close()
	}

	prompt := PromptInitial

	defer writeHistory(repl)

	fmt.Println("🚄 Elena DB version", common.Version)
	if created {
		fmt.Println("creating db", dbName)
	}

	parser := query.NewParser()
	formatter := prettyjson.NewFormatter()
	formatter.NullColor = color.New(color.FgRed)

	for {
		if input, err := repl.Prompt(prompt); err == nil {
			if input == "" {
				continue
			}
			if !strings.HasSuffix(input, PromptEnd) {
				prompt = PromptWaiting
				continue
			}
			prompt = PromptInitial
			repl.AppendHistory(input)

			parseAndPrint(parser, formatter, input)
		} else {
			// End of REPL session
			fmt.Println()
			return nil
		}
	}
}

func parseAndPrint(parser *query.Parser, formatter *prettyjson.Formatter, input string) error {
	res, err := parser.Parse(strings.NewReader(input))
	if err != nil {
		return err
	}

	bytes, err := json.Marshal(res, json.DefaultOptionsV2())
	if err != nil {
		return err
	}

	formattedBytes, formatErr := formatter.Format(bytes)
	if formatErr != nil {
		return formatErr
	}

	fmt.Printf("%s\n", formattedBytes)
	return nil
}

func writeHistory(line *liner.State) {
	if f, err := os.Create(history_fn); err != nil {
		fmt.Print("Error writing history file: ", err)
	} else {
		line.WriteHistory(f)
		f.Close()
	}
}
