package repl

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/database"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/go-json-experiment/json"
	"github.com/hokaccha/go-prettyjson"

	liner "github.com/proyectitos-fisi/elena-prompt"
)

const (
	PromptIdle    = "elena> "
	PromptWaiting = "   ... "
	HistoryFile   = ".elenadb_repl_history"
)

var (
	history_fn = filepath.Join(os.TempDir(), HistoryFile)
)

func StartREPL(dbName string) error {
	fmt.Println("🚄 Elena DB version", common.Version)
	elena, err := database.StartElenaBusiness(dbName)

	if err != nil {
		return err
	}

	if elena.IsJustCreated {
		fmt.Println("created db", dbName)
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

	prompt := PromptIdle

	defer writeHistory(repl)

	parser := query.NewParser()

	symbolStack := stack{}
	fullInput := ""

mainLoop:
	for {
		if input, err := repl.Prompt(prompt); err == nil {
			if input == "" {
				continue
			}

			sanitized := removeQuottedStrings(input)
			isEnd := isEndOfQuery(sanitized)
			fullInput += input + " "

			for _, c := range input {
				if c == '{' {
					symbolStack = symbolStack.Push('{')
				}
				if c == '}' {
					if symbolStack = symbolStack.Pop(); symbolStack == nil {
						fmt.Println("Syntax error: too many closing brackets")
						prompt = PromptIdle
						fullInput = ""
						continue mainLoop
					}
				}
			}

			if isEnd && symbolStack.Empty() {
				repl.AppendHistory(strings.TrimSpace(fullInput))
				elapsed, err := executeAndDisplay(elena, repl, parser, fullInput)
				if err != nil {
					fmt.Printf(
						"\n\033[31mError:\033[0m %v"+
							"\n🚄 0 row(s) (%s)\n",
						err, elapsed,
					)
					fmt.Println()
				}

				prompt = PromptIdle
				fullInput = ""
				continue
			}

			prompt = PromptWaiting

		} else {
			// End of REPL session
			fmt.Println()
			return nil
		}
	}
}

func executeAndDisplay(
	elena *database.ElenaDB,
	repl *liner.State,
	parser *query.Parser,
	fullInput string,
) (*time.Duration, error) {
	// 🚆 Database query execution!
	start := time.Now()
	tuples, schema, bindedQuery, plan, err := elena.ExecuteThisBaby(fullInput)
	if err != nil {
		elapsed := time.Since(start)
		return &elapsed, err
	}
	fmt.Println("\n===== Binding ======\n")
	printQuery(bindedQuery)

	fmt.Println("\n==== Query plan ====\n")
	fmt.Println(plan.ToString())

	count := 0

	if !schema.IsEmpty() {
		fmt.Println("\n====== Results =====\n")
		schema.PrintAsTableHeader()

		for tuple := range tuples {
			tuple.PrintAsRow(schema)
			count++
		}
		schema.PrintTableDivisor()
	}

	elapsed := time.Since(start)
	fmt.Printf("\n🚄 %d row(s) (%s)\n\n", count, elapsed)
	return &elapsed, nil
}

func newFormatter() prettyjson.Formatter {
	formatter := prettyjson.NewFormatter()
	formatter.NullColor = color.New(color.FgRed)
	formatter.KeyColor = color.New(color.FgMagenta)
	formatter.StringColor = color.New(color.FgGreen)
	formatter.BoolColor = color.New(color.FgYellow)
	formatter.NumberColor = color.New(color.FgRed)

	return *formatter
}

func printQuery(query *query.Query) error {
	formatter := newFormatter()
	bytes, err := json.Marshal(query, json.DefaultOptionsV2())
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

var endOfQueryRegex = regexp.MustCompile(`pe(\s?)+`)

func isEndOfQuery(input string) bool {
	return endOfQueryRegex.MatchString(input)
}

var quotesRegex = regexp.MustCompile(`"([^"]*)"`)

func removeQuottedStrings(text string) string {
	return quotesRegex.ReplaceAllString(text, "")
}

type stack []rune

func (s stack) Empty() bool {
	return len(s) == 0
}

func (s stack) Pop() stack {
	l := len(s)
	if l == 0 {
		return nil
	}
	return s[:l-1]
}

func (s stack) Push(str rune) stack {
	return append(s, str)
}
