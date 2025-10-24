package repl

import (
	"fisi/elenadb/internal/query"
	"fisi/elenadb/pkg/common"
	"fisi/elenadb/pkg/database"
	"fisi/elenadb/pkg/storage/table/value"
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
	fmt.Printf(
		"🚄 Elena DB v"+common.Version+"\n"+
			"   Built Date: "+common.BirthDate+"\n\n"+
			"   ElenaDB es una DBMS construida por estudiantes de la UNMSM como proyecto final\n"+
			"   de Algoritmos y Estructuras de Datos. Este sistema fue desarrollado con fines\n"+
			"   educativos y no debe usarse en entornos de producción (todavía).\n\n"+
			"   Utilice %v para conocer su uso.\n\n",
		color.YellowString("ayuda"),
	)

	elena, err := database.StartElenaBusiness(dbName)
	defer elena.RestInPeace()

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

			switch strings.TrimSpace(input) {
			case "limpia":
				clearScreen()
				repl.AppendHistory("limpia")
				continue
			case "ayuda":
				displayHelp()
				repl.AppendHistory("ayuda")
				continue
			case "tablas":
				listTables(elena)
				repl.AppendHistory("tablas")
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
				elapsed, err := ExecuteAndDisplay(elena, parser, fullInput)
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

func ExecuteAndDisplay(
	elena *database.ElenaDB,
	parser *query.Parser,
	fullInput string,
) (*time.Duration, error) {
	// chequear si begins con explicame
	const explainPrefix = "explicame "
	explainMode := strings.HasPrefix(strings.ToLower(fullInput), explainPrefix)
	input := strings.TrimPrefix(strings.TrimSpace(fullInput), explainPrefix)

	// Variable para indicar si estamos en modo "explicame"
	var isExplain bool
	if explainMode {
		isExplain = true
	}

	// 🚆 Database query execution!
	start := time.Now()
	tuples, schema, bindedQuery, plan, err := elena.ExecuteThisBaby(input, isExplain)
	if err != nil {
		elapsed := time.Since(start)
		return &elapsed, err
	}
	if tuples == nil {
		return nil, nil
	}

	if isExplain {
		fmt.Print("\n==== Parsing & Binding ====\n")
		printQuery(bindedQuery)

		fmt.Print("\n==== Query plan ====\n\n")
		fmt.Println(plan.ToString())
	}
	count := 0
	shouldPrintResults := !isExplain && !schema.IsEmpty()

	if shouldPrintResults {
		schema.PrintAsTableHeader()
	}

	for tuple := range tuples {
		if tuple.IsError() {
			elapsed := time.Since(start)
			return &elapsed, tuple.Error
		}

		if shouldPrintResults {
			tuple.Value.PrintAsRow(schema)
		}
		count++
	}
	if shouldPrintResults {
		schema.PrintTableDivisor()
		fmt.Println()
	}

	elapsed := time.Since(start)
	fmt.Printf("🚄 %d row(s) (%s)\n\n", count, elapsed)
	return &elapsed, nil
}

func clearScreen() {
	fmt.Print("\033[H\033[2J")
}

func listTables(db *database.ElenaDB) {
	fmt.Println()
	for _, table := range db.Catalog.TableMetadataMap {
		fmt.Println(table.Name)
		for _, col := range table.Schema.GetColumns() {
			fmt.Printf("  %s: %s", col.ColumnName, color.GreenString(col.ColumnType.AsString()))
			if col.ColumnType == value.TypeVarChar {
				fmt.Printf("(%d)", col.StorageSize)
			}
			if col.IsNullable {
				fmt.Print("?")
			}
			if col.IsUnique {
				fmt.Print(color.MagentaString(" @unique"))
			}
			if col.IsIdentity {
				fmt.Print(color.MagentaString(" @id"))
			}
			fmt.Println()
		}
		fmt.Println()
	}
}

func displayHelp() {
	fmt.Printf(`
   🚄🌫🌫  Bienvenido a la shell de ElenaDB!

   Hemos preparado la tabla %s para que puedas utilizar a Elena

   Obten algunos registros de la tabla

     %v

   Puedes utilizar filtros y seleccionar solo las columnas que desees

     %v

   Prueba obteniendo el top 3 de estudiantes

     %v

   También puedes crear tus propias tablas

     %v

   E insertar nuevos registro

     %v

   Añade "explicame" al inicio de tu consulta para ver el plan de ejecución

     %v

   Notas importantes:
   - todas las queries terminan con %s 🇵🇪
   - utiliza %s para ver todas las tablas
   - utiliza %s para limpiar la pantalla
   - utiliza %s para mostrar esta ayuda

`,
		color.GreenString("estudiantes"),
		Highlight("dame todo de estudiantes limite 5 pe"),
		Highlight("dame { codigo, nombre, correo, creditos } de estudiantes donde (creditos > 212 y es_tercio == false) pe"),
		Highlight("dame { correo, creditos } de estudiantes ordenado por creditos desc limite 3 pe"),
		Highlight("creame tabla cursos { id int @id, codigo char(3) @unique, nombre char(24) } pe"),
		Highlight("mete { codigo: \"123\", nombre: \"Programacion\" } en cursos pe"),
		Highlight("explicame <consulta> pe"),
		color.YellowString("pe"),
		color.YellowString("tablas"),
		color.YellowString("limpia"),
		color.YellowString("ayuda"),
	)
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

// FLAD_ESTRUCTURA: stack
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
