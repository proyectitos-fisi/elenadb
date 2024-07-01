package repl

import (
	"github.com/fatih/color"
)

var identifiers = []string{
	"dame", "de", "donde", "pe",
	"creame", "tabla",
	"mete", "en", "retornando",
	"borra",
	"explicame",
	"set",
}

var types = []string{
	"int", "fkey", "char", "float", "bool",
}

type TokenType int

const (
	GenericKeyword TokenType = iota
	Identifier
	DataType
	Number
	String
	Annotation
	Operator
	Whitespace
	Literal
	Unknown
	Command
)

type Token struct {
	Type  TokenType
	Value []rune
}

type RunesWalker struct {
	input []rune
	pos   int
}

type WalkedRange struct {
	walker *RunesWalker
	value  []rune
}

func (r *RunesWalker) Walk() rune {
	if r.Exhausted() {
		return 0
	}
	chop := r.input[r.pos]
	r.pos++
	return chop
}

func (r *RunesWalker) WalkN(n int) WalkedRange {
	initialPos := r.pos
	for i := 0; i < n; i++ {
		if r.Exhausted() {
			break
		}
		r.pos++
	}
	return WalkedRange{r, r.input[initialPos:r.pos]}
}

func (r *RunesWalker) Chops(expected rune) bool {
	if r.Peek() == expected {
		r.Walk()
		return true
	}
	return false
}

func (r *RunesWalker) WalkWhile(verb func(rune) bool) WalkedRange {
	return r.OffsetWalkWhile(verb, 0)
}

func (r *RunesWalker) OffsetWalkWhile(verb func(rune) bool, offset int) WalkedRange {
	initialPos := r.pos
	curr := r.pos + offset

	if curr >= len(r.input) {
		r.pos = len(r.input)
		return WalkedRange{r, r.input[initialPos:]}
	}

	for curr < len(r.input) && verb(r.input[curr]) {
		curr++
	}
	r.pos = curr

	return WalkedRange{r, r.input[initialPos:curr]}
}

func (r *RunesWalker) Peek() rune {
	if r.Exhausted() {
		return 0
	}
	return r.input[r.pos]
}

func (r *RunesWalker) Exhausted() bool {
	return r.pos >= len(r.input)
}

func (r WalkedRange) Grow(offset int) WalkedRange {
	if offset == 0 {
		return r
	}

	if r.walker.pos+offset >= len(r.walker.input) {
		extra := r.walker.input[r.walker.pos:]
		r.walker.pos = len(r.walker.input)
		return WalkedRange{
			walker: r.walker,
			value:  append(r.value, extra...),
		}
	}

	extra := r.walker.input[r.walker.pos : r.walker.pos+offset]
	r.walker.pos += offset
	return WalkedRange{
		walker: r.walker,
		value:  append(r.value, extra...),
	}
}

func (t *Token) Colorized() string {
	tokenType := t.Type
	if isIdentifier(t.Value) {
		tokenType = Identifier
	} else if isDataType(t.Value) {
		tokenType = DataType
	} else if isLiteral(t.Value) {
		tokenType = Literal
	}

	color := TokenColor[tokenType]
	return color.Sprint(string(t.Value))
}

// TokenColor maps token types to their corresponding colors
var TokenColor = map[TokenType]color.Color{
	GenericKeyword: *color.New(color.FgWhite),
	Identifier:     *color.New(color.FgYellow),
	Number:         *color.New(color.FgGreen),
	String:         *color.New(color.FgGreen),
	Operator:       *color.New(color.FgRed),
	Unknown:        *color.New(color.FgWhite),
	Annotation:     *color.New(color.FgMagenta),
	DataType:       *color.New(color.FgGreen),
	Literal:        *color.New(color.FgMagenta),
	Command:        *color.New(color.FgBlue),
}

func token(walked WalkedRange, tokenType TokenType) Token {
	return Token{
		Value: walked.value,
		Type:  tokenType,
	}
}

func isIdentifier(input []rune) bool {
	for _, id := range identifiers {
		if string(input) == id {
			return true
		}
	}
	return false
}

func isDataType(input []rune) bool {
	for _, id := range types {
		if string(input) == id {
			return true
		}
	}
	return false
}

func isLiteral(input []rune) bool {
	str := string(input)
	if str == "true" || str == "false" {
		return true
	}
	return false
}
