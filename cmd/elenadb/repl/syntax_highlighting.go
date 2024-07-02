package repl

import (
	"strings"
	"unicode"
)

func tokenize(input []rune) chan Token {
	walker := RunesWalker{input, 0}
	ch := make(chan Token)

	go func() {
		for {
			if walker.Exhausted() {
				close(ch)
				break
			}

			if walker.Peek() == '@' {
				ch <- token(walker.OffsetWalkWhile(unicode.IsLetter, 1), Annotation)

			} else if walker.Peek() == '"' {
				ch <- token(walker.OffsetWalkWhile(isNotQuote, 1).Grow(1), String)

			} else if unicode.IsLetter(walker.Peek()) {
				ch <- token(walker.WalkWhile(isAlphanumeric), GenericKeyword)

			} else if unicode.IsNumber(walker.Peek()) {
				ch <- token(walker.WalkWhile(numberOrDot), Number)

			} else if unicode.IsSpace(walker.Peek()) {
				ch <- token(walker.WalkWhile(unicode.IsSpace), Whitespace)

			} else if unicode.IsSymbol(walker.Peek()) {
				ch <- token(walker.WalkWhile(isOperator), Operator)

			} else {
				ch <- token(walker.WalkN(1), Unknown)
			}
		}
	}()
	return ch
}

func SyntaxHighlighting(input []rune, pos int) []rune {
	highlighted := strings.Builder{}

	for token := range tokenize(input) {
		highlighted.WriteString(token.Colorized())
	}
	return []rune(highlighted.String())
}

func Highlight(input string) string {
	return string(SyntaxHighlighting([]rune(input), 0))
}

var isNotQuote = func(r rune) bool {
	return r != '"'
}

var numberOrDot = func(r rune) bool {
	return unicode.IsNumber(r) || r == '.'
}

var isAlphanumeric = func(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

var isOperator = func(r rune) bool {
	return unicode.IsSymbol(r) || r == '!'
}
