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

			if walker.Peek() == '"' {
				str := walker.WalkWhileWithOffset(func(r rune) bool {
					return r != '"'
				}, 1)
				ch <- token(str.Grow(1), String)

			} else if unicode.IsLetter(walker.Peek()) {
				ch <- token(walker.WalkWhile(unicode.IsLetter), GenericKeyword)

			} else if unicode.IsNumber(walker.Peek()) {
				ch <- token(
					walker.WalkWhile(func(r rune) bool {
						return unicode.IsNumber(r) || r == '.'
					}),
					Number,
				)
			} else if unicode.IsSpace(walker.Peek()) {
				ch <- token(walker.WalkWhile(unicode.IsSpace), Whitespace)

			} else if unicode.IsSymbol(walker.Peek()) {
				ch <- token(walker.WalkWhile(unicode.IsSymbol), Operator)
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
