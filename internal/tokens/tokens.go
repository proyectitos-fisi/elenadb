package tokens

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"unicode"
)

type TkType uint8
const (
    TkBracketOpen TkType = iota
    TkBracketClosed
    TkParenOpen
    TkParenClosed
    TkSeparator
    TkValueIndicator
    TkNullable

    TkBoolOp

    TkWord
    TkAnnotation
)

type Token struct {
    Type TkType
    Data string
}

type TokenIterator struct {
    arr []Token
    off int
    size int
}

func NewIterator() *TokenIterator {
    return &TokenIterator{
        arr: []Token{},
        off: -1,
        size: 0,
    }
}

func (tki *TokenIterator) Load(tkType TkType, tkData string) {
    tki.arr = append(tki.arr, Token{
        Type: tkType,
        Data: tkData,
    })

    tki.size++
}

func (tki *TokenIterator) Next() (Token, error) {
    if tki.off + 1 >= tki.size {
        return Token{}, errors.New("iterend")
    }

    tki.off++
    data := tki.arr[tki.off]
    return data, nil
}

func getType(rn rune) TkType {
    switch rn {
    case '{':
        return TkBracketOpen
    case '}':
        return TkBracketClosed
    case '(':
        return TkParenOpen
    case ')':
        return TkParenClosed
    case ',':
        return TkSeparator
    case ':':
        return TkValueIndicator
    case '?':
        return TkNullable
    case '@':
        return TkAnnotation
    case '>', '<', '=', '!':
        return TkBoolOp
    default:
        return TkWord
    }
}

func Tokenize(rd *bufio.Reader) (*TokenIterator) {
    strBuilder := new(strings.Builder)
    returnable := NewIterator()
    var typ TkType

    var flags struct {
        isWord       bool
        isAnnotation bool
    }

    for {
        rn, _, err := rd.ReadRune()
        if err == io.EOF {
            if strBuilder.Len() != 0 {
                returnable.Load(typ, strBuilder.String())
            }

            break
        }

        if unicode.IsSpace(rn) {
            if (flags.isWord || flags.isAnnotation) && strBuilder.Len() != 0 {
                returnable.Load(typ, strBuilder.String())
                strBuilder.Reset()
            }

            flags.isWord, flags.isAnnotation = false, false
            continue
        }

        typ = getType(rn)

        switch typ {
        case TkAnnotation:
            flags.isAnnotation = true
            // commented out so the '@' wont get into the actual
            // data, uncomment for it to be added anyways
            // build.WriteRune(rn)
            continue
        case TkWord:
            if flags.isAnnotation {
                typ = TkAnnotation
            }

            flags.isWord = !flags.isAnnotation
            strBuilder.WriteRune(rn)
            continue
        default:
            if strBuilder.Len() != 0 {
                if flags.isWord {
                    returnable.Load(TkWord, strBuilder.String())
                } else {
                    returnable.Load(TkAnnotation, strBuilder.String())
                }
            }

            strBuilder.Reset()
            flags.isWord, flags.isAnnotation = false, false

            strBuilder.WriteRune(rn)
            returnable.Load(typ, strBuilder.String())
            strBuilder.Reset()
            continue
        }
    }

    return returnable
}

