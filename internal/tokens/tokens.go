package tokens

import (
	"bufio"
	"errors"
	"fmt"
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
    TkString

    TkBoolOp

    TkWord
    TkAnnotation

    whitespace
)


// filter-only step types' names map
var LexedTokenStepNameTable = map[TkType]string{
    TkBracketOpen: "Opened Bracket",
    TkBracketClosed: "Closed Bracket",
    TkParenOpen: "Opened Parentheses",
    TkParenClosed: "Closed Parentheses",
    TkSeparator: "Comma Separator",
    TkValueIndicator: "Colon",
    TkNullable: "Nullable",
    TkBoolOp: "Bool Operator",
    TkWord: "Word",
    TkAnnotation: "Annotation",
    TkString: "String",
}

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

func (tki *TokenIterator) Size() int {
    return tki.size
}

func (tki *TokenIterator) GetAll() []Token {
    return tki.arr
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

func (tki *TokenIterator) Peek() (Token, error) {
    if tki.off + 1 >= tki.size {
        return Token{}, errors.New("iterend")
    }

    data := tki.arr[tki.off + 1]
    return data, nil
}


func getType(rn rune) TkType {
    if unicode.IsSpace(rn) {
        return whitespace
    }

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
    case '"':
        return TkString
    default:
        return TkWord
    }
}

func Tokenize(rd *bufio.Reader) (*TokenIterator, error) {
    strBuilder := new(strings.Builder)
    returnable := NewIterator()
    var typ TkType
    var last TkType

    var flags struct {
        isWord       bool
        isAnnotation bool
        isBoolOp     bool
        isNexusOp    bool
        isString     bool
    }

    for {
        rn, _, err := rd.ReadRune()
        if err == io.EOF {
            if flags.isString {
                return nil, fmt.Errorf("string literal left opened")
            }

            if strBuilder.Len() != 0 {
                returnable.Load(last, strBuilder.String())
            }

            break
        }

        typ = getType(rn)
        switch typ {
        case whitespace:
            if flags.isString {
                strBuilder.WriteRune(rn)
                continue
            }

            if strBuilder.Len() != 0 {
                returnable.Load(last, strBuilder.String())
                strBuilder.Reset()
            }

            flags.isWord, flags.isAnnotation = false, false
        case TkAnnotation:
            if flags.isString {
                strBuilder.WriteRune(rn)
                continue
            }

            if strBuilder.Len() != 0 && !flags.isAnnotation {
                returnable.Load(last, strBuilder.String())
                strBuilder.Reset()
                flags.isBoolOp, flags.isNexusOp = false, false
            }

            flags.isAnnotation = true
            // commented out so the '@' wont get into the actual
            // data, uncomment for it to be added anyways
            // build.WriteRune(rn)
        case TkString:
            if strBuilder.Len() != 0 && flags.isString {
                returnable.Load(last, strBuilder.String())
                strBuilder.Reset()
                flags.isBoolOp, flags.isNexusOp = false, false
            }

            flags.isString = !flags.isString
        case TkWord:
            if flags.isString {
                strBuilder.WriteRune(rn)
                continue
            }

            if strBuilder.Len() != 0 && !flags.isWord && !flags.isAnnotation {
                returnable.Load(last, strBuilder.String())
                strBuilder.Reset()
                flags.isBoolOp, flags.isNexusOp = false, false
            }

            if flags.isAnnotation {
                typ = TkAnnotation
            }

            flags.isWord = !flags.isAnnotation
            flags.isBoolOp, flags.isNexusOp = false, false
            strBuilder.WriteRune(rn)
        case TkBoolOp:
            if flags.isString {
                strBuilder.WriteRune(rn)
                continue
            }

            if strBuilder.Len() != 0 && !flags.isBoolOp {
                returnable.Load(last, strBuilder.String())
                strBuilder.Reset()
                flags.isWord, flags.isAnnotation = false, false
            }

            strBuilder.WriteRune(rn)
            flags.isBoolOp = true
        default:
            if flags.isString {
                strBuilder.WriteRune(rn)
                continue
            }

            if strBuilder.Len() != 0 {
                returnable.Load(last, strBuilder.String())
                strBuilder.Reset()
                flags.isWord, flags.isAnnotation, flags.isBoolOp, flags.isNexusOp = false, false, false, false
            }

            strBuilder.WriteRune(rn)
            returnable.Load(typ, strBuilder.String())
            strBuilder.Reset()
        }

        last = typ
    }

    return returnable, nil
}

