package query

import (
	"fmt"
	"strconv"
	"strings"

    "fisi/elenadb/internal/tokens"
    valuepkg "fisi/elenadb/pkg/storage/table/value"
)

func FieldType(field string) valuepkg.ValueType {
    switch field {
    case "id":
        return valuepkg.TypeInt32
    case "name":
        return valuepkg.TypeVarChar
    case "isGerencial":
        return valuepkg.TypeBoolean
    default:
        return valuepkg.TypeFloat32
    }
}

type QueryFilter struct {
    Out *tokens.TkStack
    In  *tokens.TkStack

    binder func(string)valuepkg.ValueType
}

func NewQueryFilter() *QueryFilter {
    return &QueryFilter{
        Out: &tokens.TkStack{},
        In: &tokens.TkStack{},
    }
}

func CompareBool(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    boolTypes := map[string]bool {
        "true": true,
        "false": false,
    }

    actualBool, ok := boolTypes[strings.ToLower(value)]
    if !ok {
        return false, fmt.Errorf("invalid boolean comparation of %s with %s", value, field)
    }

    switch cmp {
    case "!=":
        return (actualBool != mapper[field].(bool)), nil
    case "==":
        return (actualBool == mapper[field].(bool)), nil
    }

    return false, nil
}

func CompareInt32(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    actuali64, convErr := strconv.ParseInt(value, 10, 32)
    if convErr != nil {
        return false, convErr
    }

    actuali32 := int32(actuali64)
    switch cmp {
    case "<=":
        return (actuali32 <= int32(mapper[field].(int))), nil
    case "<":
        return (actuali32 < int32(mapper[field].(int))), nil
    case ">=":
        return (actuali32 >= int32(mapper[field].(int))), nil
    case ">":
        return (actuali32 > int32(mapper[field].(int))), nil
    case "!=":
        return (actuali32 != int32(mapper[field].(int))), nil
    case "==":
        return (actuali32 == int32(mapper[field].(int))), nil
    }

    return false, nil
}

func CompareFloat32(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    actuali64, convErr := strconv.ParseFloat(value, 64)
    if convErr != nil {
        return false, convErr
    }

    actuali32 := float32(actuali64)
    switch cmp {
    case "<=":
        return (actuali32 <= float32(mapper[field].(float64))), nil
    case "<":
        return (actuali32 < float32(mapper[field].(float64))), nil
    case ">=":
        return (actuali32 >= float32(mapper[field].(float64))), nil
    case ">":
        return (actuali32 > float32(mapper[field].(float64))), nil
    case "!=":
        return (actuali32 != float32(mapper[field].(float64))), nil
    case "==":
        return (actuali32 == float32(mapper[field].(float64))), nil
    }

    return false, nil
}

func CompareString(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    switch cmp {
    case "<=":
        return (value <= mapper[field].(string)), nil
    case "<":
        return (value < mapper[field].(string)), nil
    case ">=":
        return (value >= mapper[field].(string)), nil
    case ">":
        return (value > mapper[field].(string)), nil
    case "!=":
        return (value != mapper[field].(string)), nil
    case "==":
        return (value == mapper[field].(string)), nil
    }

    return false, nil
}

func CastAndCompare(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    switch FieldType(field) {
        case valuepkg.TypeBoolean:
            return CompareBool(field, cmp, value, mapper)
        case valuepkg.TypeInt32:
            return CompareInt32(field, cmp, value, mapper)
        case valuepkg.TypeFloat32:
            return CompareFloat32(field, cmp, value, mapper)
        case valuepkg.TypeVarChar:
            return CompareString(field, cmp, value, mapper)
        default:
            panic("invalid type")
    }
}

func (qf *QueryFilter) Push(tk *tokens.Token) {
    if tk.Type == tokens.TkParenOpen {
        qf.In.Push(*tk)
        return
    }

    if (tk.Type == tokens.TkWord || tk.Type == tokens.TkString) && tk.Data != "y" && tk.Data != "o" {
        qf.Out.Push(*tk)
        return
    }

    if tk.Type != tokens.TkParenClosed {
        if tk.Data != "y" && tk.Data != "o" {
            qf.In.Push(*tk)
            return
        }

        peekTk, peekErr := qf.In.Peek()
        if peekErr != nil {
            qf.In.Push(*tk)
            return
        }

        if peekTk.Data != "y" && peekTk.Data != "o" && peekTk.Type != tokens.TkParenOpen {
            tkN, _ := qf.In.Pop()
            qf.Out.Push(tkN)
            qf.In.Push(*tk)
            return
        }
    }

    for {
        tk, err := qf.In.Pop()
        if err != nil {
            return
        }

        if tk.Type == tokens.TkParenOpen {
            return
        }

        qf.Out.Push(tk)
    }
}

func (qf *QueryFilter) execrec(mapper map[string]interface{}) (string, bool, error) {
    tk, err := qf.Out.Pop()
    if err != nil {
        return "", false, err
    }

    if (tk.Type == tokens.TkWord && tk.Data != "y" && tk.Data != "o") || tk.Type == tokens.TkString {
        return tk.Data, false, nil
    }

    rightstr, rightbool, rightexecerr := qf.execrec(mapper)
    if rightexecerr != nil {
        return "", false, rightexecerr
    }

    leftstr, leftbool, leftexecerr := qf.execrec(mapper)
    if leftexecerr != nil {
        return "", false, leftexecerr
    }

    switch true {
    case tk.Data == "y":
        return "", (leftbool && rightbool), nil
    case tk.Data == "o":
        return "", (leftbool || rightbool), nil
    }

    cmpBool, cmpErr := CastAndCompare(leftstr, tk.Data, rightstr, mapper)
    if cmpErr != nil {
        return "", false, cmpErr
    }

    return "", cmpBool, nil
}

func (qf *QueryFilter) Exec(mapper map[string]interface{}) (bool, error) {
    for qf.In.Len() > 0 {
        tk, err := qf.In.Pop()
        if err != nil {
            break
        }

        qf.Out.Push(tk)
    }

    _, execbool, execerr := qf.execrec(mapper)
    return execbool, execerr
}


