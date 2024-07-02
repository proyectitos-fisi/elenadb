package query

import (
	"fmt"
	"strconv"
	"strings"

	"fisi/elenadb/internal/tokens"
	valuepkg "fisi/elenadb/pkg/storage/table/value"
)

type QueryFilter struct {
    Out      *tokens.TkStack
    In       *tokens.TkStack
    offset   int
    Resolver func(string)valuepkg.ValueType
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
        return (mapper[field].(bool) != actualBool), nil
    case "==":
        return (mapper[field].(bool) == actualBool), nil
    default:
        return false, fmt.Errorf("invalid boolean operation %s", cmp)
    }
}

func CompareInt32(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    actuali64, convErr := strconv.ParseInt(value, 10, 32)
    if convErr != nil {
        return false, convErr
    }

    actuali32 := int32(actuali64)
    switch cmp {
    case "<=":
        return (int32(mapper[field].(int)) <= actuali32), nil
    case "<":
        return (int32(mapper[field].(int)) < actuali32), nil
    case ">=":
        return (int32(mapper[field].(int)) >= actuali32), nil
    case ">":
        return (int32(mapper[field].(int)) > actuali32), nil
    case "!=":
        return (int32(mapper[field].(int)) != actuali32), nil
    case "==":
        return (int32(mapper[field].(int)) == actuali32), nil
    default:
        return false, fmt.Errorf("invalid boolean operation %s", cmp)
    }
}

func CompareFloat32(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    actualf64, convErr := strconv.ParseFloat(value, 64)
    if convErr != nil {
        return false, convErr
    }

    actualf32 := float32(actualf64)
    switch cmp {
    case "<=":
        return (float32(mapper[field].(float64)) <= actualf32), nil
    case "<":
        return (float32(mapper[field].(float64)) < actualf32), nil
    case ">=":
        return (float32(mapper[field].(float64)) >= actualf32), nil
    case ">":
        return (float32(mapper[field].(float64)) > actualf32), nil
    case "!=":
        return (float32(mapper[field].(float64)) != actualf32), nil
    case "==":
        return (float32(mapper[field].(float64)) == actualf32), nil
    default:
        return false, fmt.Errorf("invalid boolean operation %s", cmp)
    }
}

func CompareString(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    switch cmp {
    case "<=":
        return (mapper[field].(string) <= value), nil
    case "<":
        return (mapper[field].(string) < value), nil
    case ">=":
        return (mapper[field].(string) >= value), nil
    case ">":
        return (mapper[field].(string) > value), nil
    case "!=":
        return (mapper[field].(string) != value), nil
    case "==":
        return (mapper[field].(string) == value), nil
    default:
        return false, fmt.Errorf("invalid boolean operation %s", cmp)
    }
}

func (qf *QueryFilter) CastAndCompare(field string, cmp string, value string, mapper map[string]interface{}) (bool, error) {
    switch qf.Resolver(field) {
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

func (qf *QueryFilter) Push(tk *tokens.Token) error {
    if tk.Type == tokens.TkParenOpen {
        qf.In.Push(*tk)
        return nil
    }

    if (tk.Type == tokens.TkWord || tk.Type == tokens.TkString) && tk.Data != "y" && tk.Data != "o" {
        qf.Out.Push(*tk)
        return nil
    }

    if tk.Type != tokens.TkParenClosed {
        if tk.Data != "y" && tk.Data != "o" {
            qf.In.Push(*tk)
            return nil
        }

        peekTk, peekErr := qf.In.Peek()
        if peekErr != nil {
            qf.In.Push(*tk)
            return nil
        }

        if peekTk.Data != "y" && peekTk.Data != "o" && peekTk.Type != tokens.TkParenOpen {
            tkN, _ := qf.In.Pop()
            qf.Out.Push(tkN)
            qf.In.Push(*tk)
            return nil
        }
    }

    for {
        tk, err := qf.In.Pop()
        if err != nil {
            break
        }

        if tk.Type == tokens.TkParenOpen {
            return nil
        }

        qf.Out.Push(tk)
    }

    qf.In = nil
    qf.Out = nil
    return fmt.Errorf("not enough open parentheses to close")
}

func (qf *QueryFilter) execrec(mapper map[string]interface{}) (string, bool, error) {
    tk, err := qf.Out.NdPop()
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

    cmpBool, cmpErr := qf.CastAndCompare(leftstr, tk.Data, rightstr, mapper)
    if cmpErr != nil {
        return "", false, cmpErr
    }

    return "", cmpBool, nil
}

func (qf *QueryFilter) Load() (error) {
    for qf.In.Len() > 0 {
        tk, err := qf.In.Pop()
        if err != nil {
            break
        }

        if tk.Type == tokens.TkParenOpen {
            return fmt.Errorf("some parentheses were left opened")
        }

        qf.Out.Push(tk)
    }

    return nil
}

func (qf *QueryFilter) Exec(mapper map[string]interface{}) (bool, error) {
    _, execbool, execerr := qf.execrec(mapper)
    return execbool, execerr
}


