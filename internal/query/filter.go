package query

import (
	"fmt"
	"strconv"
	"strings"

	"fisi/elenadb/internal/tokens"
	valuepkg "fisi/elenadb/pkg/storage/table/value"
)

type QueryFilter struct {
    out *tokens.TkStack
    in  *tokens.TkStack
}

// tabla.column -> tipo
func FieldType(string) valuepkg.ValueType {
    // FIXME: MOCK
    return valuepkg.TypeFloat32
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
        return (actuali32 <= mapper[field].(int32)), nil
    case "<":
        return (actuali32 < mapper[field].(int32)), nil
    case ">=":
        return (actuali32 >= mapper[field].(int32)), nil
    case ">":
        return (actuali32 > mapper[field].(int32)), nil
    case "!=":
        return (actuali32 != mapper[field].(int32)), nil
    case "==":
        return (actuali32 == mapper[field].(int32)), nil
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
        return (actuali32 <= mapper[field].(float32)), nil
    case "<":
        return (actuali32 < mapper[field].(float32)), nil
    case ">=":
        return (actuali32 >= mapper[field].(float32)), nil
    case ">":
        return (actuali32 > mapper[field].(float32)), nil
    case "!=":
        return (actuali32 != mapper[field].(float32)), nil
    case "==":
        return (actuali32 == mapper[field].(float32)), nil
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
    if tk.Type == tokens.TkBoolOp || tk.Type == tokens.TkParenOpen {
        qf.in.Push(*tk)
        return
    }

    if tk.Type == tokens.TkWord {
        qf.out.Push(*tk)
        return
    }

    for {
        tk, err := qf.in.Pop()
        if err != nil {
            return
        }

        if tk.Type == tokens.TkParenOpen {
            return
        }

        qf.out.Push(tk)
    }
}

func (qf *QueryFilter) execrec(mapper map[string]interface{}) (string, bool, error) {
    tk, err := qf.out.Pop()
    if err != nil {
        return "", false, err
    }

    leftstr, leftbool, leftexecerr := qf.execrec(mapper)
    if leftexecerr != nil {
        return "", false, leftexecerr
    }

    rightstr, rightbool, rightexecerr := qf.execrec(mapper)
    if rightexecerr != nil {
        return "", false, rightexecerr
    }

    switch true {
    case tk.Data == "y":
        return "", (leftbool && rightbool), nil
    case tk.Data == "o":
        return "", (leftbool || rightbool), nil
    default:
        cmpBool, cmpErr := CastAndCompare(leftstr, tk.Data, rightstr, mapper)
        if cmpErr != nil {
            return "", false, cmpErr
        }

        return "", cmpBool, nil
    }
}

func (qf *QueryFilter) Exec(mapper map[string]interface{}) (bool, error) {
    _, execbool, execerr := qf.execrec(mapper)
    return execbool, execerr
}


