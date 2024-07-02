package query

import (
	"fisi/elenadb/internal/tokens"
	"fisi/elenadb/pkg/storage/table/value"
	"fmt"
	"strconv"
)

type QueryBuilder struct {
    qu []Query
}

func NewQueryBuilder() *QueryBuilder {
    return &QueryBuilder{
        qu: make([]Query, 0),
    }
}

func (qb *QueryBuilder) PushInstr(tp QueryInstrType) {
    qb.qu = append(qb.qu, Query{
        QueryType: tp,
    })
}

type ParseFn func(*QueryBuilder, *tokens.Token) error

func parseCreateFn(qu *QueryBuilder, _ *tokens.Token) error {
    qu.PushInstr(QueryCreate)
    return nil
}

func parseRetrieveFn(qb *QueryBuilder, _ *tokens.Token) error {
    qb.PushInstr(QueryRetrieve)
    return nil
}

func parseInsertFn(qb *QueryBuilder, _ *tokens.Token) error {
    qb.PushInstr(QueryInsert)
    return nil
}

func parseChangeFn(qb *QueryBuilder, _ *tokens.Token) error {
    qb.PushInstr(QueryUpdate)
    return nil
}

func parseEraseFn(qu *QueryBuilder, _ *tokens.Token) error {
    qu.PushInstr(QueryErase)
    return nil
}

func parseTableNameFn(qb *QueryBuilder, tk *tokens.Token) error {
    qb.qu[len(qb.qu)-1].QueryInstrName = tk.Data
    return nil
}

func parseFieldTypeFn(qb *QueryBuilder, tk *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Type = value.NewValueTypeFromUserType(tk.Data)
    return nil
}

func parseDbFn(qb *QueryBuilder, _ *tokens.Token) error {
    qb.qu[len(qb.qu)-1].QueryDbInstr = true
    return nil
}

func parseFieldKeyFn(qb *QueryBuilder, tk *tokens.Token) error {
    newField := QueryField{
        Name: tk.Data,
    }

    qb.qu[len(qb.qu)-1].Fields = append(qb.qu[len(qb.qu)-1].Fields, newField)
    return nil
}

func parseReturningFieldKeyFn(qb *QueryBuilder, tk *tokens.Token) error {
    qb.qu[len(qb.qu)-1].Returning = append(qb.qu[len(qb.qu)-1].Returning, tk.Data)
    return nil
}

func parseTypeFn(qb *QueryBuilder, tk *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Type = value.NewValueTypeFromUserType(tk.Data)
    return nil
}

func parseNullableTypeFn(qb *QueryBuilder, _ *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Nullable = true
    return nil
}

func parseAnnotationFn(qb *QueryBuilder, tk *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Annotations = append(fields[len(fields)-1].Annotations, tk.Data)
    return nil
}

func parseValueFn(qb *QueryBuilder, tk *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Value = tk.Data
    return nil
}

func parseFkeyFn(qb *QueryBuilder, _ *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Foreign = true
    return nil
}

func parseFkeyPathFn(qb *QueryBuilder, tk *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].ForeignPath = tk.Data
    return nil
}

func parseCompositeTypeFn(qb *QueryBuilder, tk *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Type = value.NewValueTypeFromUserType(tk.Data)
    return nil
}

func parseNumberFn(qb *QueryBuilder, tk *tokens.Token) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    // char(len) has a max length of 255
    length, convErr := strconv.ParseUint(tk.Data, 10, 8)

    if convErr != nil {
        return fmt.Errorf("expected a number from [0, 255] but got \"%s\"", tk.Data)
    }

    fields[len(fields)-1].Length = uint8(length)
    return nil
}

func parseEraseTableNameFn(qb *QueryBuilder, tk *tokens.Token) error {
    qb.qu[len(qb.qu)-1].QueryInstrName = tk.Data
    return nil
}

func parseSelectorFn(qb *QueryBuilder, _ *tokens.Token) error {
    qb.qu[len(qb.qu)-1].Filter = NewQueryFilter()
    return nil
}

func parseSelectorOpenBranchFn(qb *QueryBuilder, _ *tokens.Token) error {
    qb.qu[len(qb.qu)-1].Filter = NewQueryFilter()
    return nil
}


func selectorPushTokenFn(qb *QueryBuilder, tk *tokens.Token) error {
    pushErr := qb.qu[len(qb.qu)-1].Filter.Push(tk)
    if pushErr != nil {
        return pushErr
    }

    return nil
}

func parseBeginStepFn(qb *QueryBuilder, tk *tokens.Token) error {
    if len(qb.qu) < 1 {
        return nil
    }

    filter := qb.qu[len(qb.qu)-1].Filter
    if filter != nil {
        finErr := filter.Load()
        if finErr != nil {
            return finErr
        }
    }

    return nil
}

func parseOrderingKey(qb *QueryBuilder, tk *tokens.Token) error {
    qb.qu[len(qb.qu)-1].OrderedBy = tk.Data
    return nil
}


var defaultParseFnTable map[StepType]ParseFn = map[StepType]ParseFn{
    FsmBeginStep: parseBeginStepFn,
    FsmCreate: parseCreateFn,
    FsmRetrieve: parseRetrieveFn,
    FsmInsertStep: parseInsertFn,
    FsmDb: parseDbFn,
    FsmTableName: parseTableNameFn,
    FsmFieldKey: parseFieldKeyFn,
    FsmFieldType: parseFieldTypeFn,
    FsmFieldCompositeType: parseCompositeTypeFn,
    FsmNumber: parseNumberFn,
    FsmFieldNullable: parseNullableTypeFn,
    FsmFieldValue: parseValueFn,
    FsmFieldAnnotation: parseAnnotationFn,
    FsmFieldFkey: parseFkeyFn,
    FsmFieldFkeyPath: parseFkeyPathFn,
    FsmRetrieveTableName: parseTableNameFn,
    FsmRetrieveAll: parseFieldKeyFn,
    FsmReturningFieldKey: parseReturningFieldKeyFn,
    FsmSelector: parseSelectorFn,
    FsmSelectorOpenBranch: selectorPushTokenFn,
    FsmSelectorKey: selectorPushTokenFn,
    FsmSelectorCmp: selectorPushTokenFn,
    FsmSelectorValue: selectorPushTokenFn,
    FsmSelectorNexus: selectorPushTokenFn,
    FsmSelectorCloseBranch: selectorPushTokenFn,
    FsmErase: parseEraseFn,
    FsmOrderingKey: parseOrderingKey,

    FsmChange: parseChangeFn,
}



