package query

import (
	"fisi/elenadb/pkg/storage/table/value"
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

type ParseFn func(*QueryBuilder, string) error

func parseCreateFn(qu *QueryBuilder, _ string) error {
    qu.PushInstr(QueryCreate)
    return nil
}

func parseRetrieveFn(qb *QueryBuilder, _ string) error {
    qb.PushInstr(QueryRetrieve)
    return nil
}

func parseInsertFn(qb *QueryBuilder, _ string) error {
    qb.PushInstr(QueryInsert)
    return nil
}

func parseTableNameFn(qb *QueryBuilder, data string) error {
    qb.qu[len(qb.qu)-1].QueryInstrName = data
    return nil
}

func parseFieldTypeFn(qb *QueryBuilder, data string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Type = value.NewValueTypeFromUserType(data)
    return nil
}

func parseDbFn(qb *QueryBuilder, _ string) error {
    qb.qu[len(qb.qu)-1].QueryDbInstr = true
    return nil
}

func parseFieldKeyFn(qb *QueryBuilder, data string) error {
    newField := QueryField{
        Name: data,
    }

    qb.qu[len(qb.qu)-1].Fields = append(qb.qu[len(qb.qu)-1].Fields, newField)
    return nil
}

func parseTypeFn(qb *QueryBuilder, data string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Type = value.NewValueTypeFromUserType(data)
    return nil
}

func parseNullableTypeFn(qb *QueryBuilder, data string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Nullable = true
    return nil
}

func parseAnnotationFn(qb *QueryBuilder, data string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Annotations = append(fields[len(fields)-1].Annotations, data)
    return nil
}

func parseValueFn(qb *QueryBuilder, data string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Value = data
    return nil
}

func parseFkeyFn(qb *QueryBuilder, _ string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Foreign = true
    return nil
}

func parseFkeyPathFn(qb *QueryBuilder, data string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].ForeignPath = data
    return nil
}

func parseCompositeTypeFn(qb *QueryBuilder, data string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    fields[len(fields)-1].Type = value.NewValueTypeFromUserType(data)
    return nil
}

func parseNumberFn(qb *QueryBuilder, data string) error {
    fields := qb.qu[len(qb.qu)-1].Fields
    length, convErr := strconv.ParseUint(data, 10, 8)
    if convErr != nil {
        return convErr
    }

    fields[len(fields)-1].Length = uint8(length)
    return nil
}

var defaultParseFnTable map[StepType]ParseFn = map[StepType]ParseFn{
    FsmCreateStep: parseCreateFn,
    FsmRetrieveStep: parseRetrieveFn,
    FsmInsertStep: parseInsertFn,
    FsmDb: parseDbFn,
    FsmName: parseTableNameFn,
    FsmFieldKey: parseFieldKeyFn,
    FsmFieldType: parseFieldTypeFn,
    FsmFieldCompositeType: parseCompositeTypeFn,
    FsmNumber: parseNumberFn,
    FsmFieldNullable: parseNullableTypeFn,
    FsmFieldValue: parseValueFn,
    FsmFieldAnnotation: parseAnnotationFn,
    FsmFieldFkey: parseFkeyFn,
    FsmFieldFkeyPath: parseFkeyPathFn,
    FsmRetrieveFromSome: parseTableNameFn,
    FsmRetrieveFields: parseFieldKeyFn,
}



