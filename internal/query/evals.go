package query

import "fisi/elenadb/internal/tokens"

type EvalFn func(*tokens.Token) bool

func evalFieldTypeFn(tk *tokens.Token) bool {
    _, ok :=  isBasicTypeMap[tk.Data]
    return ok
}

func evalCompositeFieldTypeFn(tk *tokens.Token) bool {
    _, ok :=  isCompositeTypeMap[tk.Data]
    return ok
}


var defaultEvalFnTable map[StepType]EvalFn = map[StepType]EvalFn{
    FsmCreateStep: nil,
    FsmRetrieveStep: nil,
    FsmInsertStep: nil,
    FsmDb: nil,
    FsmName: nil,
    FsmFieldKey: nil,
    FsmFieldType: evalFieldTypeFn,
    FsmFieldCompositeType: evalCompositeFieldTypeFn,
    FsmNumber: nil,
    FsmFieldNullable: nil,
    FsmFieldValue: nil,
    FsmFieldAnnotation: nil,
    FsmFieldFkey: nil,
    FsmFieldFkeyPath: nil,
    FsmRetrieveFromSome: nil,
    FsmRetrieveFields: nil,
}
