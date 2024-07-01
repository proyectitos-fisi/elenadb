package query

import (
	"fisi/elenadb/internal/tokens"
)

type EvalFn func(*tokens.Token) bool

func evalBeginStep(_ *tokens.Token) bool {
    return true
}

func evalFieldTypeFn(tk *tokens.Token) bool {
    _, ok :=  isBasicTypeMap[tk.Data]
    return ok
}

func evalCompositeFieldTypeFn(tk *tokens.Token) bool {
    _, ok :=  isCompositeTypeMap[tk.Data]
    return ok
}

func evalSelectorNexusFn(tk *tokens.Token) bool {
    return tk.Data == "y" || tk.Data == "o"
}

func evalSelectorKeyFn(tk *tokens.Token) bool {
    return !isKeyword(tk)
}

var defaultEvalFnTable map[StepType]EvalFn = map[StepType]EvalFn{
    FsmBeginStep: nil,
    FsmCreate: nil,
    FsmRetrieve: nil,
    FsmInsertStep: nil,
    FsmDb: nil,
    FsmTableName: nil,
    FsmFieldKey: nil,
    FsmFieldType: evalFieldTypeFn,
    FsmFieldCompositeType: evalCompositeFieldTypeFn,
    FsmNumber: nil,
    FsmFieldNullable: nil,
    FsmFieldValue: nil,
    FsmFieldAnnotation: nil,
    FsmFieldFkey: nil,
    FsmFieldFkeyPath: nil,
    FsmRetrieveTableName: nil,
    FsmRetrieveAll: nil,
    FsmSelector: nil,
    FsmSelectorOpenBranch: nil,
    FsmSelectorKey: nil,
    FsmSelectorCmp: nil,
    FsmSelectorValue: nil,
    FsmSelectorNexus: evalSelectorNexusFn,
    FsmSelectorCloseBranch: nil,
    FsmSelectorEos: nil,
    FsmErase: nil,
    FsmEraseFrom: nil,
    FsmEraseTableName: nil,
}
