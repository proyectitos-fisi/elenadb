package query

import (
	"fisi/elenadb/internal/tokens"
)

type StepType uint16
const (
    FsmBeginStep StepType = iota

    FsmTableName
    FsmListSeparator
    FsmValueAssign
    FsmOpenList
    FsmCloseList
    FsmOpenSelector
    FsmCloseSelector
    FsmEos

    FsmFieldKey
    FsmFieldType
    FsmFieldCompositeType
    FsmFieldNullable
    FsmFieldValue
    FsmFieldAnnotation
    FsmFieldFkey
    FsmFieldFkeyPath
    FsmNumber

    FsmTable
    FsmDb

    FsmCreate

    FsmReturningKey
    FsmReturningFieldKey

    FsmRetrieve
    FsmRetrieveFrom
    FsmRetrieveTableName
    FsmRetrieveAll

    FsmOrdering
    FsmOrderingBy
    FsmOrderingKey
    FsmOrderingDirectionAsc
    FsmOrderingDirectionDesc

    FsmChange
    FsmChangeAt

    FsmInsertStep
    FsmInsertAt

    FsmSelector
    FsmSelectorOpenBranch
    FsmSelectorCloseBranch
    FsmSelectorKey
    FsmSelectorCmp
    FsmSelectorValue
    FsmSelectorNexus

    FsmErase
    FsmEraseFrom
)


type FsmNode struct {
    Step           StepType
    ExpectByTypes   bool
    ExpectedString string
    ExpectedTypes   []tokens.TkType
    Eof            bool
    Children       map[StepType]*FsmNode
}

var isBasicTypeMap = map[string]struct{}{
    "int": {},
    "float": {},
    "bool": {},
}

var isCompositeTypeMap = map[string]struct{}{
    "char": {},
}

func isKeyword(tk *tokens.Token) bool {
    _, basicok := isBasicTypeMap[tk.Data]
    if basicok {
        return true
    }

    _, comok := isCompositeTypeMap[tk.Data]
    if comok {
        return true
    }

    if tk.Data == "fkey" {
        return true
    }

    if tk.Data == "de" {
        return true
    }

    if tk.Data == "pe" {
        return true
    }

    return false
}

func (fsm *FsmNode) Eval(tk *tokens.Token) bool {
    if fsm.ExpectByTypes {
        for index := range fsm.ExpectedTypes {
            if fsm.ExpectedTypes[index] == tk.Type {
                return true
            }
        }

        return false
    }

    if fsm.Step == FsmFieldType {
        _, ok := isBasicTypeMap[tk.Data]
        return ok
    }

    if fsm.Step == FsmFieldCompositeType {
        _, ok := isCompositeTypeMap[tk.Data]
        return ok
    }

    if len(fsm.ExpectedString) == 0 {
        return !isKeyword(tk)
    }

    return (fsm.ExpectedString == tk.Data)
}

func NewFsm() *FsmNode {
    return &FsmNode{
        Step: FsmBeginStep,
        ExpectedString: "pe",
        Eof: true,
        Children: map[StepType]*FsmNode{},
    }
}

func (fsm *FsmNode) AddRule(node *FsmNode, sts ...StepType) *FsmNode {
    cursor := fsm
    ok := false

    if node.Children == nil {
        node.Children = map[StepType]*FsmNode{}
    }

    for index := range sts {
        _, ok = cursor.Children[sts[index]]
        node.Step = sts[index]

        if !ok {
            cursor.Children[sts[index]] = node
            return fsm
        } else {
            cursor = cursor.Children[sts[index]]
        }
    }

    // in case the objective is to edit an existing step
    // at the same path
    *cursor = *node
    return fsm
}

func defaultParseFsm() *FsmNode {
    beginStep := &FsmNode{
        Step: FsmBeginStep,
        ExpectedString: "pe",
        Eof: true,
        Children: map[StepType]*FsmNode{},
    }

    // fsm selector-specific nodes
    selector := &FsmNode{
        Step: FsmSelector,
        ExpectByTypes: false,
        ExpectedString: "donde",
        Children: map[StepType]*FsmNode{},
    }

    selectorOpenBranch := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkParenOpen,
        },
        Children: map[StepType]*FsmNode{},
    }

    selectorKey := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        Children: map[StepType]*FsmNode{},
    }

    selectorCmp := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkBoolOp,
        },
        Children: map[StepType]*FsmNode{},
    }

    selectorValue := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
            tokens.TkString,
        },
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    selectorNexus := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    selectorCloseBranch := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkParenClosed,
        },
        Children: map[StepType]*FsmNode{},
    }

    selector.AddRule(selectorOpenBranch, FsmSelectorOpenBranch)
    selector.AddRule(selectorKey, FsmSelectorKey)

    selectorOpenBranch.AddRule(selectorOpenBranch, FsmSelectorOpenBranch)
    selectorOpenBranch.AddRule(selectorKey, FsmSelectorKey)

    selectorKey.AddRule(selectorCmp, FsmSelectorCmp)

    selectorCmp.AddRule(selectorValue, FsmSelectorValue)

    selectorValue.AddRule(selectorCloseBranch, FsmSelectorCloseBranch)
    selectorValue.AddRule(selectorNexus, FsmSelectorNexus)

    selectorCloseBranch.AddRule(selectorCloseBranch, FsmSelectorCloseBranch)
    selectorCloseBranch.AddRule(beginStep, FsmBeginStep)
    selectorCloseBranch.AddRule(selectorNexus, FsmSelectorNexus)

    selectorNexus.AddRule(selectorKey, FsmSelectorKey)
    selectorNexus.AddRule(selectorOpenBranch, FsmSelectorOpenBranch)

    // fsm creame-specific rules
    createTableFieldKey := &FsmNode{
        Step: FsmFieldKey,
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        Children: map[StepType]*FsmNode{},
    }

    createTableFieldType := &FsmNode{
        Step: FsmFieldType,
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        Children: map[StepType]*FsmNode{},
    }

    createTableFieldCompositeType := &FsmNode{
        Step: FsmFieldCompositeType,
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        Children: map[StepType]*FsmNode{},
    }

    createTableNullable := &FsmNode{
        Step: FsmFieldNullable,
        ExpectedString: "?",
        Children: map[StepType]*FsmNode{},
    }

    createTableAnnotation := &FsmNode{
        Step: FsmFieldAnnotation,
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkAnnotation,
        },
        Children: map[StepType]*FsmNode{},
    }

    createTableEos := &FsmNode{
        Step: FsmEos,
        ExpectedString: ",",
        Children: map[StepType]*FsmNode{},
    }

    createTableCloseList := &FsmNode{
        Step: FsmCloseList,
        ExpectedTypes: []tokens.TkType{
            tokens.TkBracketClosed,
        },
        ExpectedString: "}",
        ExpectByTypes: true,
        Children: map[StepType]*FsmNode{},
    }
    // fsm create-specific rules
    beginStep.
    AddRule(&FsmNode{
        ExpectedString: "creame",
    }, FsmCreate).
    AddRule(&FsmNode{
        ExpectedString: "db",
    }, FsmCreate, FsmDb).
    AddRule(&FsmNode{
        ExpectByTypes: true,
        ExpectedString: "",
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
    }, FsmCreate, FsmDb, FsmTableName).
    AddRule(beginStep, FsmCreate, FsmDb, FsmTableName, FsmBeginStep).
    // creame tabla
    AddRule(&FsmNode{
        ExpectedString: "tabla",
    }, FsmCreate, FsmTable).
    AddRule(&FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
    }, FsmCreate, FsmTable, FsmTableName).
    AddRule(&FsmNode{
        ExpectedString: "{",
    }, FsmCreate, FsmTable, FsmTableName, FsmOpenList).
    AddRule(createTableFieldKey, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey).
    // composite types
    AddRule(createTableFieldCompositeType, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType).
    AddRule(createTableFieldCompositeType, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType).
    AddRule(createTableNullable, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmFieldNullable).
    AddRule(createTableAnnotation, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmFieldAnnotation).
    AddRule(createTableEos, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmEos).
    AddRule(&FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkParenOpen,
        },
    }, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector).
    AddRule(&FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
    }, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber).
    AddRule(&FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkParenClosed,
        },
    }, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber, FsmCloseSelector).
    AddRule(createTableNullable, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber, FsmCloseSelector, FsmFieldNullable).
    AddRule(createTableAnnotation, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber, FsmCloseSelector, FsmFieldAnnotation).
    AddRule(createTableEos, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber, FsmCloseSelector, FsmEos).
    // regular/basic types
    AddRule(createTableFieldType, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType).
    AddRule(createTableNullable, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldNullable).
    AddRule(createTableAnnotation, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldAnnotation).
    AddRule(createTableAnnotation, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldNullable, FsmFieldAnnotation).
    AddRule(createTableEos, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldNullable, FsmEos).
    AddRule(createTableEos, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldAnnotation, FsmEos).
    AddRule(createTableAnnotation, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldAnnotation, FsmFieldAnnotation).
    AddRule(createTableEos, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmEos).
    AddRule(createTableFieldKey, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmEos, FsmFieldKey).
    AddRule(createTableCloseList, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmEos, FsmCloseList).
    AddRule(beginStep, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmEos, FsmCloseList, FsmBeginStep).
    AddRule(&FsmNode{
        ExpectedString: "fkey",
    }, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldFkey).
    AddRule(&FsmNode{
        ExpectedString: "(",
    }, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector).
    AddRule(&FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
    }, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath).
    AddRule(&FsmNode{
        ExpectedString: ")",
    }, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath, FsmCloseSelector).
    AddRule(createTableNullable, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath, FsmCloseSelector, FsmFieldNullable).
    AddRule(createTableAnnotation, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath, FsmCloseSelector, FsmFieldAnnotation).
    AddRule(createTableEos, FsmCreate, FsmTable, FsmTableName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath, FsmCloseSelector, FsmEos)

    // fsm dame-specific rules
    retrieve := &FsmNode{
        ExpectedString: "dame",
    }

    retrieveFrom := &FsmNode{
        ExpectedString: "de",
        Children: map[StepType]*FsmNode{},
    }

    retrieveTableName := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
    }

    retrieveFieldKey := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    retrieveOrdering := &FsmNode{
        ExpectedString: "ordenado",
        Children: map[StepType]*FsmNode{},
    }

    retrieveOrderingBy := &FsmNode{
        ExpectedString: "por",
        Children: map[StepType]*FsmNode{},
    }

    retrieveOrderingKey := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    beginStep.
    AddRule(retrieve, FsmRetrieve).
    AddRule(&FsmNode{
        ExpectedString: "todo",
    }, FsmRetrieve, FsmRetrieveAll).
    AddRule(retrieveFrom, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom).
    AddRule(retrieveTableName, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName).
    AddRule(beginStep, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmBeginStep).
    AddRule(selector, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmSelector).
    AddRule(&FsmNode{
        ExpectedString: "{",
    }, FsmRetrieve, FsmOpenList).
    AddRule(retrieveFieldKey, FsmRetrieve, FsmOpenList, FsmFieldKey).
    AddRule(&FsmNode{
        ExpectedString: ",",
    }, FsmRetrieve, FsmOpenList, FsmFieldKey, FsmListSeparator).
    AddRule(retrieveFieldKey, FsmRetrieve, FsmOpenList, FsmFieldKey, FsmListSeparator, FsmFieldKey).
    AddRule(&FsmNode{
        ExpectedString: "}",
    }, FsmRetrieve, FsmOpenList, FsmFieldKey, FsmCloseList).
    AddRule(retrieveFrom, FsmRetrieve, FsmOpenList, FsmFieldKey, FsmCloseList, FsmRetrieveFrom).
    AddRule(retrieveOrdering, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering).
    AddRule(retrieveOrderingBy, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering, FsmOrderingBy).
    AddRule(retrieveOrderingKey, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering, FsmOrderingBy, FsmOrderingKey).
    // expect "asc" or "desc"
    AddRule(&FsmNode{
        ExpectedString: "desc",
    }, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering, FsmOrderingBy, FsmOrderingKey, FsmOrderingDirectionDesc).
    AddRule(&FsmNode{
        ExpectedString: "asc",
    }, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering, FsmOrderingBy, FsmOrderingKey, FsmOrderingDirectionAsc).
    AddRule(beginStep, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering, FsmOrderingBy, FsmOrderingKey, FsmOrderingDirectionAsc, FsmBeginStep).
    AddRule(beginStep, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering, FsmOrderingBy, FsmOrderingKey, FsmOrderingDirectionDesc, FsmBeginStep).
    AddRule(beginStep, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering, FsmOrderingBy, FsmOrderingKey, FsmBeginStep).
    AddRule(selector, FsmRetrieve, FsmRetrieveAll, FsmRetrieveFrom, FsmRetrieveTableName, FsmOrdering, FsmOrderingBy, FsmOrderingKey, FsmSelector)

    // fsm cambia-specific rules
    change := &FsmNode{
        ExpectedString: "cambia",
    }

    changeAt := &FsmNode{
        ExpectedString: "en",
    }

    changeTableName := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
    }

    changeOpenList := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkBracketOpen,
        },
        ExpectedString: "{",
    }

    changeCloseList := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkBracketClosed,
        },
        ExpectedString: "{",
    }

    changeFieldKey := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
    }

    changeValueAssign := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkValueIndicator,
        },
    }

    changeFieldValue := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
            tokens.TkString,
        },
    }

    changeSeparator := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkSeparator,
        },
    }

    beginStep.
    AddRule(change, FsmChange).
    AddRule(changeAt, FsmChange, FsmChangeAt).
    AddRule(changeTableName, FsmChange, FsmChangeAt, FsmTableName).
    AddRule(changeOpenList, FsmChange, FsmChangeAt, FsmTableName, FsmOpenList).
    AddRule(changeFieldKey, FsmChange, FsmChangeAt, FsmTableName, FsmOpenList, FsmFieldKey).
    AddRule(changeValueAssign, FsmChange, FsmChangeAt, FsmTableName, FsmOpenList, FsmFieldKey, FsmValueAssign).
    AddRule(changeFieldValue, FsmChange, FsmChangeAt, FsmTableName, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue).
    AddRule(changeSeparator, FsmChange, FsmChangeAt, FsmTableName, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmListSeparator).
    AddRule(changeFieldKey, FsmChange, FsmChangeAt, FsmTableName, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmListSeparator, FsmFieldKey).
    AddRule(changeCloseList, FsmChange, FsmChangeAt, FsmTableName, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList).
    AddRule(selector, FsmChange, FsmChangeAt, FsmTableName, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmSelector)

    // fsm borra-specific rules
    erase := &FsmNode{
        ExpectedString: "borra",
        Children: map[StepType]*FsmNode{},
    }

    eraseFrom := &FsmNode{
        ExpectedString: "de",
        Children: map[StepType]*FsmNode{},
    }

    eraseTableName := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    beginStep.
    AddRule(erase, FsmErase).
    AddRule(eraseFrom, FsmErase, FsmEraseFrom).
    AddRule(eraseTableName, FsmErase, FsmEraseFrom, FsmTableName).
    AddRule(selector, FsmErase, FsmEraseFrom, FsmTableName, FsmSelector)

    // fsm mete-specific rules
    insertFieldKey := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    insertReturningFieldKey := &FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    meteCloseList := &FsmNode{
        ExpectedString: "}",
    }

    beginStep.
    AddRule(&FsmNode{
        ExpectedString: "mete",
    }, FsmInsertStep).
    AddRule(&FsmNode{
        ExpectedString: "{",
    }, FsmInsertStep, FsmOpenList).
    AddRule(insertFieldKey, FsmInsertStep, FsmOpenList, FsmFieldKey).
    AddRule(&FsmNode{
        ExpectedString: ":",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign).
    AddRule(&FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
            tokens.TkString,
        },
        ExpectedString: "",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue).
    AddRule(&FsmNode{
        ExpectedString: ",",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmListSeparator).
    AddRule(insertFieldKey, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmListSeparator, FsmFieldKey).
    AddRule(meteCloseList,
        FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList,
    ).
    AddRule(meteCloseList,
        FsmInsertStep, FsmOpenList, FsmCloseList,
    ).
    AddRule(&FsmNode{
        ExpectedString: "en",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt).
    AddRule(&FsmNode{
        ExpectByTypes: true,
        ExpectedTypes: []tokens.TkType{
            tokens.TkWord,
        },
        ExpectedString: "",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName).
    // "mete" queries without "retornando" end here
    AddRule(beginStep, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmBeginStep).
    // has the syntax: retornando {a,b,c} pe
    AddRule(&FsmNode{
        ExpectedString: "retornando",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey).
    AddRule(&FsmNode{
        ExpectedString: "{",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey, FsmOpenList).
    AddRule(insertReturningFieldKey, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey, FsmOpenList, FsmReturningFieldKey).
    AddRule(&FsmNode{
        ExpectedString: ",",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey, FsmOpenList, FsmReturningFieldKey, FsmListSeparator).
    AddRule(insertReturningFieldKey, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey, FsmOpenList, FsmReturningFieldKey, FsmListSeparator, FsmReturningFieldKey).
    AddRule(&FsmNode{
        ExpectedString: "}",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey, FsmOpenList, FsmReturningFieldKey, FsmCloseList).
    AddRule(&FsmNode{
        ExpectedString: "}",
    }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey, FsmOpenList, FsmReturningFieldKey, FsmListSeparator, FsmReturningFieldKey, FsmCloseList).
    AddRule(beginStep, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey, FsmOpenList, FsmReturningFieldKey, FsmListSeparator, FsmReturningFieldKey, FsmCloseList, FsmBeginStep).
    AddRule(beginStep, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmTableName, FsmReturningKey, FsmOpenList, FsmReturningFieldKey, FsmCloseList, FsmBeginStep)
    // "mete" queries with "retornando" ends here

    return beginStep
}
