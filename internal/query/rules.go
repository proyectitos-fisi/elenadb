package query

import "fisi/elenadb/internal/tokens"

type StepType uint16
const (
    FsmBeginStep StepType = iota

    FsmEos

    FsmCreateStep
    FsmRetrieveStep
    FsmInsertStep
    FsmDeleteStep
    FsmChangeStep

    FsmOpenSelector
    FsmCloseSelector

    FsmTable
    FsmDb

    FsmName
    FsmListSeparator
    FsmValueAssign

    FsmOpenList
    FsmCloseList

    FsmFieldKey
    FsmFieldType
    FsmFieldCompositeType
    FsmFieldNullable
    FsmFieldValue
    FsmFieldAnnotation

    FsmNumber

    FsmFieldFkey
    FsmFieldFkeyPath

    FsmRetrieveFrom
    FsmRetrieveFromSome
    FsmRetrieveFields

    FsmInsertAt
)

type FsmNode struct {
    Step           StepType
    ExpectByType   bool
    ExpectedString string
    ExpectedType   tokens.TkType
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
    if fsm.ExpectByType {
        if fsm.ExpectedType != tk.Type {
            return false
        }
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
    createTableFieldKey := &FsmNode{
        Step: FsmFieldKey,
        ExpectByType: true,
        ExpectedType: tokens.TkWord,
        Children: map[StepType]*FsmNode{},
    }

    createTableFieldType := &FsmNode{
        Step: FsmFieldType,
        ExpectByType: true,
        ExpectedType: tokens.TkWord,
        Children: map[StepType]*FsmNode{},
    }

    createTableFieldCompositeType := &FsmNode{
        Step: FsmFieldCompositeType,
        ExpectByType: true,
        ExpectedType: tokens.TkWord,
        Children: map[StepType]*FsmNode{},
    }

    createTableNullable := &FsmNode{
        Step: FsmFieldNullable,
        ExpectedString: "?",
        Children: map[StepType]*FsmNode{},
    }

    createTableAnnotation := &FsmNode{
        Step: FsmFieldAnnotation,
        ExpectByType: true,
        ExpectedType: tokens.TkAnnotation,
        Children: map[StepType]*FsmNode{},
    }

    createTableEos := &FsmNode{
        Step: FsmEos,
        ExpectedString: ",",
        Children: map[StepType]*FsmNode{},
    }

    createTableCloseList := &FsmNode{
        Step: FsmCloseList,
        ExpectedType: tokens.TkBracketClosed,
        ExpectedString: "}",
        ExpectByType: true,
        Children: map[StepType]*FsmNode{},
    }

    retrieveTableFrom := &FsmNode{
        Step: FsmRetrieveFrom,
        ExpectedString: "de",
        Children: map[StepType]*FsmNode{},
    }

    retrieveFieldKey := &FsmNode{
        ExpectByType: true,
        ExpectedType: tokens.TkWord,
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    insertFieldKey := &FsmNode{
        ExpectByType: true,
        ExpectedType: tokens.TkWord,
        ExpectedString: "",
        Children: map[StepType]*FsmNode{},
    }

    beginStep := NewFsm()
    beginStep.
        // creame db
        AddRule(&FsmNode{
            ExpectedString: "creame",
        }, FsmCreateStep).
        AddRule(&FsmNode{
            ExpectedString: "db",
        }, FsmCreateStep, FsmDb).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedString: "",
            ExpectedType: tokens.TkWord,
        }, FsmCreateStep, FsmDb, FsmName).
        AddRule(beginStep,
            FsmCreateStep, FsmDb, FsmName, FsmBeginStep).
        // creame tabla
        AddRule(&FsmNode{
            ExpectedString: "tabla",
        }, FsmCreateStep, FsmTable).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedType: tokens.TkWord,
            ExpectedString: "",
        }, FsmCreateStep, FsmTable, FsmName).
        AddRule(&FsmNode{
            ExpectedString: "{",
        }, FsmCreateStep, FsmTable, FsmName, FsmOpenList).
        AddRule(createTableFieldKey,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey).
        // composite types
        AddRule(createTableFieldCompositeType,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType).
        AddRule(createTableFieldCompositeType,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType).
        AddRule(createTableNullable,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmFieldNullable).
        AddRule(createTableAnnotation,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmFieldAnnotation).
        AddRule(createTableEos,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmEos).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedType: tokens.TkParenOpen,
        }, FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedType: tokens.TkWord,
            ExpectedString: "",
        }, FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedType: tokens.TkParenClosed,
        }, FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber, FsmCloseSelector).
        AddRule(createTableNullable,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber, FsmCloseSelector, FsmFieldNullable).
        AddRule(createTableAnnotation,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber, FsmCloseSelector, FsmFieldAnnotation).
        AddRule(createTableEos,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldCompositeType, FsmOpenSelector, FsmNumber, FsmCloseSelector, FsmEos).
        // regular/basic types
        AddRule(createTableFieldType,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType).
        AddRule(createTableNullable,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldNullable).
        AddRule(createTableAnnotation,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldAnnotation).
        AddRule(createTableAnnotation,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldNullable, FsmFieldAnnotation).
        AddRule(createTableEos,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldNullable, FsmEos).
        AddRule(createTableEos,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldAnnotation, FsmEos).
        AddRule(createTableAnnotation,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmFieldAnnotation, FsmFieldAnnotation).
        AddRule(createTableEos,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmEos).
        AddRule(createTableFieldKey,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmEos, FsmFieldKey).
        AddRule(createTableCloseList,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmEos, FsmCloseList).
        AddRule(beginStep,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldType, FsmEos, FsmCloseList, FsmBeginStep).
        AddRule(&FsmNode{
            ExpectedString: "fkey",
        }, FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldFkey).
        AddRule(&FsmNode{
            ExpectedString: "(",
        }, FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedType: tokens.TkWord,
            ExpectedString: "",
        }, FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath).
        AddRule(&FsmNode{
            ExpectedString: ")",
        }, FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath, FsmCloseSelector).
        AddRule(createTableNullable,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath, FsmCloseSelector, FsmFieldNullable).
        AddRule(createTableAnnotation,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath, FsmCloseSelector, FsmFieldAnnotation).
        AddRule(createTableEos,
            FsmCreateStep, FsmTable, FsmName, FsmOpenList, FsmFieldKey, FsmFieldFkey, FsmOpenSelector, FsmFieldFkeyPath, FsmCloseSelector, FsmEos).
        // 'dame'
        AddRule(&FsmNode{
            ExpectedString: "dame",
        }, FsmRetrieveStep).
        AddRule(&FsmNode{
            ExpectedString: "todo",
        }, FsmRetrieveStep, FsmRetrieveFields).
        AddRule(retrieveTableFrom,
            FsmRetrieveStep, FsmRetrieveFields, FsmRetrieveFrom).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedType: tokens.TkWord,
            ExpectedString: "",
        }, FsmRetrieveStep, FsmRetrieveFields, FsmRetrieveFrom, FsmRetrieveFromSome).
        AddRule(beginStep,
            FsmRetrieveStep, FsmRetrieveFields, FsmRetrieveFrom, FsmRetrieveFromSome, FsmBeginStep).
        AddRule(&FsmNode{
            ExpectedString: "{",
        }, FsmRetrieveStep, FsmOpenList).
        AddRule(retrieveFieldKey,
            FsmRetrieveStep, FsmOpenList, FsmFieldKey).
        AddRule(&FsmNode{
            ExpectedString: ",",
        }, FsmRetrieveStep, FsmOpenList, FsmFieldKey, FsmListSeparator).
        AddRule(retrieveFieldKey,
            FsmRetrieveStep, FsmOpenList, FsmFieldKey, FsmListSeparator, FsmFieldKey).
        AddRule(&FsmNode{
            ExpectedString: "}",
        }, FsmRetrieveStep, FsmOpenList, FsmFieldKey, FsmCloseList).
        AddRule(retrieveTableFrom,
            FsmRetrieveStep, FsmOpenList, FsmFieldKey, FsmCloseList, FsmRetrieveFrom).
        // 'mete' instruction
        AddRule(&FsmNode{
            ExpectedString: "mete",
        }, FsmInsertStep).
        AddRule(&FsmNode{
            ExpectedString: "{",
        }, FsmInsertStep, FsmOpenList).
        AddRule(insertFieldKey,
            FsmInsertStep, FsmOpenList, FsmFieldKey).
        AddRule(&FsmNode{
            ExpectedString: ":",
        }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedType: tokens.TkWord,
            ExpectedString: "",
        }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue).
        AddRule(&FsmNode{
            ExpectedString: ",",
        }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmListSeparator).
        AddRule(insertFieldKey,
            FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmListSeparator, FsmFieldKey).
        AddRule(&FsmNode{
            ExpectedString: "}",
        }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList).
        AddRule(&FsmNode{
            ExpectedString: "en",
        }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt).
        AddRule(&FsmNode{
            ExpectByType: true,
            ExpectedType: tokens.TkWord,
            ExpectedString: "",
        }, FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmName).
        AddRule(beginStep,
            FsmInsertStep, FsmOpenList, FsmFieldKey, FsmValueAssign, FsmFieldValue, FsmCloseList, FsmInsertAt, FsmName, FsmBeginStep)

    return beginStep
}
