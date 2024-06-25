package query

type QueryInstrType string

const (
    QueryCreate QueryInstrType = "creame"
    QueryRetrieve QueryInstrType = "dame"
    QueryInsert QueryInstrType = "mete"
    QueryDelete QueryInstrType = "borra"
    QueryUpdate QueryInstrType = "cambia"
)

type QueryField struct {
    Foreign     bool
    Name        string
    Type        string
    Length      uint8
    Value       interface{}
    ForeignPath string
    Nullable    bool
    Annotations []string
}

type QueryFilter struct {}
func (qf *QueryFilter) Exec(map[string]interface{}) bool {
    return false
}

type Query struct {
    QueryType      QueryInstrType
    QueryTypeStr   string
    QueryInstrName string
    QueryDbInstr   bool
    Fields         []QueryField
    GetAllFields   bool
    Filter         *QueryFilter
}

