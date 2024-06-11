package query

type QueryInstrType uint8
const (
    queryCreate QueryInstrType = iota
    queryRetrieve
    queryInsert
    queryDelete
    queryUpdate
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

