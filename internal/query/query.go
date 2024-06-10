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
    Name        string
    QueryType   string
    Value       interface{}
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
    Filter         *QueryFilter
}

