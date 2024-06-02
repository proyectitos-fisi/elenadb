package query

type QueryInstrType uint8
const (
    Create QueryInstrType = iota
    Retrieve
    Insert
    Delete
    Update
)

type QueryField struct {
    Name  string
    Type  string
    Value interface{}
}

type QueryFilter struct {
    head *filternode
}

func (qf *QueryFilter) Exec(map[string]interface{}) bool

type Query struct {
    Type      QueryInstrType
    TableName string
    Fields    []QueryField
    Filter    *QueryFilter
}



