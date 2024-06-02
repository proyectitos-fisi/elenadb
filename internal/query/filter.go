package query


type filternode struct {
    children []*filternode
    data interface{}
}



