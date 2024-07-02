package tokens

import "fmt"

type tkNode struct {
    data Token
    child *tkNode
    paren *tkNode
}

type TkStack struct {
    tail *tkNode
    size int
    aux  *tkNode
}

func (stck *TkStack) Push(tk Token) error {
    node := &tkNode{
        data: tk,
        child: nil,
        paren: stck.tail,
    }

    if stck.tail == nil {
        stck.tail = node
        stck.size++
        return nil
    }

    stck.tail.child = node
    stck.tail = node
    stck.aux = stck.tail

    stck.size++
    return nil
}

func (stck *TkStack) Pop() (Token, error) {
    if stck.tail == nil {
        return Token{}, fmt.Errorf("tkstack: empty stack")
    }

    tk := stck.tail.data

    stck.tail = stck.tail.paren

    if stck.tail != nil {
        stck.tail.child = nil
    }

    stck.size--
    return tk, nil
}

func (stck *TkStack) NdPop() (Token, error) {
    if stck.aux == nil {
        return Token{}, fmt.Errorf("tkstack: empty stack")
    }

    tk := stck.aux.data
    stck.aux = stck.aux.paren
    return tk, nil
}

func (stck *TkStack) Peek() (Token, error) {
    if stck.Len() == 0 {
        return Token{}, fmt.Errorf("tkstack: empty stack")
    }

    return stck.tail.data, nil
}

func (stck *TkStack) Len() int {
    return stck.size
}

func (stck *TkStack) GetAll() []Token {
    r := []Token{}
    cursor := stck.tail
    for cursor != nil {
        r = append(r, cursor.data)
        cursor = cursor.paren
    }

    return r
}


