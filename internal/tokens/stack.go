package tokens

import "fmt"

type tkNode struct {
    data Token
    child *tkNode
    paren *tkNode
}

type TkStack struct {
    head *tkNode
    tail *tkNode
    size int
}

func (stck *TkStack) Push(tk Token) error {
    node := &tkNode{
        data: tk,
        child: nil,
        paren: nil,
    }

    if stck.head == nil {
        stck.head = node
        stck.tail = stck.head
    }

    node.paren = stck.tail
    stck.tail.child = node
    stck.tail = node

    stck.size++
    return nil
}

func (stck *TkStack) Pop() (Token, error) {
    if stck.Len() == 0 {
        return Token{}, fmt.Errorf("tkstack: empty stack")
    }

    tk := stck.tail.data
    stck.tail = stck.tail.paren
    stck.size--
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


