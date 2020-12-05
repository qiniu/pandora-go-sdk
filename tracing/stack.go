package tracing

import (
	"context"
	"sync"
)

type element struct {
	data context.Context
	next *element
}

type stack struct {
	lock *sync.Mutex
	head *element
	Size int
}

func (stk *stack) Push(data context.Context) {
	element := new(element)
	element.data = data
	temp := stk.head
	element.next = temp
	stk.head = element
	stk.Size++
}

func (stk *stack) Pop() context.Context {
	if stk.head == nil {
		return nil
	}
	r := stk.head.data
	stk.head = stk.head.next
	stk.Size--
	return r
}

func (stk *stack) Top() context.Context {
	if stk.head == nil {
		return nil
	}
	r := stk.head.data
	return r
}

func New() *stack {
	stk := new(stack)
	stk.lock = &sync.Mutex{}
	return stk
}
