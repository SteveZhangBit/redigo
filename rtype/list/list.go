package list

import "container/list"

const (
	ListTail = 0
	ListHead = 1
)

type Element list.Element

type LinkedList struct {
	list.List
}

func New() *LinkedList {
	l := &LinkedList{}
	l.Init()
	return l
}

// Return the element with the value.
func (l *LinkedList) SearchKey(v interface{}) *Element {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == v {
			return (*Element)(e)
		}
	}
	return nil
}

// Return the element at that index.
func (l *LinkedList) Index(n int) *Element {
	e := l.Front()
	for i := 0; e != nil && i < n; i++ {
		e = e.Next()
	}
	return (*Element)(e)
}

// Pop the tail of the list and push it to the front.
func (l *LinkedList) Rotate() {
	tail := l.Back()
	l.Remove(tail)
	l.PushFront(tail)
}

func (l *LinkedList) PopFront() *Element {
	e := l.Front()
	if e != nil {
		l.Remove(e)
	}
	return (*Element)(e)
}

func (l *LinkedList) PopBack() *Element {
	e := l.Back()
	if e != nil {
		l.Remove(e)
	}
	return (*Element)(e)
}
