package list

import (
	"container/list"

	"github.com/SteveZhangBit/redigo/rtype"
)

type LLElement struct {
	e *list.Element
}

func (l *LLElement) Prev() rtype.ListElement {
	if e := l.e.Prev(); e != nil {
		return &LLElement{e: e}
	}
	return nil
}

func (l *LLElement) Next() rtype.ListElement {
	if e := l.e.Next(); e != nil {
		return &LLElement{e: e}
	}
	return nil
}

func (l *LLElement) Value() rtype.String {
	return l.e.Value.(rtype.String)
}

func (l *LLElement) SetValue(v rtype.String) {
	l.e.Value = v
}

type LinkedList struct {
	l *list.List
}

func (l *LinkedList) Front() rtype.ListElement {
	if e := l.l.Front(); e != nil {
		return &LLElement{e: e}
	}
	return nil
}

func (l *LinkedList) Back() rtype.ListElement {
	if e := l.l.Back(); e != nil {
		return &LLElement{e: e}
	}
	return nil
}

func (l *LinkedList) InsertAfter(v rtype.String, at rtype.ListElement) rtype.ListElement {
	if e := l.l.InsertAfter(v, at.(*LLElement).e); e != nil {
		return &LLElement{e: e}
	}
	return nil
}

func (l *LinkedList) InsertBefore(v rtype.String, at rtype.ListElement) rtype.ListElement {
	if e := l.l.InsertBefore(v, at.(*LLElement).e); e != nil {
		return &LLElement{e: e}
	}
	return nil
}

func (l *LinkedList) Len() int {
	return l.l.Len()
}

func (l *LinkedList) MoveAfter(e, at rtype.ListElement) {
	l.l.MoveAfter(e.(*LLElement).e, at.(*LLElement).e)
}

func (l *LinkedList) MoveBefore(e, at rtype.ListElement) {
	l.l.MoveBefore(e.(*LLElement).e, at.(*LLElement).e)
}

func (l *LinkedList) MoveToFront(e rtype.ListElement) {
	l.l.MoveToFront(e.(*LLElement).e)
}

func (l *LinkedList) MoveToBack(e rtype.ListElement) {
	l.l.MoveToBack(e.(*LLElement).e)
}

func (l *LinkedList) PushBack(v rtype.String) rtype.ListElement {
	if e := l.l.PushBack(v); e != nil {
		return &LLElement{e: e}
	}
	return nil
}

func (l *LinkedList) PushFront(v rtype.String) rtype.ListElement {
	if e := l.l.PushFront(v); e != nil {
		return &LLElement{e: e}
	}
	return nil
}

func (l *LinkedList) Remove(e rtype.ListElement) rtype.String {
	return l.l.Remove(e.(*LLElement).e).(rtype.String)
}

// Return the element with the value.
func (l *LinkedList) SearchKey(v rtype.String) rtype.ListElement {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value() == v {
			return e
		}
	}
	return nil
}

// Return the element at that index.
func (l *LinkedList) Index(n int) rtype.ListElement {
	e := l.Front()
	for i := 0; e != nil && i < n; i++ {
		e = e.Next()
	}
	return e
}

// Pop the tail of the list and push it to the front.
func (l *LinkedList) Rotate() {
	l.PushFront(l.Remove(l.Back()))
}

func (l *LinkedList) PopFront() rtype.ListElement {
	e := l.Front()
	if e != nil {
		l.Remove(e)
	}
	return e
}

func (l *LinkedList) PopBack() rtype.ListElement {
	e := l.Back()
	if e != nil {
		l.Remove(e)
	}
	return e
}

func (l *LinkedList) Iterator(head int) rtype.Iterator {
	return NewIterator(l, head)
}

func New() rtype.List {
	l := list.New()
	l.Init()
	return &LinkedList{l: l}
}

type ListIterator struct {
	l    rtype.List
	node rtype.ListElement
	head int
}

func (l *ListIterator) HasNext() bool {
	if l.node == nil {
		if l.head == rtype.REDIS_LIST_HEAD {
			return l.l.Front() != nil
		} else {
			return l.l.Back() != nil
		}
	}

	if l.head == rtype.REDIS_LIST_HEAD {
		return l.node.Next() != nil
	} else {
		return l.node.Prev() != nil
	}
}

func (l *ListIterator) Next() interface{} {
	if l.node == nil {
		if l.head == rtype.REDIS_LIST_HEAD {
			l.node = l.l.Front()
		} else {
			l.node = l.l.Back()
		}
	} else {
		if l.head == rtype.REDIS_LIST_HEAD {
			l.node = l.node.Next()
		} else {
			l.node = l.node.Prev()
		}
	}

	return l.node
}

func (l *ListIterator) Remove() {
	if l.node == nil {
		return
	}

	var next rtype.ListElement

	if l.head == rtype.REDIS_LIST_HEAD {
		next = l.node.Next()
	} else {
		next = l.node.Prev()
	}
	l.l.Remove(l.node)
	l.node = next
}

func NewIterator(l rtype.List, head int) rtype.Iterator {
	return &ListIterator{l: l, head: head}
}
