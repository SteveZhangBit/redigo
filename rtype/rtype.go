package rtype

const (
	REDIS_HASH_KEY = 1 << iota
	REDIS_HASH_VALUE
)

const (
	REDIS_LIST_TAIL = 0
	REDIS_LIST_HEAD = 1
)

type String interface {
	String() string
	Bytes() []byte
	Len() int64
	Append(b []byte) String
}

type HashMap interface {
	/* Add an element, discard the old if the key already exists.
	 * Return false on insert and true on update. */
	Set(key []byte, v String) bool
	Get(key []byte) (String, bool)
	Delete(key []byte)
	Len() int
	Iterate(iterf func(key []byte, v String))
}

type List interface {
	Front() ListElement
	Back() ListElement
	InsertAfter(v String, at ListElement) ListElement
	InsertBefore(v String, at ListElement) ListElement
	Len() int
	MoveAfter(e, at ListElement)
	MoveBefore(e, at ListElement)
	MoveToFront(e ListElement)
	MoveToBack(e ListElement)
	PushBack(v String) ListElement
	PushFront(v String) ListElement
	Remove(e ListElement) String
	SearchKey(v String) ListElement
	Index(n int) ListElement
	Rotate()
	PopFront() ListElement
	PopBack() ListElement
	Iterator(head int) Iterator
}

type ListElement interface {
	Prev() ListElement
	Next() ListElement
	Value() String
	SetValue(v String)
}

type Set interface {
	Add(v String) bool
	Remove(v String) bool
	Size() int
	IsMember(v String) bool
	RandomElement() String
}

type ZSet interface {
	Add(score float64, v String) bool
	Update(score float64, v String) bool
	Get(v String) (float64, bool)
	Delete(score float64, v String) bool
	Len() int
	Head() ZSetItem
	Tail() ZSetItem
	GetByRank(rank uint) ZSetItem
	GetRank(score float64, v String) uint
}

type ZSetItem interface {
	Next() ZSetItem
	Prev() ZSetItem
	Value() String
	Score() float64
}

type Iterator interface {
	HasNext() bool
	Next() interface{}
	Remove()
}
