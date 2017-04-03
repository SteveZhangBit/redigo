package list

import (
	"container/list"

	"github.com/SteveZhangBit/redigo/pubsub"

	"github.com/SteveZhangBit/redigo/rstring"

	"github.com/SteveZhangBit/redigo/shared"

	"github.com/SteveZhangBit/redigo"
)

const (
	ListTail = 0
	ListHead = 1
)

type LinkedList struct {
	list.List
}

func New() *LinkedList {
	l := &LinkedList{}
	l.Init()
	return l
}

// Return the element with the value.
func (l *LinkedList) SearchKey(v interface{}) *list.Element {
	for e := l.Front(); e != nil; e = e.Next() {
		if e.Value == v {
			return e
		}
	}
	return nil
}

// Return the element at that index.
func (l *LinkedList) Index(n int) *list.Element {
	e := l.Front()
	for i := 0; e != nil && i < n; i++ {
		e = e.Next()
	}
	return e
}

// Pop the tail of the list and push it to the front.
func (l *LinkedList) Rotate() {
	tail := l.Back()
	l.Remove(tail)
	l.PushFront(tail)
}

func (l *LinkedList) PopFront() *list.Element {
	e := l.Front()
	if e != nil {
		l.Remove(e)
	}
	return e
}

func (l *LinkedList) PopBack() *list.Element {
	e := l.Back()
	if e != nil {
		l.Remove(e)
	}
	return e
}

func CheckType(c *redigo.RedigoClient, o interface{}) (ok bool) {
	if _, ok = o.(*LinkedList); !ok {
		c.AddReply(shared.WrongTypeErr)
	}
	return
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

func push(c *redigo.RedigoClient, where int) {
	var l *LinkedList
	var pushed uint = 0

	if o := c.DB.LookupKeyWrite(c.Argv[1]); o != nil {
		var ok bool
		if l, ok = o.(*LinkedList); !ok {
			c.AddReply(shared.WrongTypeErr)
			return
		}
	} else {
		l = New()
		c.DB.Add(c.Argv[1], l)
	}

	for i := 2; i < c.Argc; i++ {
		if where == ListHead {
			l.PushFront(rstring.New(c.Argv[i]))
		} else {
			l.PushBack(rstring.New(c.Argv[i]))
		}
		pushed++
	}
	c.AddReplyInt64(int64(l.Len()))
	if pushed > 0 {
		var e string
		if where == ListHead {
			e = "lpush"
		} else {
			e = "rpush"
		}
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyList, e, c.Argv[1], c.DB.ID)
	}
	c.Server.Dirty += pushed
}

func LPUSHCommand(c *redigo.RedigoClient) {
	push(c, ListHead)
}

func RPUSHCommand(c *redigo.RedigoClient) {
	push(c, ListTail)
}

func pushx(c *redigo.RedigoClient, ref *rstring.RString, val *rstring.RString, where int) {
	var l *LinkedList
	var inserted bool = false

	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o == nil || !CheckType(c, o) {
		return
	} else {
		l = o.(*LinkedList)
	}

	if ref != nil {
		/* We're not sure if this value can be inserted yet, but we cannot
		 * convert the list inside the iterator. We don't want to loop over
		 * the list twice (once to see if the value can be inserted and once
		 * to do the actual insert), so we assume this value can be inserted. */

		// Seek refval from head to tail
		for e := l.Front(); e != nil; e = e.Next() {
			if rstring.EqualStringObjects(e.Value.(*rstring.RString), ref) {
				if where == ListTail {
					l.InsertAfter(val, e)
				} else {
					l.InsertBefore(val, e)
				}
				inserted = true
			}
		}

		if inserted {
			c.DB.SignalModifyKey(c.Argv[1])
			pubsub.NotifyKeyspaceEvent(pubsub.NotifyList, "linsert", c.Argv[1], c.DB.ID)
			c.Server.Dirty++
		} else {
			c.AddReply(shared.CNegOne)
			return
		}
	} else {
		var e string
		if where == ListHead {
			e = "lpush"
			l.PushFront(val)
		} else {
			e = "rpush"
			l.PushBack(val)
		}
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyList, e, c.Argv[1], c.DB.ID)
		c.Server.Dirty++
	}
	c.AddReplyInt64(int64(l.Len()))
}

func LINSERTCommand(c *redigo.RedigoClient) {
	if string(c.Argv[2]) == "after" {
		pushx(c, rstring.New(c.Argv[3]), rstring.New(c.Argv[4]), ListTail)
	} else if string(c.Argv[2]) == "before" {
		pushx(c, rstring.New(c.Argv[3]), rstring.New(c.Argv[4]), ListHead)
	} else {
		c.AddReply(shared.SyntaxErr)
	}
}

func LLENCommand(c *redigo.RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil && CheckType(c, o) {
		c.AddReplyInt64(int64(o.(*LinkedList).Len()))
	}
}

func LINDEXCommand(c *redigo.RedigoClient) {
	var l *LinkedList

	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o == nil || !CheckType(c, o) {
		return
	} else {
		l = o.(*LinkedList)
	}

	if index, ok := rstring.GetInt64FromStringOrReply(c, c.Argv[2], ""); ok {
		e := l.Index(int(index))
		if e != nil {
			c.AddReplyBulk(e.Value.(*rstring.RString).String())
		} else {
			c.AddReply(shared.NullBulk)
		}
	}
}

func LSETCommand(c *redigo.RedigoClient) {
	var l *LinkedList
	var index int

	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.NoKeyErr); o == nil || !CheckType(c, o) {
		return
	} else {
		l = o.(*LinkedList)
	}
	if x, ok := rstring.GetInt64FromStringOrReply(c, c.Argv[2], ""); !ok {
		return
	} else {
		index = int(x)
	}

	e := l.Index(index)
	if e != nil {
		e.Value = rstring.New(c.Argv[3])
		c.AddReply(shared.OK)
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyList, "lset", c.Argv[1], c.DB.ID)
		c.Server.Dirty++
	} else {
		c.AddReply(shared.OutOfRangeErr)
	}
}

func pop(c *redigo.RedigoClient, where int) {
	var l *LinkedList

	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.NullBulk); o == nil || !CheckType(c, o) {
		return
	} else {
		l = o.(*LinkedList)
	}

	var event string
	var e *list.Element
	if where == ListHead {
		event = "lpop"
		e = l.PopFront()
	} else {
		event = "rpop"
		e = l.PopBack()
	}
	if e == nil {
		c.AddReply(shared.NullBulk)
	} else {
		c.AddReplyBulk(e.Value.(*rstring.RString).String())
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyList, event, c.Argv[1], c.DB.ID)
		if l.Len() == 0 {
			pubsub.NotifyKeyspaceEvent(pubsub.NotifyGeneric, "del", c.Argv[1], c.DB.ID)
			c.DB.Delete(c.Argv[1])
		}
		c.DB.SignalModifyKey(c.Argv[1])
		c.Server.Dirty++
	}
}

func LPOPCommand(c *redigo.RedigoClient) {
	pop(c, ListHead)
}

func RPOPCommand(c *redigo.RedigoClient) {
	pop(c, ListTail)
}

func LTRIMCommand(c *redigo.RedigoClient) {
	var l *LinkedList
	var start, end, llen int

	if x, ok := rstring.GetInt64FromStringOrReply(c, c.Argv[2], ""); !ok {
		return
	} else if y, ok := rstring.GetInt64FromStringOrReply(c, c.Argv[3], ""); !ok {
		return
	} else {
		start = int(x)
		end = int(y)
	}
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.OK); o == nil || !CheckType(c, o) {
		return
	} else {
		l = o.(*LinkedList)
	}
	llen = l.Len()

	// convert negative indexes
	if start < 0 {
		start = llen + start
	}
	if end < 0 {
		end = llen + end
	}
	if start < 0 {
		start = 0
	}

	var ltrim, rtrim int
	// Invariant: start >= 0, so this test will be true when end < 0.
	// The range is empty when start > end or start >= length.
	if start > end || start >= llen {
		ltrim = llen
		rtrim = 0
	} else {
		if end >= llen {
			end = llen - 1
		}
		ltrim = start
		rtrim = llen - end - 1
	}

	// Remove list elements to perform the trim
	for i := 0; i < ltrim; i++ {
		e := l.Front()
		l.Remove(e)
	}
	for i := 0; i < rtrim; i++ {
		e := l.Back()
		l.Remove(e)
	}

	pubsub.NotifyKeyspaceEvent(pubsub.NotifyList, "ltrim", c.Argv[1], c.DB.ID)
	if l.Len() == 0 {
		c.DB.Delete(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyGeneric, "del", c.Argv[1], c.DB.ID)
	}
	c.DB.SignalModifyKey(c.Argv[1])
	c.Server.Dirty++
	c.AddReply(shared.OK)
}

/* Removes the first count occurrences of elements equal to value from the list stored at key. The count argument influences the operation in the following ways:
 * count > 0: Remove elements equal to value moving from head to tail.
 * count < 0: Remove elements equal to value moving from tail to head.
 * count = 0: Remove all elements equal to value.
 * For example, LREM list -2 "hello" will remove the last two occurrences of "hello" in the list stored at list.
 * Note that non-existing keys are treated like empty lists, so when key does not exist, the command will always return 0.*/
func LREMCommand(c *redigo.RedigoClient) {
	var l *LinkedList
	var toremove int

	if x, ok := rstring.GetInt64FromStringOrReply(c, c.Argv[2], ""); !ok {
		return
	} else {
		toremove = int(x)
	}
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil || !CheckType(c, o) {
		return
	} else {
		l = o.(*LinkedList)
	}

	val := rstring.New(c.Argv[3])
	removed := 0
	if toremove < 0 {
		toremove = -toremove
		for e := l.Back(); e != nil; e = e.Prev() {
			if rstring.EqualStringObjects(e.Value.(*rstring.RString), val) {
				l.Remove(e)
				c.Server.Dirty++
				removed++
				if toremove != 0 && removed == toremove {
					break
				}
			}
		}
	} else {
		for e := l.Front(); e != nil; e = e.Next() {
			if rstring.EqualStringObjects(e.Value.(*rstring.RString), val) {
				l.Remove(e)
				c.Server.Dirty++
				removed++
				if toremove != 0 && removed == toremove {
					break
				}
			}
		}
	}
	if l.Len() == 0 {
		c.DB.Delete(c.Argv[1])
	}
	c.AddReplyInt64(int64(removed))
	if removed > 0 {
		c.DB.SignalModifyKey(c.Argv[1])
	}
}

func RPPOPLPUSHCommand(c *redigo.RedigoClient) {

}

func BLPOPCommand(c *redigo.RedigoClient) {

}

func BRPOPCommand(c *redigo.RedigoClient) {

}

func BRPOPLPUSHCommand(c *redigo.RedigoClient) {

}
