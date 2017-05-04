package redigo

import (
	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/list"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/shared"
)

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

func listPush(c CommandArg, where int) {
	var l rtype.List
	var pushed int

	if o := c.DB.LookupKeyWrite(c.Argv[1]); o != nil {
		var ok bool
		if l, ok = o.(rtype.List); !ok {
			c.AddReply(shared.WrongTypeErr)
			return
		}
	} else {
		l = list.New()
		c.DB.Add(c.Argv[1], l)
	}

	for i := 2; i < c.Argc; i++ {
		if where == rtype.REDIS_LIST_HEAD {
			l.PushFront(rstring.New(c.Argv[i]))
		} else {
			l.PushBack(rstring.New(c.Argv[i]))
		}
		pushed++
	}
	c.AddReplyInt64(int64(l.Len()))
	if pushed > 0 {
		var e string
		if where == rtype.REDIS_LIST_HEAD {
			e = "lpush"
		} else {
			e = "rpush"
		}
		c.DB.SignalModifyKey(c.Argv[1])
		NotifyKeyspaceEvent(REDIS_NOTIFY_LIST, e, c.Argv[1], c.DB.ID)
	}
	c.Server.Dirty += pushed
}

func LPUSHCommand(c CommandArg) {
	listPush(c, rtype.REDIS_LIST_HEAD)
}

func RPUSHCommand(c CommandArg) {
	listPush(c, rtype.REDIS_LIST_TAIL)
}

func listPushx(c CommandArg, ref rtype.String, val rtype.String, where int) {
	var l rtype.List
	var inserted bool

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	if ref != nil {
		/* We're not sure if this value can be inserted yet, but we cannot
		 * convert the list inside the iterator. We don't want to loop over
		 * the list twice (once to see if the value can be inserted and once
		 * to do the actual insert), so we assume this value can be inserted. */

		// Seek refval from head to tail
		for e := l.Front(); e != nil; e = e.Next() {
			if rstring.EqualStringObjects(e.Value(), ref) {
				if where == rtype.REDIS_LIST_TAIL {
					l.InsertAfter(val, e)
				} else {
					l.InsertBefore(val, e)
				}
				inserted = true
			}
		}

		if inserted {
			c.DB.SignalModifyKey(c.Argv[1])
			NotifyKeyspaceEvent(REDIS_NOTIFY_LIST, "linsert", c.Argv[1], c.DB.ID)
			c.Server.Dirty++
		} else {
			c.AddReply(shared.CNegOne)
			return
		}
	} else {
		var e string
		if where == rtype.REDIS_LIST_HEAD {
			e = "lpush"
			l.PushFront(val)
		} else {
			e = "rpush"
			l.PushBack(val)
		}
		c.DB.SignalModifyKey(c.Argv[1])
		NotifyKeyspaceEvent(REDIS_NOTIFY_LIST, e, c.Argv[1], c.DB.ID)
		c.Server.Dirty++
	}
	c.AddReplyInt64(int64(l.Len()))
}

func LPUSHXCommand(c CommandArg) {
	listPushx(c, nil, rstring.New(c.Argv[2]), rtype.REDIS_LIST_HEAD)
}

func RPUSHXCommand(c CommandArg) {
	listPushx(c, nil, rstring.New(c.Argv[2]), rtype.REDIS_LIST_TAIL)
}

func LINSERTCommand(c CommandArg) {
	if string(c.Argv[2]) == "after" {
		listPushx(c, rstring.New(c.Argv[3]), rstring.New(c.Argv[4]), rtype.REDIS_LIST_TAIL)
	} else if string(c.Argv[2]) == "before" {
		listPushx(c, rstring.New(c.Argv[3]), rstring.New(c.Argv[4]), rtype.REDIS_LIST_HEAD)
	} else {
		c.AddReply(shared.SyntaxErr)
	}
}

func LLENCommand(c CommandArg) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil {
		if l, ok := o.(rtype.List); !ok {
			c.AddReply(shared.WrongTypeErr)
		} else {
			c.AddReplyInt64(int64(l.Len()))
		}
	}
}

func LINDEXCommand(c CommandArg) {
	var l rtype.List

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	if index, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); ok {
		e := l.Index(int(index))
		if e != nil {
			c.AddReplyBulk(e.Value().String())
		} else {
			c.AddReply(shared.NullBulk)
		}
	}
}

func LSETCommand(c CommandArg) {
	var l rtype.List
	var index int

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.NoKeyErr); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	if x, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); !ok {
		return
	} else {
		index = int(x)
	}

	e := l.Index(index)
	if e != nil {
		e.SetValue(rstring.New(c.Argv[3]))
		c.AddReply(shared.OK)
		c.DB.SignalModifyKey(c.Argv[1])
		NotifyKeyspaceEvent(REDIS_NOTIFY_LIST, "lset", c.Argv[1], c.DB.ID)
		c.Server.Dirty++
	} else {
		c.AddReply(shared.OutOfRangeErr)
	}
}

func listPop(c CommandArg, where int) {
	var l rtype.List

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.NullBulk); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	var event string
	var e rtype.ListElement
	if where == rtype.REDIS_LIST_HEAD {
		event = "lpop"
		e = l.PopFront()
	} else {
		event = "rpop"
		e = l.PopBack()
	}
	if e == nil {
		c.AddReply(shared.NullBulk)
	} else {
		c.AddReplyBulk(e.Value().String())
		NotifyKeyspaceEvent(REDIS_NOTIFY_LIST, event, c.Argv[1], c.DB.ID)
		if l.Len() == 0 {
			NotifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB.ID)
			c.DB.Delete(c.Argv[1])
		}
		c.DB.SignalModifyKey(c.Argv[1])
		c.Server.Dirty++
	}
}

func LPOPCommand(c CommandArg) {
	listPop(c, rtype.REDIS_LIST_HEAD)
}

func RPOPCommand(c CommandArg) {
	listPop(c, rtype.REDIS_LIST_TAIL)
}

func LRANGECommand(c CommandArg) {

}

func LTRIMCommand(c CommandArg) {
	var l rtype.List
	var start, end, llen int

	if x, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); !ok {
		return
	} else if y, ok := GetInt64FromStringOrReply(c, c.Argv[3], ""); !ok {
		return
	} else {
		start = int(x)
		end = int(y)
	}

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.OK); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
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

	NotifyKeyspaceEvent(REDIS_NOTIFY_LIST, "ltrim", c.Argv[1], c.DB.ID)
	if l.Len() == 0 {
		c.DB.Delete(c.Argv[1])
		NotifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB.ID)
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
func LREMCommand(c CommandArg) {
	var l rtype.List
	var toremove int

	if x, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); !ok {
		return
	} else {
		toremove = int(x)
	}

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	val := rstring.New(c.Argv[3])
	removed := 0
	if toremove < 0 {
		toremove = -toremove
		for e := l.Back(); e != nil; e = e.Prev() {
			if rstring.EqualStringObjects(e.Value(), val) {
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
			if rstring.EqualStringObjects(e.Value(), val) {
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

func RPOPLPUSHCommand(c CommandArg) {

}

func BLPOPCommand(c CommandArg) {

}

func BRPOPCommand(c CommandArg) {

}

func BRPOPLPUSHCommand(c CommandArg) {

}
