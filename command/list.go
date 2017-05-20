package command

import (
	"time"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/list"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
)

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

func listPush(c redigo.CommandArg, where int) {
	var l rtype.List
	var pushed int

	if o := c.DB().LookupKeyWrite(c.Argv[1]); o != nil {
		var ok bool
		if l, ok = o.(rtype.List); !ok {
			c.AddReply(redigo.WrongTypeErr)
			return
		}
	} else {
		l = list.New()
		c.DB().Add(c.Argv[1], l)
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
		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_LIST, e, c.Argv[1], c.DB().GetID())
	}
	c.Server().AddDirty(pushed)
}

func LPUSHCommand(c redigo.CommandArg) {
	listPush(c, rtype.REDIS_LIST_HEAD)
}

func RPUSHCommand(c redigo.CommandArg) {
	listPush(c, rtype.REDIS_LIST_TAIL)
}

func listPushx(c redigo.CommandArg, ref rtype.String, val rtype.String, where int) {
	var l rtype.List
	var inserted bool

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], redigo.CZero); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(redigo.WrongTypeErr)
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
				break
			}
		}

		if inserted {
			c.DB().SignalModifyKey(c.Argv[1])
			c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_LIST, "linsert", c.Argv[1], c.DB().GetID())
			c.Server().AddDirty(1)
		} else {
			c.AddReply(redigo.CNegOne)
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
		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_LIST, e, c.Argv[1], c.DB().GetID())
		c.Server().AddDirty(1)
	}
	c.AddReplyInt64(int64(l.Len()))
}

func LPUSHXCommand(c redigo.CommandArg) {
	listPushx(c, nil, rstring.New(c.Argv[2]), rtype.REDIS_LIST_HEAD)
}

func RPUSHXCommand(c redigo.CommandArg) {
	listPushx(c, nil, rstring.New(c.Argv[2]), rtype.REDIS_LIST_TAIL)
}

func LINSERTCommand(c redigo.CommandArg) {
	if string(c.Argv[2]) == "after" {
		listPushx(c, rstring.New(c.Argv[3]), rstring.New(c.Argv[4]), rtype.REDIS_LIST_TAIL)
	} else if string(c.Argv[2]) == "before" {
		listPushx(c, rstring.New(c.Argv[3]), rstring.New(c.Argv[4]), rtype.REDIS_LIST_HEAD)
	} else {
		c.AddReply(redigo.SyntaxErr)
	}
}

func LLENCommand(c redigo.CommandArg) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], redigo.CZero); o != nil {
		if l, ok := o.(rtype.List); !ok {
			c.AddReply(redigo.WrongTypeErr)
		} else {
			c.AddReplyInt64(int64(l.Len()))
		}
	}
}

func LINDEXCommand(c redigo.CommandArg) {
	var l rtype.List

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], redigo.NullBulk); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(redigo.WrongTypeErr)
		return
	}

	if index, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); ok {
		e := l.Index(int(index))
		if e != nil {
			c.AddReplyBulk(e.Value().Bytes())
		} else {
			c.AddReply(redigo.NullBulk)
		}
	}
}

func LSETCommand(c redigo.CommandArg) {
	var l rtype.List
	var index int

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], redigo.NoKeyErr); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(redigo.WrongTypeErr)
		return
	}

	if x, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); !ok {
		return
	} else {
		index = int(x)
	}

	e := l.Index(index)
	if e != nil {
		e.SetValue(rstring.New(c.Argv[3]))
		c.AddReply(redigo.OK)
		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_LIST, "lset", c.Argv[1], c.DB().GetID())
		c.Server().AddDirty(1)
	} else {
		c.AddReply(redigo.OutOfRangeErr)
	}
}

func listPop(c redigo.CommandArg, where int) {
	var l rtype.List

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], redigo.NullBulk); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(redigo.WrongTypeErr)
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
		c.AddReply(redigo.NullBulk)
	} else {
		c.AddReplyBulk(e.Value().Bytes())
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_LIST, event, c.Argv[1], c.DB().GetID())
		if l.Len() == 0 {
			c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB().GetID())
			c.DB().Delete(c.Argv[1])
		}
		c.DB().SignalModifyKey(c.Argv[1])
		c.Server().AddDirty(1)
	}
}

func LPOPCommand(c redigo.CommandArg) {
	listPop(c, rtype.REDIS_LIST_HEAD)
}

func RPOPCommand(c redigo.CommandArg) {
	listPop(c, rtype.REDIS_LIST_TAIL)
}

func LRANGECommand(c redigo.CommandArg) {
	var l rtype.List
	var start, end, llen, rangelen int

	if x, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); !ok {
		return
	} else if y, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[3]), ""); !ok {
		return
	} else {
		start = int(x)
		end = int(y)
	}

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], redigo.EmptyMultiBulk); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReplyError(string(redigo.WrongTypeErr))
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

	/* Invariant: start >= 0, so this test will be true when end < 0.
	 * The range is empty when start > end or start >= length. */
	if start > end || start >= llen {
		c.AddReply(redigo.EmptyMultiBulk)
		return
	}
	if end >= llen {
		end = llen - 1
	}

	rangelen = end - start + 1

	// Return the result in form of a multi-bulk reply
	c.AddReplyMultiBulkLen(rangelen)
	/* If we are nearest to the end of the list, reach the element
	 * starting from tail and going backward, as it is faster. */
	if start > llen/2 {
		start -= llen
	}
	e := l.Index(start)
	for rangelen > 0 {
		c.AddReplyBulk(e.Value().Bytes())
		e = e.Next()
		rangelen--
	}
}

func LTRIMCommand(c redigo.CommandArg) {
	var l rtype.List
	var start, end, llen int

	if x, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); !ok {
		return
	} else if y, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[3]), ""); !ok {
		return
	} else {
		start = int(x)
		end = int(y)
	}

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], redigo.OK); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(redigo.WrongTypeErr)
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

	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_LIST, "ltrim", c.Argv[1], c.DB().GetID())
	if l.Len() == 0 {
		c.DB().Delete(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB().GetID())
	}
	c.DB().SignalModifyKey(c.Argv[1])
	c.Server().AddDirty(1)
	c.AddReply(redigo.OK)
}

/* Removes the first count occurrences of elements equal to value from the list stored at key. The count argument influences the operation in the following ways:
 * count > 0: Remove elements equal to value moving from head to tail.
 * count < 0: Remove elements equal to value moving from tail to head.
 * count = 0: Remove all elements equal to value.
 * For example, LREM list -2 "hello" will remove the last two occurrences of "hello" in the list stored at list.
 * Note that non-existing keys are treated like empty lists, so when key does not exist, the command will always return 0.*/
func LREMCommand(c redigo.CommandArg) {
	var l rtype.List
	var toremove int

	if x, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); !ok {
		return
	} else {
		toremove = int(x)
	}

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], redigo.CZero); o == nil {
		return
	} else if l, ok = o.(rtype.List); !ok {
		c.AddReply(redigo.WrongTypeErr)
		return
	}

	val := rstring.New(c.Argv[3])
	removed := 0
	if toremove < 0 {
		toremove = -toremove

		iter := l.Iterator(rtype.REDIS_LIST_TAIL)
		for iter.HasNext() {
			e := iter.Next().(rtype.ListElement)
			if rstring.EqualStringObjects(e.Value(), val) {
				iter.Remove()
				c.Server().AddDirty(1)
				removed++
				if toremove != 0 && removed == toremove {
					break
				}
			}
		}
	} else {
		iter := l.Iterator(rtype.REDIS_LIST_HEAD)
		for iter.HasNext() {
			e := iter.Next().(rtype.ListElement)
			if rstring.EqualStringObjects(e.Value(), val) {
				iter.Remove()
				c.Server().AddDirty(1)
				removed++
				if toremove != 0 && removed == toremove {
					break
				}
			}
		}
	}
	if l.Len() == 0 {
		c.DB().Delete(c.Argv[1])
	}
	c.AddReplyInt64(int64(removed))
	if removed > 0 {
		c.DB().SignalModifyKey(c.Argv[1])
	}
}

func RPOPLPUSHCommand(c redigo.CommandArg) {

}

/*============================== blocking APIs ========================================*/

/* This is how the current blocking POP works, we use BLPOP as example:
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if blocking is not required.
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blocking_keys) mapping keys to a list of clients
 *   blocking for this keys.
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we mark this key as "ready", and after the current command,
 *   MULTI/EXEC block, or script, is executed, we serve all the clients waiting
 *   for this list, from the one that blocked first, to the last, accordingly
 *   to the number of elements we have in the ready list.
 */

func GetTimeoutFromStringOrReply(c redigo.CommandArg, o rtype.String, unit time.Duration) (t time.Duration, ok bool) {
	var x int64

	if x, ok = GetInt64FromStringOrReply(c, o, "timeout is not an integer or out of range"); !ok {
		return
	}

	if x < 0 {
		c.AddReplyError("timeout is negtive")
		ok = false
		return
	}

	t = time.Duration(x) * unit
	return
}

func listBlockingPop(c redigo.CommandArg, where int) {
	var l rtype.List
	var timeout time.Duration
	var ok bool

	if timeout, ok = GetTimeoutFromStringOrReply(c, rstring.New(c.Argv[c.Argc-1]), time.Second); !ok {
		return
	}

	for i := 1; i < c.Argc-1; i++ {
		if o := c.DB().LookupKeyWrite(c.Argv[i]); o != nil {
			if l, ok = o.(rtype.List); !ok {
				c.AddReply(redigo.WrongTypeErr)
				return
			}
			if l.Len() != 0 {
				// Non empty list, this is like a non normal [LR]POP
				var event string
				var val rtype.String
				if where == rtype.REDIS_LIST_HEAD {
					event = "lpop"
					val = l.PopFront().Value()
				} else {
					event = "rpop"
					val = l.PopBack().Value()
				}

				c.AddReplyMultiBulkLen(2)
				c.AddReplyBulk(c.Argv[i])
				c.AddReplyBulk(val.Bytes())
				c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_LIST, event, c.Argv[i], c.DB().GetID())

				if l.Len() == 0 {
					c.DB().Delete(c.Argv[i])
					c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[i], c.DB().GetID())
				}
				c.DB().SignalModifyKey(c.Argv[i])
				c.Server().AddDirty(1)

				// At least one list is non-empty, so return
				return
			}
		}
	}

	// If the list is empty or the key does not exists we must block
	c.Client.BlockForKeys(c.Argv[1:c.Argc-1], timeout)
}

func BLPOPCommand(c redigo.CommandArg) {
	listBlockingPop(c, rtype.REDIS_LIST_HEAD)
}

func BRPOPCommand(c redigo.CommandArg) {
	listBlockingPop(c, rtype.REDIS_LIST_TAIL)
}

func BRPOPLPUSHCommand(c redigo.CommandArg) {

}
