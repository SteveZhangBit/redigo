package cmd

import (
	"bytes"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/protocol"
	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/rtype/zset"
)

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

func ZADDCommand(c *redigo.CommandArg) {
	var z rtype.ZSet

	/* TODO: Parse options. At the end 'scoreidx' is set to the argument position
	 * of the score of the first score-element pair. */
	scoreidx := 2
	elements := c.Argc - scoreidx
	if elements%2 != 0 {
		c.AddReply(protocol.SyntaxErr)
		return
	}
	elements /= 2

	/* Start parsing all the scores, we need to emit any syntax error
	 * before executing additions to the sorted set, as the command should
	 * either execute fully or nothing at all. */
	scores := make([]float64, elements)
	for i := 0; i < elements; i++ {
		if x, ok := GetFloat64FromStringOrReply(c, rstring.New(c.Argv[scoreidx+i*2]), ""); !ok {
			return
		} else {
			scores[i] = x
		}
	}

	// Lookup the key and create the sorted set if does not exist.
	if o := c.DB.LookupKeyWrite(c.Argv[1]); o == nil {
		z = zset.New()
		c.DB.Add(c.Argv[1], z)
	} else {
		var ok bool
		if z, ok = o.(rtype.ZSet); !ok {
			c.AddReply(protocol.WrongTypeErr)
			return
		}
	}

	var added, updated int64
	for i := 0; i < elements; i++ {
		curobj := rstring.New(c.Argv[scoreidx+i*2+1])
		score := scores[i]

		// Check if the key is already in the set
		if curscore, ok := z.Get(curobj); ok {
			/* Remove and re-insert when score changed. We can safely
			 * delete the key object from the skiplist, since the
			 * dictionary still has a reference to it. */
			if score != curscore {
				z.Update(score, curobj)
				c.Server.Dirty++
				updated++
			}
		} else {
			z.Add(score, curobj)
			c.Server.Dirty++
			added++
		}
	}
	c.AddReplyInt64(added + updated)
	if added > 0 || updated > 0 {
		c.DB.SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_ZSET, "zadd", c.Argv[1], c.DB.ID)
	}
}

func ZINCRBYCommand(c *redigo.CommandArg) {

}

func ZREMCommand(c *redigo.CommandArg) {
	var z rtype.ZSet

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], protocol.CZero); o == nil {
		return
	} else if z, ok = o.(rtype.ZSet); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	var deleted int64
	var keyremoved bool
	for i := 2; i < c.Argc; i++ {
		val := rstring.New(c.Argv[i])
		if score, ok := z.Get(val); ok {
			deleted++
			z.Delete(score, val)
			if z.Len() == 0 {
				c.DB.Delete(c.Argv[1])
				keyremoved = true
				break
			}
		}
	}

	if deleted > 0 {
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_ZSET, "zrem", c.Argv[1], c.DB.ID)
		if keyremoved {
			c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB.ID)
		}
		c.DB.SignalModifyKey(c.Argv[1])
		c.Server.Dirty++
	}
	c.AddReplyInt64(deleted)
}

func ZREMRANGEBYRANKCommand(c *redigo.CommandArg) {

}

func ZREMRANGEBYSCORECommand(c *redigo.CommandArg) {

}

func ZREMRANGEBYLEXCommand(c *redigo.CommandArg) {

}

func ZUNIONSTORECommand(c *redigo.CommandArg) {

}

func ZINTERSTORECommand(c *redigo.CommandArg) {

}

func zrange(c *redigo.CommandArg, reverse bool) {
	var z rtype.ZSet
	var start, end int
	var withscores bool

	if x, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); !ok {
		return
	} else {
		if y, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[3]), ""); !ok {
			return
		} else {
			start, end = int(x), int(y)
		}
	}

	if c.Argc == 5 && bytes.Equal(c.Argv[4], []byte("withscores")) {
		withscores = true
	} else if c.Argc >= 5 {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], protocol.EmptyMultiBulk); o == nil {
		return
	} else if z, ok = o.(rtype.ZSet); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	// Sanitize indexes.
	length := z.Len()
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}

	/* Invariant: start >= 0, so this test will be true when end < 0.
	 * The range is empty when start > end or start >= length. */
	if start > end || start >= length {
		c.AddReply(protocol.EmptyMultiBulk)
		return
	}
	if end >= length {
		end = length - 1
	}
	rangelen := end - start + 1
	if withscores {
		c.AddReplyMultiBulkLen(rangelen * 2)
	} else {
		c.AddReplyMultiBulkLen(rangelen)
	}

	var ln rtype.ZSetItem
	if reverse {
		if start > 0 {
			ln = z.GetByRank(uint(length - start))
		} else {
			ln = z.Tail()
		}
	} else {
		if start > 0 {
			ln = z.GetByRank(uint(start + 1))
		} else {
			ln = z.Head()
		}
	}

	for ; rangelen > 0; rangelen-- {
		c.AddReplyBulk(ln.Value().Bytes())
		if withscores {
			c.AddReplyFloat64(ln.Score())
		}
		if reverse {
			ln = ln.Prev()
		} else {
			ln = ln.Next()
		}
	}
}

func ZRANGECommand(c *redigo.CommandArg) {
	zrange(c, false)
}

func ZREVRANGECommand(c *redigo.CommandArg) {
	zrange(c, true)
}

func zrangescore(c *redigo.CommandArg, reverse bool) {

}

func ZRANGEBYSCORECommand(c *redigo.CommandArg) {

}

func ZREVRANGEBYSCORECommand(c *redigo.CommandArg) {

}

func ZCOUNTCommand(c *redigo.CommandArg) {

}

func ZLEXCOUNTCommand(c *redigo.CommandArg) {

}

func ZRANGEBYLEXCommand(c *redigo.CommandArg) {

}

func ZREVRANGEBYLEXCommand(c *redigo.CommandArg) {

}

func ZCARDCommand(c *redigo.CommandArg) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], protocol.CZero); o != nil {
		if z, ok := o.(rtype.ZSet); !ok {
			c.AddReply(protocol.WrongTypeErr)
		} else {
			c.AddReplyInt64(int64(z.Len()))
		}
	}
}

func ZSCORECommand(c *redigo.CommandArg) {
	var z rtype.ZSet

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], protocol.NullBulk); o == nil {
		return
	} else if z, ok = o.(rtype.ZSet); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	if score, ok := z.Get(rstring.New(c.Argv[2])); ok {
		c.AddReplyFloat64(score)
	} else {
		c.AddReply(protocol.NullBulk)
	}
}

func zrank(c *redigo.CommandArg, reverse bool) {
	var z rtype.ZSet

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], protocol.NullBulk); o == nil {
		return
	} else if z, ok = o.(rtype.ZSet); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	length := z.Len()
	val := rstring.New(c.Argv[2])
	if score, ok := z.Get(val); ok {
		rank := z.GetRank(score, val)
		if reverse {
			c.AddReplyInt64(int64(length) - int64(rank))
		} else {
			c.AddReplyInt64(int64(rank - 1))
		}
	} else {
		c.AddReply(protocol.NullBulk)
	}
}

func ZRANKCommand(c *redigo.CommandArg) {
	zrank(c, false)
}

func ZREVRANKCommand(c *redigo.CommandArg) {
	zrank(c, true)
}

func ZSCANCommand(c *redigo.CommandArg) {

}
