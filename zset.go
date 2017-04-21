package redigo

import (
	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/rtype/zset"
	"github.com/SteveZhangBit/redigo/shared"
)

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

func ZADDCommand(c *RedigoClient) {
	var z rtype.ZSet

	/* TODO: Parse options. At the end 'scoreidx' is set to the argument position
	 * of the score of the first score-element pair. */
	scoreidx := 2
	elements := c.Argc - scoreidx
	if elements%2 != 0 {
		c.AddReply(shared.SyntaxErr)
	}

	/* Start parsing all the scores, we need to emit any syntax error
	 * before executing additions to the sorted set, as the command should
	 * either execute fully or nothing at all. */
	scores := make([]float64, elements)
	for i := 0; i < elements; i++ {
		if x, ok := GetFloat64FromStringOrReply(c, c.Argv[scoreidx+i*2], ""); !ok {
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
			c.AddReply(shared.WrongTypeErr)
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
		NotifyKeyspaceEvent(REDIS_NOTIFY_ZSET, "zadd", c.Argv[1], c.DB.ID)
	}
}

func ZINCRBYCommand(c *RedigoClient) {

}

func ZREMCommand(c *RedigoClient) {
	var z rtype.ZSet

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil {
		return
	} else if z, ok = o.(rtype.ZSet); !ok {
		c.AddReply(shared.WrongTypeErr)
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
		NotifyKeyspaceEvent(REDIS_NOTIFY_ZSET, "zrem", c.Argv[1], c.DB.ID)
		if keyremoved {
			NotifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB.ID)
		}
		c.DB.SignalModifyKey(c.Argv[1])
		c.Server.Dirty++
	}
	c.AddReplyInt64(deleted)
}

func ZREMRANGEBYRANKCommand(c *RedigoClient) {

}

func ZREMRANGEBYSCORECommand(c *RedigoClient) {

}

func ZREMRANGEBYLEXCommand(c *RedigoClient) {

}

func ZUNIONSTORECommand(c *RedigoClient) {

}

func ZINTERSTORECommand(c *RedigoClient) {

}

func zrange(c *RedigoClient, reverse bool) {
	var z rtype.ZSet
	var start, end int
	var withscores bool

	if x, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); !ok {
		return
	} else {
		if y, ok := GetInt64FromStringOrReply(c, c.Argv[3], ""); !ok {
			return
		} else {
			start, end = int(x), int(y)
		}
	}

	if c.Argc == 5 && c.Argv[4] == "withscores" {
		withscores = true
	} else if c.Argc >= 5 {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.EmptyMultiBulk); o == nil {
		return
	} else if z, ok = o.(rtype.ZSet); !ok {
		c.AddReply(shared.WrongTypeErr)
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
		c.AddReply(shared.EmptyMultiBulk)
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
		c.AddReplyBulk(ln.Value().String())
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

func ZRANGECommand(c *RedigoClient) {
	zrange(c, false)
}

func ZREVRANGECommand(c *RedigoClient) {
	zrange(c, true)
}

func zrangescore(c *RedigoClient, reverse bool) {
	// var z rtype.ZSet
	// var minidx, maxidx int

	// if reverse {
	// 	// Range is given as [max,min]
	// 	minidx, maxidx = 3, 2
	// } else {
	// 	// Range is given as [min,max]
	// 	minidx, maxidx = 2, 3
	// }
}

func ZRANGEBYSCORECommand(c *RedigoClient) {

}

func ZREVRANGEBYSCORECommand(c *RedigoClient) {

}

func ZCOUNTCommand(c *RedigoClient) {

}

func ZLEXCOUNTCommand(c *RedigoClient) {

}

func ZRANGEBYLEXCommand(c *RedigoClient) {

}

func ZREVRANGEBYLEXCommand(c *RedigoClient) {

}

func ZCARDCommand(c *RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil {
		if z, ok := o.(rtype.ZSet); !ok {
			c.AddReply(shared.WrongTypeErr)
		} else {
			c.AddReplyInt64(int64(z.Len()))
		}
	}
}

func ZSCORECommand(c *RedigoClient) {
	var z rtype.ZSet

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o == nil {
		return
	} else if z, ok = o.(rtype.ZSet); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	if score, ok := z.Get(rstring.New(c.Argv[2])); ok {
		c.AddReplyFloat64(score)
	} else {
		c.AddReply(shared.NullBulk)
	}
}

func zrank(c *RedigoClient, reverse bool) {
	var z rtype.ZSet

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o == nil {
		return
	} else if z, ok = o.(rtype.ZSet); !ok {
		c.AddReply(shared.WrongTypeErr)
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
		c.AddReply(shared.NullBulk)
	}
}

func ZRANKCommand(c *RedigoClient) {
	zrank(c, false)
}

func ZREVRANKCommand(c *RedigoClient) {
	zrank(c, true)
}

func ZSCANCommand(c *RedigoClient) {

}
