package command

import (
	"fmt"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/pubsub"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/rtype/zset"
	"github.com/SteveZhangBit/redigo/rtype/zset/zskiplist"
	"github.com/SteveZhangBit/redigo/shared"
)

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

func ZADDCommand(c *redigo.RedigoClient) {
	var z *zset.ZSet

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
		if z, ok = o.(*zset.ZSet); !ok {
			c.AddReply(shared.WrongTypeErr)
			return
		}
	}

	var added, updated int
	for i := 0; i < elements; i++ {
		curobj := rstring.New(c.Argv[scoreidx+i*2+1])
		score := scores[i]

		switch z_enc := z.Val.(type) {
		case *zskiplist.ZSkiplist:
			// Check if the key is already in the set
			if curscore, ok := z.Dict[*curobj]; ok {
				/* Remove and re-insert when score changed. We can safely
				 * delete the key object from the skiplist, since the
				 * dictionary still has a reference to it. */
				if score != curscore {
					z_enc.Delete(curscore, curobj)
					z_enc.Insert(score, curobj)
					z.Dict[*curobj] = score

					c.Server.Dirty++
					updated++
				}
			} else {
				z_enc.Insert(score, curobj)
				z.Dict[*curobj] = score
				c.Server.Dirty++
				added++
			}

		default:
			panic(fmt.Sprintf("Type %T is not a string object", z_enc))
		}
	}
	c.AddReplyInt64(int64(added + updated))
	if added > 0 || updated > 0 {
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyZSet, "zadd", c.Argv[1], c.DB.ID)
	}
}

func ZINCRBYCommand(c *redigo.RedigoClient) {

}

func ZREMCommand(c *redigo.RedigoClient) {
	var z *zset.ZSet

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil {
		return
	} else if z, ok = o.(*zset.ZSet); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	var deleted int
	var keyremoved bool
	switch z_enc := z.Val.(type) {
	case *zskiplist.ZSkiplist:
		for i := 2; i < c.Argc; i++ {
			val := rstring.New(c.Argv[i])
			if score, ok := z.Dict[*val]; ok {
				deleted++

				z_enc.Delete(score, val)
				delete(z.Dict, *val)

				if len(z.Dict) == 0 {
					c.DB.Delete(c.Argv[1])
					keyremoved = true
					break
				}
			}
		}

	default:
		panic(fmt.Sprintf("Type %T is not a string object", z_enc))
	}
	if deleted > 0 {
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyZSet, "zrem", c.Argv[1], c.DB.ID)
		if keyremoved {
			pubsub.NotifyKeyspaceEvent(pubsub.NotifyGeneric, "del", c.Argv[1], c.DB.ID)
		}
		c.DB.SignalModifyKey(c.Argv[1])
		c.Server.Dirty++
	}
	c.AddReplyInt64(int64(deleted))
}

func ZREMRANGEBYRANKCommand(c *redigo.RedigoClient) {

}

func ZREMRANGEBYSCORECommand(c *redigo.RedigoClient) {

}

func ZREMRANGEBYLEXCommand(c *redigo.RedigoClient) {

}

func ZUNIONSTORECommand(c *redigo.RedigoClient) {

}

func ZINTERSTORECommand(c *redigo.RedigoClient) {

}

func zrange(c *redigo.RedigoClient, reverse bool) {
	var z *zset.ZSet
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
	} else if z, ok = o.(*zset.ZSet); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	// Sanitize indexes.
	length := len(z.Dict)
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

	switch z_enc := z.Val.(type) {
	case *zskiplist.ZSkiplist:
		var ln *zskiplist.ZSkiplistNode
		if reverse {
			if start > 0 {
				ln = z_enc.GetElementByRank(uint(length - start))
			} else {
				ln = z_enc.Tail
			}
		} else {
			if start > 0 {
				ln = z_enc.GetElementByRank(uint(start + 1))
			} else {
				ln = z_enc.Header.Level[0].Forward
			}
		}

		for ; rangelen > 0; rangelen-- {
			c.AddReplyBulk(ln.Obj.String())
			if withscores {
				c.AddReplyFloat64(ln.Score)
			}
			if reverse {
				ln = ln.Backward
			} else {
				ln = ln.Level[0].Forward
			}
		}

	default:
		panic(fmt.Sprintf("Type %T is not a string object", z_enc))
	}
}

func ZRANGECommand(c *redigo.RedigoClient) {
	zrange(c, false)
}

func ZREVRANGECommand(c *redigo.RedigoClient) {
	zrange(c, true)
}

func zrangescore(c *redigo.RedigoClient, reverse bool) {
	// var z *zset.ZSet
	// var minidx, maxidx int

	// if reverse {
	// 	// Range is given as [max,min]
	// 	minidx, maxidx = 3, 2
	// } else {
	// 	// Range is given as [min,max]
	// 	minidx, maxidx = 2, 3
	// }
}

func ZRANGEBYSCORECommand(c *redigo.RedigoClient) {

}

func ZREVRANGESCORECommand(c *redigo.RedigoClient) {

}

func ZCOUNTCommand(c *redigo.RedigoClient) {

}

func ZLEXCOUNTCommand(c *redigo.RedigoClient) {

}

func ZRANGEBYLEXCommand(c *redigo.RedigoClient) {

}

func ZREVRANGEBYLEXCommand(c *redigo.RedigoClient) {

}

func ZCARDCommand(c *redigo.RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil {
		if z, ok := o.(*zset.ZSet); !ok {
			c.AddReply(shared.WrongTypeErr)
		} else {
			c.AddReplyInt64(int64(len(z.Dict)))
		}
	}
}

func ZSCORECommand(c *redigo.RedigoClient) {
	var z *zset.ZSet

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o == nil {
		return
	} else if z, ok = o.(*zset.ZSet); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	if score, ok := z.Dict[*rstring.New(c.Argv[2])]; ok {
		c.AddReplyFloat64(score)
	} else {
		c.AddReply(shared.NullBulk)
	}
}

func zrank(c *redigo.RedigoClient, reverse bool) {
	var z *zset.ZSet

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o == nil {
		return
	} else if z, ok = o.(*zset.ZSet); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	length := len(z.Dict)

	switch z_enc := z.Val.(type) {
	case *zskiplist.ZSkiplist:
		val := rstring.New(c.Argv[2])
		if score, ok := z.Dict[*val]; ok {
			rank := z_enc.GetRank(score, val)
			if reverse {
				c.AddReplyInt64(int64(length) - int64(rank))
			} else {
				c.AddReplyInt64(int64(rank - 1))
			}
		} else {
			c.AddReply(shared.NullBulk)
		}

	default:
		panic(fmt.Sprintf("Type %T is not a string object", z_enc))
	}
}

func ZRANKCommand(c *redigo.RedigoClient) {
	zrank(c, false)
}

func ZREVRANKCommand(c *redigo.RedigoClient) {
	zrank(c, true)
}

func ZSCANCommand(c *redigo.RedigoClient) {

}
