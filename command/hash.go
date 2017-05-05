package command

import (
	"math"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/hash"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
)

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

func hashLookupWriteOrCreate(c redigo.CommandArg, key string) (h rtype.HashMap) {
	if o := c.DB().LookupKeyWrite(key); o == nil {
		h = hash.New()
		c.DB().Add(key, h)
	} else {
		var ok bool
		if h, ok = o.(rtype.HashMap); !ok {
			c.AddReply(redigo.WrongTypeErr)
		}
	}
	return
}

func HSETCommand(c redigo.CommandArg) {
	var h rtype.HashMap
	if h = hashLookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	update := h.Set(c.Argv[2], rstring.New(c.Argv[3]))
	if update {
		c.AddReply(redigo.CZero)
	} else {
		c.AddReply(redigo.COne)
	}
	c.DB().SignalModifyKey(c.Argv[1])
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_HASH, "hset", c.Argv[1], c.DB().GetID())
	c.Server().AddDirty(1)
}

func HSETNXCommand(c redigo.CommandArg) {
	var h rtype.HashMap
	if h = hashLookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	if _, ok := h.Get(c.Argv[2]); ok {
		c.AddReply(redigo.CZero)
	} else {
		h.Set(c.Argv[2], rstring.New(c.Argv[3]))
		c.AddReply(redigo.COne)
		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_HASH, "hset", c.Argv[1], c.DB().GetID())
		c.Server().AddDirty(1)
	}
}

func HMSETCommand(c redigo.CommandArg) {
	var h rtype.HashMap
	if c.Argc%2 == 1 {
		c.AddReplyError("wrong number of arguments for HMSET")
		return
	}
	if h = hashLookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	for i := 2; i < c.Argc; i += 2 {
		h.Set(c.Argv[i], rstring.New(c.Argv[i+1]))
	}
	c.AddReply(redigo.OK)
	c.DB().SignalModifyKey(c.Argv[1])
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_HASH, "hset", c.Argv[1], c.DB().GetID())
	c.Server().AddDirty(1)
}

func HINCRBYCommand(c redigo.CommandArg) {
	var h rtype.HashMap
	var val, incr int64
	if x, ok := GetInt64FromStringOrReply(c, c.Argv[3], ""); ok {
		incr = x
	} else {
		return
	}
	if h = hashLookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	if cur, ok := h.Get(c.Argv[2]); ok {
		if val, ok = GetInt64FromStringOrReply(c, cur, "hash value is not an integer"); !ok {
			return
		}
	}

	if (incr < 0 && val < 0 && incr < math.MinInt64-val) ||
		(incr > 0 && val > 0 && incr > math.MaxInt64-val) {
		c.AddReplyError("increment or decrement would overflow")
		return
	}
	val += incr
	h.Set(c.Argv[2], rstring.NewFromInt64(val))
	c.AddReplyInt64(val)
	c.DB().SignalModifyKey(c.Argv[1])
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_HASH, "hincrby", c.Argv[1], c.DB().GetID())
	c.Server().AddDirty(1)
}

func HINCRBYFLOATCommand(c redigo.CommandArg) {
	var h rtype.HashMap
	var val, incr float64
	if x, ok := GetFloat64FromStringOrReply(c, c.Argv[3], ""); ok {
		incr = x
	} else {
		return
	}
	if h = hashLookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	val += incr
	str := rstring.NewFromFloat64(val)
	h.Set(c.Argv[2], str)
	c.AddReplyBulk(str.String())
	c.DB().SignalModifyKey(c.Argv[1])
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_HASH, "hincrbyfloat", c.Argv[1], c.DB().GetID())
	c.Server().AddDirty(1)

	/* TODO: Always replicate HINCRBYFLOAT as an HSET command with the final value
	 * in order to make sure that differences in float pricision or formatting
	 * will not create differences in replicas or after an AOF restart. */
}

func hashAddFieldToReply(c redigo.CommandArg, h rtype.HashMap, key string) {
	if h == nil {
		c.AddReply(redigo.NullBulk)
		return
	}
	if val, ok := h.Get(key); !ok {
		c.AddReply(redigo.NullBulk)
	} else {
		c.AddReplyBulk(val.String())
	}
}

func HGETCommand(c redigo.CommandArg) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], redigo.NullBulk); o != nil {
		if h, ok := o.(rtype.HashMap); !ok {
			c.AddReply(redigo.WrongTypeErr)
		} else {
			hashAddFieldToReply(c, h, c.Argv[2])
		}
	}
}

func HMGETCommand(c redigo.CommandArg) {
	/* Don't abort when the key cannot be found. Non-existing keys are empty
	 * hashes, where HMGET should respond with a series of null bulks. */
	o := c.DB().LookupKeyRead(c.Argv[1])
	if h, ok := o.(rtype.HashMap); o == nil || ok {
		c.AddReplyMultiBulkLen(c.Argc - 2)
		for i := 2; i < c.Argc; i++ {
			hashAddFieldToReply(c, h, c.Argv[i])
		}
	} else {
		c.AddReply(redigo.WrongTypeErr)
	}
}

func HDELCommand(c redigo.CommandArg) {
	var h rtype.HashMap
	var deleted int
	var keyremoved bool

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], redigo.CZero); o == nil {
		return
	} else if h, ok = o.(rtype.HashMap); !ok {
		c.AddReply(redigo.WrongTypeErr)
		return
	}

	for i := 2; i < c.Argc; i++ {
		if _, ok := h.Get(c.Argv[i]); ok {
			h.Delete(c.Argv[i])
			deleted++
			if h.Len() == 0 {
				c.DB().Delete(c.Argv[1])
				keyremoved = true
				break
			}
		}
	}
	if deleted > 0 {
		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_HASH, "hdel", c.Argv[1], c.DB().GetID())
		if keyremoved {
			c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB().GetID())
		}
		c.Server().AddDirty(deleted)
	}
	c.AddReplyInt64(int64(deleted))
}

func HLENCommand(c redigo.CommandArg) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], redigo.CZero); o != nil {
		if h, ok := o.(rtype.HashMap); !ok {
			c.AddReply(redigo.WrongTypeErr)
		} else {
			c.AddReplyInt64(int64(h.Len()))
		}
	}
}

func hashGetAll(c redigo.CommandArg, flags int) {
	var h rtype.HashMap

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], redigo.CZero); o == nil {
		return
	} else if h, ok = o.(rtype.HashMap); !ok {
		c.AddReply(redigo.WrongTypeErr)
		return
	}

	multiplier := 0
	if flags&rtype.REDIS_HASH_KEY > 0 {
		multiplier++
	}
	if flags&rtype.REDIS_HASH_VALUE > 0 {
		multiplier++
	}
	length := h.Len() * multiplier
	c.AddReplyMultiBulkLen(length)

	count := 0
	h.Iterate(func(key string, val rtype.String) {
		if flags&rtype.REDIS_HASH_KEY > 0 {
			count++
			c.AddReplyBulk(key)
		}
		if flags&rtype.REDIS_HASH_VALUE > 0 {
			count++
			c.AddReplyBulk(val.String())
		}
	})
	if count != length {
		panic("count does not equal to length at hash get all method")
	}
}

func HKEYSCommand(c redigo.CommandArg) {
	hashGetAll(c, rtype.REDIS_HASH_KEY)
}

func HVALSCommand(c redigo.CommandArg) {
	hashGetAll(c, rtype.REDIS_HASH_VALUE)
}

func HGETALLCommand(c redigo.CommandArg) {
	hashGetAll(c, rtype.REDIS_HASH_KEY|rtype.REDIS_HASH_VALUE)
}

func HEXISTSCommand(c redigo.CommandArg) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], redigo.CZero); o != nil {
		if h, ok := o.(rtype.HashMap); !ok {
			c.AddReply(redigo.WrongTypeErr)
		} else if _, ok = h.Get(c.Argv[2]); ok {
			c.AddReply(redigo.COne)
		} else {
			c.AddReply(redigo.CZero)
		}
	}
}

func HSCANCommand(c redigo.CommandArg) {

}
