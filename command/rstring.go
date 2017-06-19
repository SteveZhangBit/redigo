package command

import (
	"math"
	"strconv"
	"strings"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/protocol"
	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
)

func GetInt64FromStringOrReply(c *redigo.CommandArg, o rtype.String, msg string) (x int64, ok bool) {
	switch str := o.(type) {
	case nil:
		return 0, true
	case *rstring.IntString:
		x, ok = str.Val, true
	default:
		ok = false
	}
	if !ok {
		if msg != "" {
			c.AddReplyError(msg)
		} else {
			c.AddReplyError("value is not an integer or out of range")
		}
	}
	return
}

func GetFloat64FromStringOrReply(c *redigo.CommandArg, o rtype.String, msg string) (x float64, ok bool) {
	switch str := o.(type) {
	case nil:
		return 0.0, true
	case *rstring.IntString:
		x, ok = float64(str.Val), true
	case *rstring.BytesString:
		if i, err := strconv.ParseFloat(string(str.Val), 64); err != nil {
			ok = false
		} else {
			x, ok = i, true
		}
	default:
		ok = false
	}
	if !ok {
		if msg != "" {
			c.AddReplyError(msg)
		} else {
			c.AddReplyError("value is not a valid float")
		}
	}
	return
}

func CheckStringlength(c *redigo.CommandArg, size int64) bool {
	if size > 512*1024*1024 {
		c.AddReplyError("string exceeds maximum allowed size (512MB)")
		return false
	}
	return true
}

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/
const (
	REDIS_SET_NO_FLAGS = 0
	REDIS_SET_NX       = 1 << 0 // set if key not exists
	REDIS_SET_XX       = 1 << 1 // set if key exists
)

/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>]
 * Starting with Redis 2.6.12 SET supports a set of options that modify its behavior:
 * EX seconds -- Set the specified expire time, in seconds.
 * PX milliseconds -- Set the specified expire time, in milliseconds.
 * NX -- Only set the key if it does not already exist.
 * XX -- Only set the key if it already exist.
 *
 * TODO: Currently, we only implement the very basic function of SET command. */
func rstringSet(c *redigo.CommandArg, flags int, okReply, abortReply []byte) {
	if (flags&REDIS_SET_NX > 0 && c.DB().LookupKeyWrite(c.Argv[1]) != nil) ||
		(flags&REDIS_SET_XX > 0 && c.DB().LookupKeyWrite(c.Argv[1]) == nil) {
		if len(abortReply) != 0 {
			c.AddReply(abortReply)
		} else {
			c.AddReply(protocol.NullBulk)
		}
		return
	}

	c.DB().SetKeyPersist(c.Argv[1], rstring.New(c.Argv[2]))
	c.Server().AddDirty(1)
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_STRING, "set", c.Argv[1], c.DB().GetID())
	if len(okReply) != 0 {
		c.AddReply(okReply)
	} else {
		c.AddReply(protocol.OK)
	}
}

func SETCommand(c *redigo.CommandArg) {
	flags := REDIS_SET_NO_FLAGS

	for j := 3; j < c.Argc; j++ {
		a := strings.ToLower(string(c.Argv[j]))

		// var next string
		// if j < c.Argc-1 {
		// 	next = c.Argv[j+1]
		// }

		if a == "nx" {
			flags |= REDIS_SET_NX
		} else if a == "xx" {
			flags |= REDIS_SET_XX
		} else {
			c.AddReply(protocol.SyntaxErr)
			return
		}
	}

	rstringSet(c, flags, nil, nil)
}

func SETNXCommand(c *redigo.CommandArg) {
	rstringSet(c, REDIS_SET_NX, protocol.COne, protocol.CZero)
}

func SETEXCommand(c *redigo.CommandArg) {

}

func PSETEXCommand(c *redigo.CommandArg) {

}

func rstringGet(c *redigo.CommandArg) bool {
	if o := c.LookupKeyReadOrReply(c.Argv[1], protocol.NullBulk); o == nil {
		return true
	} else if str, ok := o.(rtype.String); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return false
	} else {
		c.AddReplyBulk(str.Bytes())
		return true
	}
}

func GETCommand(c *redigo.CommandArg) {
	rstringGet(c)
}

func GETSETCommand(c *redigo.CommandArg) {
	if rstringGet(c) {
		c.DB().SetKeyPersist(c.Argv[1], rstring.New(c.Argv[2]))
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_STRING, "set", c.Argv[1], c.DB().GetID())
		c.Server().AddDirty(1)
	}
}

func SETRANGECommand(c *redigo.CommandArg) {

}

func GETRANGECommand(c *redigo.CommandArg) {

}

func MGETCommand(c *redigo.CommandArg) {
	c.AddReplyMultiBulkLen(c.Argc - 1)
	for i := 1; i < c.Argc; i++ {
		if o := c.DB().LookupKeyRead(c.Argv[i]); o == nil {
			c.AddReply(protocol.NullBulk)
		} else {
			if val, ok := o.(rtype.String); !ok {
				c.AddReply(protocol.NullBulk)
			} else {
				c.AddReplyBulk(val.Bytes())
			}
		}
	}
}

func rstringmset(c *redigo.CommandArg, nx bool) {
	if c.Argc%2 == 0 {
		c.AddReplyError("wrong number of arguments for MSET")
		return
	}
	/* Handle the NX flag. The MSETNX semantic is to return zero and don't
	 * set nothing at all if at least one already key exists. */
	busykeys := 0
	if nx {
		for i := 1; i < c.Argc; i++ {
			if c.DB().LookupKeyWrite(c.Argv[i]) != nil {
				busykeys++
			}
		}
		if busykeys {
			c.AddReply(protocol.CZero)
			return
		}
	}

	for i := 1; i < c.Argc; i++ {
		c.DB().SetKeyPersist(c.Argv[i], rstring.New(c.Argv[i+1]))
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_STRING, "set", c.Argv[i], c.DB().GetID())
	}
	c.Server().AddDirty((c.Argc - 1) / 2)
	if nx {
		c.AddReply(protocol.COne)
	} else {
		c.AddReply(protocol.OK)
	}
}

func MSETCommand(c *redigo.CommandArg) {
	rstringmset(c, false)
}

func MSETNXCommand(c *redigo.CommandArg) {
	rstringmset(c, true)
}

func INCRBYFLOATCommand(c *redigo.CommandArg) {
	var str rtype.String

	var ok bool
	o := c.DB().LookupKeyWrite(c.Argv[1])
	if str, ok = o.(rtype.String); o != nil && !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	if x, ok := GetFloat64FromStringOrReply(c, str, ""); !ok {
		return
	} else if incr, ok := GetFloat64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); !ok {
		return
	} else {
		x += incr
		if math.IsNaN(x) || math.IsInf(x, 0) {
			c.AddReplyError("increment would produce NaN or Infinity")
			return
		}

		str = rstring.NewFromFloat64(x)
		if o != nil {
			c.DB().Update(c.Argv[1], str)
		} else {
			c.DB().Add(c.Argv[1], str)
		}

		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_STRING, "incrbyfloat", c.Argv[1], c.DB().GetID())
		c.Server().AddDirty(1)
		c.AddReplyBulk(str.Bytes())

		/* TODO: Always replicate INCRBYFLOAT as a SET command with the final value
		 * in order to make sure that differences in float precision or formatting
		 * will not create differences in replicas or after an AOF restart. */
	}
}

func rstringIncrDecr(c *redigo.CommandArg, incr int64) {
	var str rtype.String

	var ok bool
	o := c.DB().LookupKeyWrite(c.Argv[1])
	if str, ok = o.(rtype.String); o != nil && !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	// When the key value does not exist, this function will still work.
	// It will produce a new 0 + incr value and set it to db.
	if x, ok := GetInt64FromStringOrReply(c, str, ""); ok {
		if (incr < 0 && x < 0 && incr < math.MinInt64-x) ||
			(incr > 0 && x > 0 && incr > math.MaxInt64-x) {
			c.AddReplyError("increment or decrement would overflow")
			return
		}
		x += incr

		// TODO: Redis uses redigo Integers to save memory, we do not implement this feature right now.
		str = rstring.NewFromInt64(x)
		if o != nil {
			c.DB().Update(c.Argv[1], str)
		} else {
			c.DB().Add(c.Argv[1], str)
		}

		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_STRING, "incrby", c.Argv[1], c.DB().GetID())
		c.Server().AddDirty(1)
		c.AddReplyInt64(x)
	}
}

func INCRCommand(c *redigo.CommandArg) {
	rstringIncrDecr(c, 1)
}

func DECRCommand(c *redigo.CommandArg) {
	rstringIncrDecr(c, -1)
}

func INCRBYCommand(c *redigo.CommandArg) {
	if incr, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); ok {
		rstringIncrDecr(c, incr)
	}
}

func DECRBYCommand(c *redigo.CommandArg) {
	if incr, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[2]), ""); ok {
		rstringIncrDecr(c, -incr)
	}
}

func APPENDCommand(c *redigo.CommandArg) {
	var str rtype.String
	var totallen int64

	var ok bool
	if o := c.DB().LookupKeyWrite(c.Argv[1]); o == nil {
		str = rstring.New(c.Argv[2])
		c.DB().Add(c.Argv[1], str)
		totallen = str.Len()
	} else if str, ok = o.(rtype.String); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	} else {
		totallen = str.Len() + int64(len(c.Argv[2]))
		if !CheckStringlength(c, totallen) {
			return
		}

		c.DB().Update(c.Argv[1], str.Append(c.Argv[2]))
	}
	c.DB().SignalModifyKey(c.Argv[1])
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_STRING, "append", c.Argv[1], c.DB().GetID())
	c.Server().AddDirty(1)
	c.AddReplyInt64(totallen)
}

func STRLENCommand(c *redigo.CommandArg) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], protocol.CZero); o != nil {
		if str, ok := o.(rtype.String); !ok {
			c.AddReply(protocol.WrongTypeErr)
		} else {
			c.AddReplyInt64(str.Len())
		}
	}
}
