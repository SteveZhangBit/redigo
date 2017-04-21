package redigo

import (
	"math"
	"strconv"

	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/shared"
)

func GetInt64FromStringOrReply(c *RedigoClient, o interface{}, msg string) (x int64, ok bool) {
	switch str := o.(type) {
	case nil:
		return 0, true
	case rstring.IntString:
		x, ok = int64(str), true
	case string:
		if i, err := strconv.ParseInt(str, 10, 64); err != nil {
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
			c.AddReplyError("value is not an integer or out of range")
		}
	}
	return
}

func GetFloat64FromStringOrReply(c *RedigoClient, o interface{}, msg string) (x float64, ok bool) {
	switch str := o.(type) {
	case nil:
		return 0.0, true
	case rstring.IntString:
		x, ok = float64(str), true
	case rstring.NormString:
		if i, err := strconv.ParseFloat(string(str), 64); err != nil {
			ok = false
		} else {
			x, ok = i, true
		}
	case string:
		if i, err := strconv.ParseFloat(str, 64); err != nil {
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

func CheckStringlength(c *RedigoClient, size int64) bool {
	if size > 512*1024*1024 {
		c.AddReplyError("string exceeds maximum allowed size (512MB)")
		return false
	}
	return true
}

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/

// SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>]
// Starting with Redis 2.6.12 SET supports a set of options that modify its behavior:
// EX seconds -- Set the specified expire time, in seconds.
// PX milliseconds -- Set the specified expire time, in milliseconds.
// NX -- Only set the key if it does not already exist.
// XX -- Only set the key if it already exist.

// TODO: Currently, we only implement the very basic function of SET command.
func SETCommand(c *RedigoClient) {
	c.DB.SetKeyPersist(c.Argv[1], rstring.New(c.Argv[2]))
	c.Server.Dirty++
	NotifyKeyspaceEvent(REDIS_NOTIFY_STRING, "set", c.Argv[1], c.DB.ID)
	c.AddReply(shared.OK)
}

func SETNXCommand(c *RedigoClient) {

}

func SETEXCommand(c *RedigoClient) {

}

func PSETEXCommand(c *RedigoClient) {

}

func SETRANGECommand(c *RedigoClient) {

}

func GETRANGECommand(c *RedigoClient) {

}

func MGETCommand(c *RedigoClient) {

}

func MSETCommand(c *RedigoClient) {

}

func MSETNXCommand(c *RedigoClient) {

}

func rstringGet(c *RedigoClient) bool {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o == nil {
		return true
	} else if str, ok := o.(rtype.String); !ok {
		c.AddReply(shared.WrongTypeErr)
		return false
	} else {
		c.AddReplyBulk(str.String())
		return true
	}
}

func GETCommand(c *RedigoClient) {
	rstringGet(c)
}

func GETSETCommand(c *RedigoClient) {
	if rstringGet(c) {
		c.DB.SetKeyPersist(c.Argv[1], rstring.New(c.Argv[2]))
		NotifyKeyspaceEvent(REDIS_NOTIFY_STRING, "set", c.Argv[1], c.DB.ID)
		c.Server.Dirty++
	}
}

func INCRBYFLOATCommand(c *RedigoClient) {
	var str rtype.String

	o := c.DB.LookupKeyWrite(c.Argv[1])
	if _, ok := o.(rtype.String); o != nil && !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	if x, ok := GetFloat64FromStringOrReply(c, o, ""); !ok {
		return
	} else if incr, ok := GetFloat64FromStringOrReply(c, c.Argv[2], ""); !ok {
		return
	} else {
		x += incr
		if math.IsNaN(x) || math.IsInf(x, 0) {
			c.AddReplyError("increment would produce NaN or Infinity")
			return
		}

		str = rstring.NewFromFloat64(x)
		if o != nil {
			c.DB.Update(c.Argv[1], str)
		} else {
			c.DB.Add(c.Argv[1], str)
		}

		c.DB.SignalModifyKey(c.Argv[1])
		NotifyKeyspaceEvent(REDIS_NOTIFY_STRING, "incrbyfloat", c.Argv[1], c.DB.ID)
		c.Server.Dirty++
		c.AddReplyBulk(str.String())

		/* TODO: Always replicate INCRBYFLOAT as a SET command with the final value
		 * in order to make sure that differences in float precision or formatting
		 * will not create differences in replicas or after an AOF restart. */
	}
}

func strIncrDecr(c *RedigoClient, incr int64) {
	var str rtype.String

	o := c.DB.LookupKeyWrite(c.Argv[1])
	if _, ok := o.(rtype.String); o != nil && !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	// When the key value does not exist, this function will still work.
	// It will produce a new 0 + incr value and set it to db.
	if x, ok := GetInt64FromStringOrReply(c, o, ""); ok {
		if (incr < 0 && x < 0 && incr < math.MinInt64-x) ||
			(incr > 0 && x > 0 && incr > math.MaxInt64-x) {
			c.AddReplyError("increment or decrement would overflow")
			return
		}
		x += incr

		// TODO: Redis uses Shared Integers to save memory, we do not implement this feature right now.
		str = rstring.NewFromInt64(x)
		if o != nil {
			c.DB.Update(c.Argv[1], str)
		} else {
			c.DB.Add(c.Argv[1], str)
		}

		c.DB.SignalModifyKey(c.Argv[1])
		NotifyKeyspaceEvent(REDIS_NOTIFY_STRING, "incrby", c.Argv[1], c.DB.ID)
		c.Server.Dirty++
		c.AddReply(shared.Colon)
		c.AddReply(str.String())
		c.AddReply(shared.CRLF)
	}
}

func INCRCommand(c *RedigoClient) {
	strIncrDecr(c, 1)
}

func DECRCommand(c *RedigoClient) {
	strIncrDecr(c, -1)
}

func INCRBYCommand(c *RedigoClient) {
	if incr, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); ok {
		strIncrDecr(c, incr)
	}
}

func DECRBYCommand(c *RedigoClient) {
	if incr, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); ok {
		strIncrDecr(c, -incr)
	}
}

func APPENDCommand(c *RedigoClient) {
	var str rtype.String
	var totallen int64

	var ok bool
	if o := c.DB.LookupKeyWrite(c.Argv[1]); o == nil {
		str = rstring.New(c.Argv[2])
		c.DB.Add(c.Argv[1], str)
		totallen = str.Len()
	} else if str, ok = o.(rtype.String); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	} else {
		totallen = str.Len() + int64(len(c.Argv[2]))
		if !CheckStringlength(c, totallen) {
			return
		}

		str.Append(c.Argv[2])
	}
	c.DB.SignalModifyKey(c.Argv[1])
	NotifyKeyspaceEvent(REDIS_NOTIFY_STRING, "append", c.Argv[1], c.DB.ID)
	c.Server.Dirty++
	c.AddReplyInt64(totallen)
}

func STRLENCommand(c *RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil {
		if str, ok := o.(rtype.String); !ok {
			c.AddReply(shared.WrongTypeErr)
		} else {
			c.AddReplyInt64(str.Len())
		}
	}
}
