package command

import (
	"fmt"
	"math"
	"strconv"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/pubsub"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/shared"
)

func GetInt64FromStringOrReply(c *redigo.RedigoClient, o interface{}, msg string) (x int64, ok bool) {
	switch str := o.(type) {
	case nil:
		return 0, true
	case *rstring.RString:
		x, ok = str.Val.(int64)
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

func GetFloat64FromStringOrReply(c *redigo.RedigoClient, o interface{}, msg string) (x float64, ok bool) {
	switch str := o.(type) {
	case nil:
		return 0.0, true

	case *rstring.RString:
		switch val := str.Val.(type) {
		case string:
			if i, err := strconv.ParseFloat(val, 64); err != nil {
				ok = false
			} else {
				x, ok = i, true
			}
		case int64:
			x, ok = float64(val), true
		default:
			panic(fmt.Sprintf("Type %T is not a string object", val))
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

func CheckStringlength(c *redigo.RedigoClient, size int64) bool {
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
func SETCommand(c *redigo.RedigoClient) {
	c.DB.SetKey(c.Argv[1], rstring.New(c.Argv[2]))
	c.Server.Dirty++
	pubsub.NotifyKeyspaceEvent(pubsub.NotifyString, "set", c.Argv[1], c.DB.ID)
	c.AddReply(shared.OK)
}

func SETNXCommand(c *redigo.RedigoClient) {

}

func SETEXCommand(c *redigo.RedigoClient) {

}

func PSETEXCommand(c *redigo.RedigoClient) {

}

func GETSETCommand(c *redigo.RedigoClient) {

}

func SetRangeCommand(c *redigo.RedigoClient) {

}

func GetRangeCommand(c *redigo.RedigoClient) {

}

func MGETCommand(c *redigo.RedigoClient) {

}

func MSETCommand(c *redigo.RedigoClient) {

}

func MSETNXCommand(c *redigo.RedigoClient) {

}

func GETCommand(c *redigo.RedigoClient) bool {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o == nil {
		return true
	} else if str, ok := o.(*rstring.RString); !ok {
		c.AddReply(shared.WrongTypeErr)
		return false
	} else {
		c.AddReplyBulk(str.String())
		return true
	}
}

func INCRBYFLOATCommand(c *redigo.RedigoClient) {
	var str *rstring.RString

	o := c.DB.LookupKeyWrite(c.Argv[1])
	if _, ok := o.(*rstring.RString); o != nil && !ok {
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

		if o != nil {
			str = o.(*rstring.RString)
			str.Val = fmt.Sprintf("%.17f", x)
		} else {
			str = rstring.NewFromFloat64(x)
			c.DB.Add(c.Argv[1], str)
		}

		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyString, "incrbyfloat", c.Argv[1], c.DB.ID)
		c.Server.Dirty++
		c.AddReplyBulk(str.String())

		/* TODO: Always replicate INCRBYFLOAT as a SET command with the final value
		 * in order to make sure that differences in float precision or formatting
		 * will not create differences in replicas or after an AOF restart. */
	}
}

func strIncrDecr(c *redigo.RedigoClient, incr int64) {
	var str *rstring.RString

	o := c.DB.LookupKeyWrite(c.Argv[1])
	if _, ok := o.(*rstring.RString); o != nil && !ok {
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
		if o != nil {
			str = o.(*rstring.RString)
			str.Val = x
		} else {
			str = rstring.NewFromInt64(x)
			c.DB.Add(c.Argv[1], str)
		}
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyString, "incrby", c.Argv[1], c.DB.ID)
		c.Server.Dirty++
		c.AddReply(shared.Colon)
		c.AddReply(str.String())
		c.AddReply(shared.CRLF)
	}
}

func INCRCommand(c *redigo.RedigoClient) {
	strIncrDecr(c, 1)
}

func DECRCommand(c *redigo.RedigoClient) {
	strIncrDecr(c, -1)
}

func INCRBYCommand(c *redigo.RedigoClient) {
	if incr, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); ok {
		strIncrDecr(c, incr)
	}
}

func DECRBYCommand(c *redigo.RedigoClient) {
	if incr, ok := GetInt64FromStringOrReply(c, c.Argv[2], ""); ok {
		strIncrDecr(c, -incr)
	}
}

func APPENDCommand(c *redigo.RedigoClient) {
	var str *rstring.RString
	var totallen int64 = 0

	var ok bool
	if o := c.DB.LookupKeyWrite(c.Argv[1]); o == nil {
		str = rstring.New(c.Argv[2])
		c.DB.Add(c.Argv[1], str)
		totallen = str.Len()
	} else if str, ok = o.(*rstring.RString); !ok {
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
	pubsub.NotifyKeyspaceEvent(pubsub.NotifyString, "append", c.Argv[1], c.DB.ID)
	c.Server.Dirty++
	c.AddReplyInt64(totallen)
}

func STRLENCommand(c *redigo.RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil {
		if str, ok := o.(*rstring.RString); !ok {
			c.AddReply(shared.WrongTypeErr)
		} else {
			c.AddReplyInt64(str.Len())
		}
	}
}
