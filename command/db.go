package command

import (
	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/rtype"
	"bytes"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
)

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

func FLUSHDBCommand(c redigo.CommandArg) {

}

func FLUSHALLCommand(c redigo.CommandArg) {

}

func DELCommand(c redigo.CommandArg) {
	var deleted int64
	for i := 0; i < c.Argc; i++ {
		c.DB().ExpireIfNeed(c.Argv[i])
		if c.DB().Delete(c.Argv[i]) {
			c.DB().SignalModifyKey(c.Argv[i])
			c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[i], c.DB().GetID())
			c.Server().AddDirty(1)
			deleted++
		}
	}
	c.AddReplyInt64(deleted)
}

/* EXISTS key1 key2 ... key_N.
 * Return value is the number of keys existing. */
func EXISTSCommand(c redigo.CommandArg) {
	var count int64
	for i := 1; i < c.Argc; i++ {
		c.DB().ExpireIfNeed(c.Argv[i])
		if c.DB().Exists(c.Argv[i]) {
			count++
		}
	}
	c.AddReplyInt64(count)
}

func SELECTCommand(c redigo.CommandArg) {
	var id int
	if x, ok := GetInt64FromStringOrReply(c, rstring.New(c.Argv[1]), "invalid DB index"); !ok {
		return
	} else {
		id = int(x)
	}

	if c.SelectDB(id) {
		c.AddReplyError("invalid DB index")
	} else {
		c.AddReply(redigo.OK)
	}
}

func RANDOMKEYCommand(c redigo.CommandArg) {
	if key := c.DB().RandomKey(); len(key) == 0 {
		c.AddReply(redigo.NullBulk)
	} else {
		c.AddReplyBulk(key)
	}
}

func KEYSCommand(c redigo.CommandArg) {
	var keys [][]byte

	pattern := c.Argv[1]
	isAllKeys := len(pattern) == 1 && pattern[0] == '*'
	for key := range c.DB().GetDict() {
		key_bytes := []byte(key)
		if isAllKeys || redigo.MatchPattern(pattern, key_bytes, false) {
			keys = append(keys, key_bytes)
		}
	}
	c.AddReplyMultiBulkLen(len(keys))
	for _, key := range keys {
		c.AddReplyBulk(key)
	}
}

func SCANCommand(c redigo.CommandArg) {

}

func DBSIZECommand(c redigo.CommandArg) {

}

func LASTSAVECommand(c redigo.CommandArg) {

}

func TYPECommand(c redigo.CommandArg) {
	var t string
	if o := c.DB().LookupKeyRead(c.Argv[1]); o == nil {
		t = "none"
	} else {
		switch o.(type) {
		case rtype.String:
			t = "string"
		case rtype.List:
			t = "list"
		case rtype.Set:
			t = "set"
		case rtype.ZSet:
			t = "zset"
		case rtype.HashMap:
			t = "hash"
		default:
			t = "unknown"
		}
	}
	c.AddReplyStatus(t)
}

func renameGeneric(c redigo.CommandArg, nx bool) {
	var o interface{}

	// To use the same key as src and dst is probably an error
	if bytes.Equal(c.Argv[1], c.Argv[2]) {
		c.AddReply(redigo.SameObjectErr)
		return
	}

	if o = c.LookupKeyWriteOrReply(c.Argv[1], redigo.NoKeyErr); o == nil {
		return
	}

	expire := c.DB().GetExpire(c.Argv[1])
	if c.DB().LookupKeyWrite(c.Argv[2]) != nil {
		if nx {
			c.AddReply(redigo.CZero)
			return
		}
		/* Overwrite: delete the old key before creating the new one
		 * with the same name. */
		c.DB().Delete(c.Argv[2])
	}
	c.DB().Add(c.Argv[2], o)
	if expire > 0 {
		c.DB().SetExpire(c.Argv[2], expire)
	}
	c.DB().Delete(c.Argv[1])
	c.DB().SignalModifyKey(c.Argv[1])
	c.DB().SignalModifyKey(c.Argv[2])
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "rename_from", c.Argv[1], c.DB().GetID())
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "rename_to", c.Argv[2], c.DB().GetID())
	c.Server().AddDirty(1)
	if nx {
		c.AddReply(redigo.COne)
	} else {
		c.AddReply(redigo.OK)
	}
}

func RENAMECommand(c redigo.CommandArg) {
	renameGeneric(c, false)
}

func RENAMENXCommand(c redigo.CommandArg) {
	renameGeneric(c, true)
}

func MOVECommand(c redigo.CommandArg) {

}

/*-----------------------------------------------------------------------------
 * Expire commands
 *----------------------------------------------------------------------------*/

func EXPIRECommand(c redigo.CommandArg) {

}

func EXPIREATCommand(c redigo.CommandArg) {

}

func PEXPIRECommand(c redigo.CommandArg) {

}

func PEXPIREATCommand(c redigo.CommandArg) {

}

func TTLCommand(c redigo.CommandArg) {

}

func PTTLCommand(c redigo.CommandArg) {

}

func PERSISTCommand(c redigo.CommandArg) {

}
