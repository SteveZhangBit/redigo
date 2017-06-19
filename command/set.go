package command

import (
	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/protocol"
	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/rtype/set"
)

func SADDCommand(c *redigo.CommandArg) {
	var s rtype.Set

	if o := c.DB().LookupKeyWrite(c.Argv[1]); o == nil {
		s = set.New(rstring.New(c.Argv[2]))
		c.DB().Add(c.Argv[1], s)
	} else {
		var ok bool
		if s, ok = o.(rtype.Set); !ok {
			c.AddReply(protocol.WrongTypeErr)
			return
		}
	}

	var added int
	for i := 2; i < c.Argc; i++ {
		if s.Add(rstring.New(c.Argv[i])) {
			added++
		}
	}
	if added > 0 {
		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_SET, "sadd", c.Argv[1], c.DB().GetID())
	}
	c.Server().AddDirty(added)
	c.AddReplyInt64(int64(added))
}

func SREMCommand(c *redigo.CommandArg) {
	var s rtype.Set

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], protocol.CZero); o == nil {
		return
	} else if s, ok = o.(rtype.Set); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	var deleted int
	var keyremoved bool
	for i := 2; i < c.Argc; i++ {
		if s.Remove(rstring.New(c.Argv[i])) {
			deleted++
			if s.Size() == 0 {
				c.DB().Delete(c.Argv[1])
				keyremoved = true
				break
			}
		}
	}
	if deleted > 0 {
		c.DB().SignalModifyKey(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_SET, "srem", c.Argv[1], c.DB().GetID())
		if keyremoved {
			c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB().GetID())
		}
		c.Server().AddDirty(deleted)
	}
	c.AddReplyInt64(int64(deleted))
}

func SMOVECommand(c *redigo.CommandArg) {

}

func SISMEMBERCommand(c *redigo.CommandArg) {
	var s rtype.Set

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], protocol.CZero); o == nil {
		return
	} else if s, ok = o.(rtype.Set); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	if s.IsMember(rstring.New(c.Argv[2])) {
		c.AddReply(protocol.COne)
	} else {
		c.AddReply(protocol.CZero)
	}
}

func SCARDCommand(c *redigo.CommandArg) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], protocol.CZero); o != nil {
		if s, ok := o.(rtype.Set); !ok {
			c.AddReply(protocol.WrongTypeErr)
		} else {
			c.AddReplyInt64(int64(s.Size()))
		}
	}
}

func SPOPCommand(c *redigo.CommandArg) {
	var s rtype.Set

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], protocol.CZero); o == nil {
		return
	} else if s, ok = o.(rtype.Set); !ok {
		c.AddReply(protocol.WrongTypeErr)
		return
	}

	e := s.RandomElement()
	s.Remove(e)
	c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_SET, "spop", c.Argv[1], c.DB().GetID())

	// TODO: Replicate/AOF this command as an SREM operation

	c.AddReplyBulk(e.Bytes())
	if s.Size() == 0 {
		c.DB().Delete(c.Argv[1])
		c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[1], c.DB().GetID())
	}
	c.DB().SignalModifyKey(c.Argv[1])
	c.Server().AddDirty(1)
}

func SRANDMEMBERCommand(c *redigo.CommandArg) {

}

func SINTERCommand(c *redigo.CommandArg) {

}

func SINTERSTORECommand(c *redigo.CommandArg) {

}

func SUNIONCommand(c *redigo.CommandArg) {

}

func SUNIONSTORECommand(c *redigo.CommandArg) {

}

func SDIFFCommand(c *redigo.CommandArg) {

}

func SDIFFSTORECommand(c *redigo.CommandArg) {

}

func SSCANCommand(c *redigo.CommandArg) {

}
