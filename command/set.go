package command

import (
	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/pubsub"
	"github.com/SteveZhangBit/redigo/rtype/rstring"
	"github.com/SteveZhangBit/redigo/rtype/set"
	"github.com/SteveZhangBit/redigo/shared"
)

func SADDCommand(c *redigo.RedigoClient) {
	var s *set.Set
	if o := c.DB.LookupKeyWrite(c.Argv[1]); o == nil {
		s = set.New()
		c.DB.Add(c.Argv[1], s)
	} else {
		var ok bool
		if s, ok = o.(*set.Set); !ok {
			c.AddReply(shared.WrongTypeErr)
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
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifySet, "sadd", c.Argv[1], c.DB.ID)
	}
	c.Server.Dirty += added
	c.AddReplyInt64(int64(added))
}

func SREMCommand(c *redigo.RedigoClient) {
	var s *set.Set

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil {
		return
	} else if s, ok = o.(*set.Set); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	var deleted int
	var keyremoved bool
	for i := 2; i < c.Argc; i++ {
		if s.Remove(rstring.New(c.Argv[i])) {
			deleted++
			if s.Size() == 0 {
				c.DB.Delete(c.Argv[1])
				keyremoved = true
				break
			}
		}
	}
	if deleted > 0 {
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifySet, "srem", c.Argv[1], c.DB.ID)
		if keyremoved {
			pubsub.NotifyKeyspaceEvent(pubsub.NotifyGeneric, "del", c.Argv[1], c.DB.ID)
		}
		c.Server.Dirty += deleted
	}
	c.AddReplyInt64(int64(deleted))
}

func SMOVECommand(c *redigo.RedigoClient) {

}

func SISMEMBERCommand(c *redigo.RedigoClient) {
	var s *set.Set

	var ok bool
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o == nil {
		return
	} else if s, ok = o.(*set.Set); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	if s.IsMember(rstring.New(c.Argv[2])) {
		c.AddReply(shared.COne)
	} else {
		c.AddReply(shared.CZero)
	}
}

func SCARDCommand(c *redigo.RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil {
		if s, ok := o.(*set.Set); !ok {
			c.AddReply(shared.WrongTypeErr)
		} else {
			c.AddReplyInt64(int64(s.Size()))
		}
	}
}

func SPOPCommand(c *redigo.RedigoClient) {
	var s *set.Set

	var ok bool
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil {
		return
	} else if s, ok = o.(*set.Set); !ok {
		c.AddReply(shared.WrongTypeErr)
		return
	}

	e := s.RandomElement()
	s.Remove(e)
	pubsub.NotifyKeyspaceEvent(pubsub.NotifySet, "spop", c.Argv[1], c.DB.ID)

	// TODO: Replicate/AOF this command as an SREM operation

	c.AddReplyBulk(e.String())
	if s.Size() == 0 {
		c.DB.Delete(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyGeneric, "del", c.Argv[1], c.DB.ID)
	}
	c.DB.SignalModifyKey(c.Argv[1])
	c.Server.Dirty++
}

func SRANDMEMBERCommand(c *redigo.RedigoClient) {

}

func SINTERCommand(c *redigo.RedigoClient) {

}

func SINTERSTORECommand(c *redigo.RedigoClient) {

}

func SUNIONCommand(c *redigo.RedigoClient) {

}

func SUNIONSTORECommand(c *redigo.RedigoClient) {

}

func SDIFFCommand(c *redigo.RedigoClient) {

}

func SDIFFSTORECommand(c *redigo.RedigoClient) {

}

func SSCANCommand(c *redigo.RedigoClient) {

}
