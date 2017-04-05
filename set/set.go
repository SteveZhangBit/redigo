package set

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/pubsub"
	"github.com/SteveZhangBit/redigo/rstring"
	"github.com/SteveZhangBit/redigo/set/intset"
	"github.com/SteveZhangBit/redigo/shared"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type HashSet map[rstring.RString]struct{}

type Set struct {
	// Should be intset or hashtable
	Val interface{}
}

func New() *Set {
	return &Set{}
}

func (s *Set) convert() {
	switch x := s.Val.(type) {
	case *intset.IntSet:
		new_s := make(HashSet)
		for i := 0; i < x.Length; i++ {
			new_s[*rstring.NewFromInt64(x.Get(i))] = struct{}{}
		}
		s.Val = new_s
	default:
		panic("Unsupported set conversion")
	}
}

func (s *Set) Add(val *rstring.RString) bool {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		if x, ok := val.Val.(int64); ok {
			if s_enc.Add(x) {
				// Convert to regular set when the intset contains too many entries.
				if s_enc.Length > redigo.MaxIntsetEntries {
					s.convert()
				}
				return true
			}
		} else {
			// Failed to get integer from object, convert to regular set.
			s.convert()
			return s.Add(val)
		}

	case HashSet:
		if _, ok := s_enc[*val]; ok {
			return true
		}

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
	return false
}

func (s *Set) Remove(val *rstring.RString) bool {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		if x, ok := val.Val.(int64); ok {
			return s_enc.Remove(x)
		}

	case HashSet:
		if _, ok := s_enc[*val]; ok {
			delete(s_enc, *val)
			return true
		}

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
	return false
}

func (s *Set) Size() int {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		return s_enc.Length

	case HashSet:
		return len(s_enc)

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
}

func (s *Set) IsMember(val *rstring.RString) bool {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		if x, ok := val.Val.(int64); ok {
			return s_enc.Find(x)
		}

	case HashSet:
		_, ok := s_enc[*val]
		return ok

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
	return false
}

func (s *Set) RandomElement() *rstring.RString {
	switch s_enc := s.Val.(type) {
	case *intset.IntSet:
		return rstring.NewFromInt64(s_enc.Random())

	case HashSet:
		count := rand.Intn(len(s_enc))
		i := 0
		for val, _ := range s_enc {
			if i < count {
				count++
			} else {
				return &rstring.RString{Val: val.Val}
			}
		}

	default:
		panic(fmt.Sprintf("Type %T is not a set object", s_enc))
	}
	return nil
}

func CheckType(c *redigo.RedigoClient, o interface{}) (ok bool) {
	if _, ok = o.(*Set); !ok {
		c.AddReply(shared.WrongTypeErr)
	}
	return
}

func SADDCommand(c *redigo.RedigoClient) {
	var s *Set
	if o := c.DB.LookupKeyWrite(c.Argv[1]); o == nil {
		s = New()
		c.DB.Add(c.Argv[1], s)
	} else {
		var ok bool
		if s, ok = o.(*Set); !ok {
			c.AddReply(shared.WrongTypeErr)
			return
		}
	}

	var added uint
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
	var s *Set
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil || !CheckType(c, o) {
		return
	} else {
		s = o.(*Set)
	}

	var deleted uint
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
	var s *Set
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o == nil || !CheckType(c, o) {
		return
	} else {
		s = o.(*Set)
	}
	if s.IsMember(rstring.New(c.Argv[2])) {
		c.AddReply(shared.COne)
	} else {
		c.AddReply(shared.CZero)
	}
}

func SCARDCommand(c *redigo.RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil && CheckType(c, o) {
		c.AddReplyInt64(int64(o.(*Set).Size()))
	}
}

func SPOPCommand(c *redigo.RedigoClient) {
	var s *Set
	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil || !CheckType(c, o) {
		return
	} else {
		s = o.(*Set)
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
