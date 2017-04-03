package hash

import (
	"math"

	"github.com/SteveZhangBit/redigo/pubsub"
	"github.com/SteveZhangBit/redigo/rstring"
	"github.com/SteveZhangBit/redigo/shared"

	"github.com/SteveZhangBit/redigo"
)

const (
	HashKey = (1 << iota)
	HashValue
)

type HashTable map[string]*rstring.RString

func CheckType(c *redigo.RedigoClient, o interface{}) (ok bool) {
	if _, ok = o.(HashTable); !ok {
		c.AddReply(shared.WrongTypeErr)
	}
	return
}

func lookupWriteOrCreate(c *redigo.RedigoClient, key []byte) (h HashTable) {
	if o := c.DB.LookupKeyWrite(key); o == nil {
		h = make(HashTable)
		c.DB.Add(key, h)
	} else {
		var ok bool
		if h, ok = o.(HashTable); !ok {
			c.AddReply(shared.WrongTypeErr)
		}
	}
	return
}

/* Add an element, discard the old if the key already exists.
 * Return false on insert and true on update. */
func set(h HashTable, field []byte, val *rstring.RString) (update bool) {
	key := string(field)
	_, update = h[key]
	h[key] = val
	return
}

func HSETCommand(c *redigo.RedigoClient) {
	var h HashTable
	if h = lookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	update := set(h, c.Argv[2], rstring.New(c.Argv[3]))
	if update {
		c.AddReply(shared.CZero)
	} else {
		c.AddReply(shared.COne)
	}
	c.DB.SignalModifyKey(c.Argv[1])
	pubsub.NotifyKeyspaceEvent(pubsub.NotifyHash, "hset", c.Argv[1], c.DB.ID)
	c.Server.Dirty++
}

func HSETNXCommand(c *redigo.RedigoClient) {
	var h HashTable
	if h = lookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	if _, ok := h[string(c.Argv[2])]; ok {
		c.AddReply(shared.CZero)
	} else {
		set(h, c.Argv[2], rstring.New(c.Argv[3]))
		c.AddReply(shared.COne)
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyHash, "hset", c.Argv[1], c.DB.ID)
		c.Server.Dirty++
	}
}

func HMSETCommand(c *redigo.RedigoClient) {
	var h HashTable
	if c.Argc%2 == 1 {
		c.AddReplyError("wrong number of arguments for HMSET")
		return
	}
	if h = lookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	for i := 2; i < c.Argc; i += 2 {
		set(h, c.Argv[i], rstring.New(c.Argv[i+1]))
	}
	c.AddReply(shared.OK)
	c.DB.SignalModifyKey(c.Argv[1])
	pubsub.NotifyKeyspaceEvent(pubsub.NotifyHash, "hset", c.Argv[1], c.DB.ID)
	c.Server.Dirty++
}

func HINCRBYCommand(c *redigo.RedigoClient) {
	var h HashTable
	var val, incr int64
	if x, ok := rstring.GetInt64FromStringOrReply(c, c.Argv[3], ""); ok {
		incr = x
	} else {
		return
	}
	if h = lookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	if cur, ok := h[string(c.Argv[2])]; ok {
		if val, ok = rstring.GetInt64FromStringOrReply(c, cur, "hash value is not an integer"); !ok {
			return
		}
	}

	if (incr < 0 && val < 0 && incr < math.MinInt64-val) ||
		(incr > 0 && val > 0 && incr > math.MaxInt64-val) {
		c.AddReplyError("increment or decrement would overflow")
		return
	}
	val += incr
	set(h, c.Argv[2], rstring.NewFromInt64(val))
	c.AddReplyInt64(val)
	c.DB.SignalModifyKey(c.Argv[1])
	pubsub.NotifyKeyspaceEvent(pubsub.NotifyHash, "hincrby", c.Argv[1], c.DB.ID)
	c.Server.Dirty++
}

func HINCRBYFLOATCommand(c *redigo.RedigoClient) {
	var h HashTable
	var val, incr float64
	if x, ok := rstring.GetFloat64FromStringOrReply(c, c.Argv[3], ""); ok {
		incr = x
	} else {
		return
	}
	if h = lookupWriteOrCreate(c, c.Argv[1]); h == nil {
		return
	}
	val += incr
	str := rstring.NewFromFloat64(val)
	set(h, c.Argv[2], str)
	c.AddReplyBulk(str.String())
	c.DB.SignalModifyKey(c.Argv[1])
	pubsub.NotifyKeyspaceEvent(pubsub.NotifyHash, "hincrbyfloat", c.Argv[1], c.DB.ID)
	c.Server.Dirty++

	/* Always replicate HINCRBYFLOAT as an HSET command with the final value
	 * in order to make sure that differences in float pricision or formatting
	 * will not create differences in replicas or after an AOF restart. */
}

func addFieldToReply(c *redigo.RedigoClient, h HashTable, field []byte) {
	if h == nil {
		c.AddReply(shared.NullBulk)
		return
	}
	if val, ok := h[string(field)]; !ok {
		c.AddReply(shared.NullBulk)
	} else {
		c.AddReplyBulk(val.String())
	}
}

func HGETCommand(c *redigo.RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.NullBulk); o != nil && CheckType(c, o) {
		addFieldToReply(c, o.(HashTable), c.Argv[2])
	}
}

func HMGETCommand(c *redigo.RedigoClient) {
	/* Don't abort when the key cannot be found. Non-existing keys are empty
	 * hashes, where HMGET should respond with a series of null bulks. */
	if o := c.DB.LookupKeyRead(c.Argv[1]); o == nil || CheckType(c, o) {
		c.AddReplyMultiBulkLen(c.Argc - 2)
		h, _ := o.(HashTable)
		for i := 2; i < c.Argc; i++ {
			addFieldToReply(c, h, c.Argv[i])
		}
	}
}

func HDELCommand(c *redigo.RedigoClient) {
	var h HashTable
	var deleted uint
	var keyremoved bool

	if o := c.LookupKeyWriteOrReply(c.Argv[1], shared.CZero); o == nil || !CheckType(c, o) {
		return
	} else {
		h = o.(HashTable)
	}

	for i := 2; i < c.Argc; i++ {
		key := string(c.Argv[i])
		if _, ok := h[key]; ok {
			delete(h, key)
			deleted++
			if len(h) == 0 {
				c.DB.Delete(c.Argv[1])
				keyremoved = true
				break
			}
		}
	}
	if deleted > 0 {
		c.DB.SignalModifyKey(c.Argv[1])
		pubsub.NotifyKeyspaceEvent(pubsub.NotifyHash, "hdel", c.Argv[1], c.DB.ID)
		if keyremoved {
			pubsub.NotifyKeyspaceEvent(pubsub.NotifyGeneric, "del", c.Argv[1], c.DB.ID)
		}
		c.Server.Dirty += deleted
	}
	c.AddReplyInt64(int64(deleted))
}

func HLENCommand(c *redigo.RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil && CheckType(c, o) {
		c.AddReplyInt64(int64(len(o.(HashTable))))
	}
}

func getall(c *redigo.RedigoClient, flags int) {
	var h HashTable
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.EmptyMultiBulk); o == nil || !CheckType(c, o) {
		return
	} else {
		h = o.(HashTable)
	}

	multiplier := 0
	if flags&HashKey > 0 {
		multiplier++
	}
	if flags&HashValue > 0 {
		multiplier++
	}
	length := len(h) * multiplier
	c.AddReplyMultiBulkLen(length)

	count := 0
	for key, val := range h {
		if flags&HashKey > 0 {
			count++
			c.AddReplyBulk(key)
		}
		if flags&HashValue > 0 {
			count++
			c.AddReplyBulk(val.String())
		}
	}
	if count != length {
		panic("count does not equal to length at hash get all method")
	}
}

func HKEYSCommand(c *redigo.RedigoClient) {
	getall(c, HashKey)
}

func HVALSCommand(c *redigo.RedigoClient) {
	getall(c, HashValue)
}

func HGETALLCommand(c *redigo.RedigoClient) {
	getall(c, HashKey|HashValue)
}

func HEXISTSCommand(c *redigo.RedigoClient) {
	if o := c.LookupKeyReadOrReply(c.Argv[1], shared.CZero); o != nil && CheckType(c, o) {
		if _, ok := o.(HashTable)[string(c.Argv[2])]; ok {
			c.AddReply(shared.COne)
		} else {
			c.AddReply(shared.CZero)
		}
	}
}

func HSCANCommand(c *redigo.RedigoClient) {

}
