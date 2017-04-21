package redigo

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/SteveZhangBit/redigo/rtype/list"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type RedigoDB struct {
	ID      int
	Dict    map[string]interface{}
	Expires map[string]time.Time
	Server  *RedigoServer
}

func NewDB() *RedigoDB {
	return &RedigoDB{
		Dict:    make(map[string]interface{}),
		Expires: make(map[string]time.Time),
	}
}

func (r *RedigoDB) LookupKey(key string) interface{} {
	/* TODO: Update the access time for the ageing algorithm.
	 * Don't do it if we have a saving child, as this will trigger
	 * a copy on write madness. */

	o, _ := r.Dict[key]
	return o
}

func (r *RedigoDB) LookupKeyRead(key string) interface{} {
	r.expireIfNeed(key)

	if o, ok := r.Dict[key]; !ok {
		r.Server.KeyspaceMisses++
		return nil
	} else {
		r.Server.KeyspaceHits++
		return o
	}
}

func (r *RedigoDB) LookupKeyWrite(key string) interface{} {
	r.expireIfNeed(key)
	return r.LookupKey(key)
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
func (r *RedigoDB) Add(key string, val interface{}) {
	if _, ok := r.Dict[key]; !ok {
		r.Dict[key] = val
		if _, ok = val.(*list.LinkedList); ok {
			r.SignalListAsReady(key)
		}
	} else {
		panic(fmt.Sprintf("The key %s already exists.", key))
	}
}

func (r *RedigoDB) Update(key string, val interface{}) {
	if _, ok := r.Dict[key]; ok {
		r.Dict[key] = val
	} else {
		panic(fmt.Sprintf("Key %s doesn't exist", key))
	}
}

func (r *RedigoDB) Delete(key string) (ok bool) {
	if _, ok = r.Expires[key]; ok {
		delete(r.Expires, key)
	}
	if _, ok = r.Dict[key]; ok {
		delete(r.Dict, key)
	}
	return
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The ref count of the value object is incremented.
 * 2) clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent). */
func (r *RedigoDB) SetKeyPersist(key string, val interface{}) {
	if r.LookupKeyWrite(key) == nil {
		r.Add(key, val)
	} else {
		r.Dict[key] = val
	}
	r.removeExpire(key)
	r.SignalModifyKey(key)
}

func (r *RedigoDB) Exists(key string) (ok bool) {
	_, ok = r.Dict[key]
	return
}

func (r *RedigoDB) RandomKey() (key string) {
	keys := make([]string, len(r.Dict))

	i := 0
	for k := range r.Dict {
		keys[i] = k
	}

	for {
		key = keys[rand.Intn(len(keys))]
		if r.expireIfNeed(key) {
			continue
		}
	}

	return
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *----------------------------------------------------------------------------*/

func (r *RedigoDB) SignalModifyKey(key string) {

}

func (r *RedigoDB) SignalListAsReady(key string) {

}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

func (r *RedigoDB) expireIfNeed(key string) bool {
	return false
}

func (r *RedigoDB) removeExpire(key string) {

}

// func (r *RedigoDB) setExpire(key string, when time.Time) {

// }

// func (r *RedigoDB) getExpire(key string) time.Time {

// }

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

func FLUSHDBCommand(c *RedigoClient) {

}

func FLUSHALLCommand(c *RedigoClient) {

}

func DELCommand(c *RedigoClient) {

}

/* EXISTS key1 key2 ... key_N.
 * Return value is the number of keys existing. */
func EXISTSCommand(c *RedigoClient) {

}

func SELECTCommand(c *RedigoClient) {

}

func RANDOMKEYCommand(c *RedigoClient) {

}

func KEYSCommand(c *RedigoClient) {

}

func SCANCommand(c *RedigoClient) {

}

func DBSIZECommand(c *RedigoClient) {

}

func LASTSAVECommand(c *RedigoClient) {

}

func TYPECommand(c *RedigoClient) {

}

func SHUTDOWNCommand(c *RedigoClient) {

}

func RENAMECommand(c *RedigoClient) {

}

func RENAMENXCommand(c *RedigoClient) {

}

func MOVECommand(c *RedigoClient) {

}

/*-----------------------------------------------------------------------------
 * Expire commands
 *----------------------------------------------------------------------------*/

func EXPIRECommand(c *RedigoClient) {

}

func EXPIREATCommand(c *RedigoClient) {

}

func PEXPIRECommand(c *RedigoClient) {

}

func PEXPIREATCommand(c *RedigoClient) {

}

func TTLCommand(c *RedigoClient) {

}

func PTTLCommand(c *RedigoClient) {

}

func PERSISTCommand(c *RedigoClient) {

}
