package server

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
	id int
	// server *RedigoServer

	dict    map[string]interface{}
	expires map[string]time.Time

	KeyspaceMisses int
	KeyspaceHits   int
}

func NewDB() *RedigoDB {
	return &RedigoDB{
		dict:    make(map[string]interface{}),
		expires: make(map[string]time.Time),
	}
}

func (r *RedigoDB) GetID() int {
	return r.id
}

func (r *RedigoDB) LookupKey(key string) interface{} {
	/* TODO: Update the access time for the ageing algorithm.
	 * Don't do it if we have a saving child, as this will trigger
	 * a copy on write madness. */

	o, _ := r.dict[key]
	return o
}

func (r *RedigoDB) LookupKeyRead(key string) interface{} {
	r.expireIfNeed(key)

	if o, ok := r.dict[key]; !ok {
		r.KeyspaceMisses++
		return nil
	} else {
		r.KeyspaceHits++
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
	if _, ok := r.dict[key]; !ok {
		r.dict[key] = val
		if _, ok = val.(*list.LinkedList); ok {
			r.SignalListAsReady(key)
		}
	} else {
		panic(fmt.Sprintf("The key %s already exists.", key))
	}
}

func (r *RedigoDB) Update(key string, val interface{}) {
	if _, ok := r.dict[key]; ok {
		r.dict[key] = val
	} else {
		panic(fmt.Sprintf("Key %s doesn't exist", key))
	}
}

func (r *RedigoDB) Delete(key string) (ok bool) {
	if _, ok = r.expires[key]; ok {
		delete(r.expires, key)
	}
	if _, ok = r.dict[key]; ok {
		delete(r.dict, key)
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
		r.dict[key] = val
	}
	r.removeExpire(key)
	r.SignalModifyKey(key)
}

func (r *RedigoDB) Exists(key string) (ok bool) {
	_, ok = r.dict[key]
	return
}

func (r *RedigoDB) RandomKey() (key string) {
	keys := make([]string, len(r.dict))

	i := 0
	for k := range r.dict {
		keys[i] = k
		i++
	}

	for {
		key = keys[rand.Intn(len(keys))]
		if r.expireIfNeed(key) {
			continue
		}
		return
	}
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
